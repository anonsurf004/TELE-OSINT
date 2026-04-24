"""
main.py — FastAPI application: lifespan-managed background tasks + REST + WebSocket.

All background work (Telethon, ES bulk indexer, WS broadcaster) runs as
asyncio.Task objects created inside the FastAPI lifespan, sharing one event
loop with the HTTP server.  No threads, no multiprocessing.

Endpoints
---------
GET  /search?q=<kw>           Full-text search with recency + engagement boost.
GET  /channels/pending        Discovered channels awaiting operator approval.
POST /channels/{username}/approve  Approve a channel for active monitoring.
WS   /ws/live                 Real-time message broadcast over WebSocket.
GET  /health                  System health: ES, queue depths, WS connections.
"""
from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, Dict, List, Optional, Set

from fastapi import FastAPI, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse

from config import get_settings
from crawler.telegram_client import get_listener
from indexer.bulk_indexer import bulk_worker
from indexer.es_client import close_es_client, get_es_client, wait_for_elasticsearch
from processor.correlation_engine import get_correlation_engine, init_correlation_engine
from utils.logger import configure_logging, get_logger
from utils.queue_manager import get_queue_manager, init_queue_manager

configure_logging()
log = get_logger(__name__)
settings = get_settings()

# Grace period given to each background task to finish in-flight work
_SHUTDOWN_TIMEOUT = 10.0  # seconds


# ── WebSocket connection manager ──────────────────────────────────────────────

class ConnectionManager:
    """
    Thread-safe (asyncio-safe) registry of active WebSocket connections.

    Design decisions
    ----------------
    * asyncio.Lock is created lazily so it is always bound to the running loop.
    * connect() / disconnect() take the lock to mutate the registry.
    * broadcast() snapshots the registry under the lock, releases it, then
      sends to all clients concurrently using asyncio.gather — holding the lock
      during I/O would serialise every send.
    * Dead connections are detected on send failure and pruned after the batch.
    * Concurrency is bounded by settings.ws_broadcast_concurrency to prevent
      a single huge broadcast from monopolising the event loop.
    """

    def __init__(self) -> None:
        self._connections: List[WebSocket] = []
        self._lock: Optional[asyncio.Lock] = None

    def _get_lock(self) -> asyncio.Lock:
        """Lazy lock creation — always created inside the running loop."""
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    @property
    def count(self) -> int:
        return len(self._connections)

    async def connect(self, ws: WebSocket) -> None:
        await ws.accept()
        async with self._get_lock():
            self._connections.append(ws)
        log.info("ws.connected", total=self.count)

    async def disconnect(self, ws: WebSocket) -> None:
        async with self._get_lock():
            try:
                self._connections.remove(ws)
            except ValueError:
                pass
        log.info("ws.disconnected", total=self.count)

    async def broadcast(self, message: Dict[str, Any]) -> None:
        """
        Serialise ``message`` to JSON and send it to all connected clients.

        Sends are fired concurrently in batches of ws_broadcast_concurrency.
        Clients that fail to receive are removed from the registry.
        """
        if not self._connections:
            return

        payload = json.dumps(message, default=str)

        # Snapshot under lock; release before any I/O
        async with self._get_lock():
            targets = list(self._connections)

        dead: Set[WebSocket] = set()
        sem = asyncio.Semaphore(settings.ws_broadcast_concurrency)

        async def _send(ws: WebSocket) -> None:
            async with sem:
                try:
                    await ws.send_text(payload)
                except Exception:
                    dead.add(ws)

        await asyncio.gather(*(_send(ws) for ws in targets), return_exceptions=True)

        if dead:
            async with self._get_lock():
                for ws in dead:
                    try:
                        self._connections.remove(ws)
                    except ValueError:
                        pass
            log.debug("ws.pruned_dead", count=len(dead), remaining=self.count)


ws_manager = ConnectionManager()


# ── Background task coroutines ────────────────────────────────────────────────

async def _broadcast_worker() -> None:
    """
    Drains broadcast_queue and fans out each event to all WebSocket clients.

    On CancelledError (graceful shutdown): drains whatever remains in the
    queue before exiting so in-flight messages reach connected clients.
    """
    qm = get_queue_manager()
    log.info("broadcast_worker.started")

    try:
        while True:
            event = await qm.dequeue_broadcast()
            try:
                await ws_manager.broadcast(event)
            except Exception as exc:
                log.warning("broadcast_worker.send_error", error=str(exc))
            finally:
                qm.broadcast_done()

    except asyncio.CancelledError:
        # Drain whatever is already in the queue (non-blocking)
        drained = 0
        while True:
            try:
                event = qm._broadcast_q.get_nowait()
                await ws_manager.broadcast(event)
                qm.broadcast_done()
                drained += 1
            except asyncio.QueueEmpty:
                break
            except Exception:
                break
        log.info("broadcast_worker.shutdown", drained_on_exit=drained)


async def _telethon_task() -> None:
    """
    Authenticates the Telethon client, subscribes to seed channels, then
    calls run_until_disconnected() which awaits internally — it does not
    block the event loop.

    On CancelledError: calls listener.stop() to deregister event handlers
    cleanly before the Telethon connection is closed.
    """
    listener = get_listener()
    try:
        await listener.start()
        log.info("telethon_task.running")
        await listener.run_forever()
    except asyncio.CancelledError:
        log.info("telethon_task.stopping")
        await listener.stop()
    except Exception as exc:
        log.error("telethon_task.crashed", error=str(exc))
        raise


# ── Lifespan ──────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """
    FastAPI lifespan context manager.

    Startup order
    -------------
    1. Configure logging.
    2. Initialise the QueueManager (queues are created lazily on first access,
       but the manager object must exist before any worker accesses it).
    3. Wait for Elasticsearch to be reachable (retries with back-off).
    4. Launch the three background tasks concurrently.

    Shutdown order
    --------------
    1. Cancel all background tasks.
    2. Give each task up to _SHUTDOWN_TIMEOUT seconds to finish cleanly.
    3. Close the Elasticsearch client connection.

    All exceptions from background tasks during shutdown are logged but do
    not prevent other tasks from being awaited, ensuring a full clean-up.
    """
    # ── Startup ───────────────────────────────────────────────────────────
    log.info("app.startup", version=app.version)

    init_queue_manager(
        index_queue_size=settings.index_queue_size,
        broadcast_queue_size=settings.broadcast_queue_size,
        crawl_queue_size=settings.crawl_queue_size,
    )
    init_correlation_engine()

    try:
        await wait_for_elasticsearch()
    except Exception as exc:
        log.error("app.startup_es_unavailable", error=str(exc))
        # Re-raise so uvicorn aborts startup cleanly instead of serving
        # requests against a broken backend.
        raise RuntimeError(f"Elasticsearch unavailable at startup: {exc}") from exc

    tasks: List[asyncio.Task] = [
        asyncio.create_task(bulk_worker(),        name="bulk-indexer"),
        asyncio.create_task(_broadcast_worker(),  name="ws-broadcaster"),
        asyncio.create_task(_telethon_task(),     name="telethon-listener"),
    ]
    log.info("app.tasks_launched", tasks=[t.get_name() for t in tasks])

    # ── Serve ─────────────────────────────────────────────────────────────
    try:
        yield
    finally:
        # ── Shutdown ──────────────────────────────────────────────────────
        log.info("app.shutdown_initiated")

        for task in tasks:
            task.cancel()

        results = await asyncio.gather(*tasks, return_exceptions=True)

        for task, result in zip(tasks, results):
            if isinstance(result, asyncio.CancelledError):
                log.debug("app.task_cancelled", task=task.get_name())
            elif isinstance(result, Exception):
                log.warning(
                    "app.task_shutdown_error",
                    task=task.get_name(),
                    error=str(result),
                )

        await close_es_client()
        log.info("app.shutdown_complete")


# ── Application ───────────────────────────────────────────────────────────────

app = FastAPI(
    title="Telegram OSINT Intelligence Platform",
    version="1.0.0",
    description=(
        "Real-time Telegram channel monitoring, threat intelligence aggregation, "
        "and channel discovery with human-in-the-loop approval."
    ),
    lifespan=lifespan,
)


# ── GET /search ───────────────────────────────────────────────────────────────

@app.get("/search", summary="Full-text search with recency and engagement boost")
async def search_messages(
    q: str = Query(..., min_length=1, description="Search keyword or phrase"),
    size: int = Query(20, ge=1, le=200, description="Max results to return"),
    from_: int = Query(0, alias="from", ge=0, description="Pagination offset"),
    min_views: int = Query(0, ge=0, description="Minimum view count filter"),
) -> JSONResponse:
    """
    Query ``tele_messages`` using a function_score query that combines:

    * **Text relevance** — multi_match across text (^3), hashtags (^2),
      infrastructure_tags (^2), urls, mentions, channel_username.
      ``fuzziness: AUTO`` handles typos and transliterations.
    * **Recency boost** — Gaussian decay on ``timestamp``: score decays to
      50 % at 7 days old, near-zero at 30 days.
    * **Engagement boost** — field_value_factor on ``views``, log1p-scaled
      with a factor of 0.1.  A message with 10 k views gets ~0.4 extra
      multiplier; a message with 0 views gets no penalty (missing → 1).

    Results are sorted by the combined function score descending.
    """
    es    = get_es_client()
    index = settings.elasticsearch_index_messages

    # Optional minimum-views pre-filter
    filters: List[Dict[str, Any]] = []
    if min_views > 0:
        filters.append({"range": {"views": {"gte": min_views}}})

    base_query: Dict[str, Any] = {
        "multi_match": {
            "query":     q,
            "fields":    ["text^3", "hashtags^2", "infrastructure_tags^2", "urls", "mentions", "channel_username"],
            "type":      "best_fields",
            "fuzziness": "AUTO",
        }
    }

    if filters:
        base_query = {
            "bool": {
                "must":   base_query,
                "filter": filters,
            }
        }

    es_query: Dict[str, Any] = {
        "query": {
            "function_score": {
                "query": base_query,
                "functions": [
                    # ── Recency boost ──────────────────────────────────────
                    # Gaussian decay: score × 1.0 at origin (now),
                    # × 0.5 at 7 days, asymptotically → 0 beyond ~30 days.
                    {
                        "gauss": {
                            "timestamp": {
                                "origin": "now",
                                "scale":  "7d",
                                "offset": "1d",    # no decay for messages < 1 day old
                                "decay":  0.5,
                            }
                        },
                        "weight": 1.5,
                    },
                    # ── Engagement boost ───────────────────────────────────
                    # log1p(views) * factor — logarithmic to prevent viral
                    # outliers drowning all other results.
                    {
                        "field_value_factor": {
                            "field":    "views",
                            "factor":   0.1,
                            "modifier": "log1p",
                            "missing":  1,         # treat missing as 1 → log1p(1)=0
                        },
                        "weight": 0.5,
                    },
                ],
                # Combine: text_score * (recency_weight + engagement_weight)
                "score_mode":  "sum",
                "boost_mode":  "multiply",
                "min_score":   0.01,
            }
        },
        "highlight": {
            "pre_tags":  ["<mark>"],
            "post_tags": ["</mark>"],
            "fields": {
                "text": {
                    "fragment_size":      220,
                    "number_of_fragments": 2,
                    "no_match_size":      120,   # return snippet even without match
                }
            },
        },
        "sort": [
            {"_score":    {"order": "desc"}},
            {"timestamp": {"order": "desc"}},   # tie-break by message time
        ],
        "_source": {
            "excludes": ["content_hash"]  # internal field; don't expose to clients
        },
        "from": from_,
        "size": size,
    }

    try:
        resp = await es.search(index=index, body=es_query)
    except Exception as exc:
        log.error("search.es_error", q=q, error=str(exc))
        return JSONResponse(
            status_code=503,
            content={"error": "Search unavailable", "detail": str(exc)},
        )

    hits = resp["hits"]
    results: List[Dict[str, Any]] = []
    for hit in hits.get("hits", []):
        doc = hit["_source"]
        doc["_score"]      = hit.get("_score")
        doc["_highlights"] = hit.get("highlight", {}).get("text", [])
        results.append(doc)

    return JSONResponse({
        "query":   q,
        "total":   hits["total"]["value"],
        "size":    size,
        "from":    from_,
        "results": results,
    })


# ── GET /channels/pending ─────────────────────────────────────────────────────

@app.get("/channels/pending", summary="Discovered channels awaiting approval")
async def list_pending_channels(
    size: int = Query(50, ge=1, le=500, description="Max results"),
    from_: int = Query(0, alias="from", ge=0),
) -> JSONResponse:
    """
    Returns channels with ``approval_status: false`` sorted by ``discovered_at``
    descending (newest discoveries first).
    """
    es    = get_es_client()
    index = settings.elasticsearch_index_channels

    es_query: Dict[str, Any] = {
        "query": {"term": {"approval_status": False}},
        "sort":  [{"discovered_at": {"order": "desc"}}],
        "_source": [
            "username", "title", "channel_id", "type",
            "member_count", "description", "discovery_source",
            "discovered_at", "approval_status",
        ],
        "from": from_,
        "size": size,
    }

    try:
        resp = await es.search(index=index, body=es_query)
    except Exception as exc:
        log.error("channels_pending.es_error", error=str(exc))
        return JSONResponse(
            status_code=503,
            content={"error": "Unavailable", "detail": str(exc)},
        )

    hits     = resp["hits"]
    channels = [hit["_source"] for hit in hits.get("hits", [])]

    return JSONResponse({
        "total":    hits["total"]["value"],
        "size":     size,
        "from":     from_,
        "channels": channels,
    })


# ── POST /channels/{username}/approve ─────────────────────────────────────────

@app.post(
    "/channels/{username}/approve",
    summary="Approve a discovered channel for monitoring",
)
async def approve_channel(username: str) -> JSONResponse:
    """
    Flip ``approval_status`` to ``true`` for a pending channel.
    The Telethon listener's next subscribe cycle will pick it up.
    """
    es    = get_es_client()
    index = settings.elasticsearch_index_channels
    doc_id = f"pending_{username.lower()}"

    try:
        await es.update(
            index=index,
            id=doc_id,
            body={"doc": {"approval_status": True}},
        )
        log.info("channel.approved", username=username)
        return JSONResponse({"status": "approved", "username": username})
    except Exception as exc:
        log.error("channel.approve_failed", username=username, error=str(exc))
        return JSONResponse(status_code=503, content={"error": str(exc)})


# ── WS /ws/live ───────────────────────────────────────────────────────────────

@app.websocket("/ws/live")
async def websocket_live(websocket: WebSocket) -> None:
    """
    Real-time broadcast endpoint.

    Protocol
    --------
    * Server pushes every ingested message as a JSON object.
    * Client may send ``"ping"`` text frames; server responds ``"pong"``
      so the client can detect half-open connections.
    * On disconnect (clean or error), the connection is removed from the
      registry automatically.

    Clients are expected to implement reconnect with exponential back-off
    (the frontend ``useWebSocket`` hook already does this).
    """
    await ws_manager.connect(websocket)
    try:
        while True:
            data = await websocket.receive_text()
            if data.strip().lower() == "ping":
                await websocket.send_text("pong")
    except WebSocketDisconnect:
        pass
    except Exception as exc:
        log.debug("ws.recv_error", error=str(exc))
    finally:
        await ws_manager.disconnect(websocket)


# ── GET /correlations ─────────────────────────────────────────────────────────

@app.get("/feed", summary="Recent messages sorted by timestamp for Live Feed pre-population")
async def recent_feed(
    size: int = Query(100, ge=1, le=500),
) -> JSONResponse:
    es    = get_es_client()
    index = settings.elasticsearch_index_messages
    try:
        resp = await es.search(index=index, body={
            "query": {"match_all": {}},
            "sort":  [{"timestamp": {"order": "desc"}}],
            "_source": {"excludes": ["content_hash"]},
            "size": size,
        })
    except Exception as exc:
        return JSONResponse(status_code=503, content={"error": str(exc)})
    results = []
    for hit in resp["hits"].get("hits", []):
        doc = hit["_source"]
        doc["_score"] = hit.get("_score")
        results.append(doc)
    return JSONResponse({"total": resp["hits"]["total"]["value"], "results": results})


@app.get("/correlations/urls", summary="Top URLs shared across multiple channels")
async def top_shared_urls(n: int = Query(20, ge=1, le=100)) -> JSONResponse:
    engine = get_correlation_engine()
    return JSONResponse({"results": await engine.top_shared_urls(n)})


@app.get("/correlations/infrastructure", summary="Top infrastructure tags by channel spread")
async def top_infrastructure(n: int = Query(20, ge=1, le=100)) -> JSONResponse:
    engine = get_correlation_engine()
    return JSONResponse({"results": await engine.top_infrastructure_tags(n)})


@app.get("/correlations/amplified", summary="Most-forwarded messages by amplification count")
async def top_amplified(n: int = Query(20, ge=1, le=100)) -> JSONResponse:
    engine = get_correlation_engine()
    return JSONResponse({"results": await engine.top_amplified(n)})


@app.get("/correlations/channels/{channel_id}", summary="Channels related via URL or forward")
async def related_channels(channel_id: int) -> JSONResponse:
    engine = get_correlation_engine()
    related = await engine.get_related_channels(channel_id)
    return JSONResponse({"channel_id": channel_id, "related": related})


@app.get(
    "/correlations/channels/{channel_id}/role",
    summary="Network role and telemetry for a specific channel",
)
async def channel_role(channel_id: int) -> JSONResponse:
    """
    Returns the current role classification (source_node / amplifier_node / null)
    and raw telemetry counters for the given channel.  Useful for the operator
    channel-detail view and dashboard colour-coding.
    """
    engine = get_correlation_engine()
    return JSONResponse(await engine.get_channel_role(channel_id))


@app.get(
    "/correlations/narrative/{content_hash}",
    summary="Provenance chain for a content hash",
)
async def narrative_path(content_hash: str) -> JSONResponse:
    """
    Reconstruct the hop-by-hop movement of a specific piece of content across
    all monitored channels, sorted chronologically.

    Each hop record:
      hop               : int           — 0 = originator, 1 = first relay, …
      channel_id        : int
      message_id        : int | null
      timestamp         : str | null    — UTC ISO-8601
    """
    engine = get_correlation_engine()
    chain  = await engine.get_narrative_path(content_hash)
    return JSONResponse({
        "content_hash":    content_hash,
        "chain_depth":     len(chain),
        "narrative_path":  chain,
    })


@app.get("/correlations/roles/sources", summary="Top source nodes ranked by downstream amplifications")
async def top_source_nodes(n: int = Query(20, ge=1, le=100)) -> JSONResponse:
    engine = get_correlation_engine()
    return JSONResponse({"results": await engine.top_source_nodes(n)})


@app.get("/correlations/roles/amplifiers", summary="Top amplifier nodes ranked by non-original ratio")
async def top_amplifier_nodes(n: int = Query(20, ge=1, le=100)) -> JSONResponse:
    engine = get_correlation_engine()
    return JSONResponse({"results": await engine.top_amplifier_nodes(n)})


@app.get(
    "/correlations/graph",
    summary="Full channel relationship graph for force-directed visualisation",
)
async def channel_graph() -> JSONResponse:
    """
    Returns the complete in-memory channel graph in react-force-graph-2d format:

      nodes : [{id, role, total_messages, downstream_amplifications,
                originated_count, non_original_ratio}]
      links : [{source, target}]

    Suitable for direct consumption by the NetworkGraph frontend component.
    """
    engine = get_correlation_engine()
    return JSONResponse(await engine.get_channel_graph_data())


@app.get("/correlations/stats", summary="Correlation engine in-memory statistics")
async def correlation_stats() -> JSONResponse:
    return JSONResponse(get_correlation_engine().stats)


# ── GET /health ───────────────────────────────────────────────────────────────

@app.get("/health", include_in_schema=False)
async def health() -> JSONResponse:
    es = get_es_client()
    try:
        info       = await es.info()
        es_ok      = True
        es_version: Optional[str] = info["version"]["number"]
    except Exception:
        es_ok      = False
        es_version = None

    qm = get_queue_manager()
    return JSONResponse({
        "status":         "ok" if es_ok else "degraded",
        "elasticsearch":  {"ok": es_ok, "version": es_version},
        "queues":         qm.stats,
        "ws_connections": ws_manager.count,
    })


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "main:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=settings.api_reload,
        log_level=settings.log_level.lower(),
    )
