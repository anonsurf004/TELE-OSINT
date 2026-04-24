# Telegram OSINT Intelligence Platform - Project Structure

```
TELE/
├── .env                              # Secrets (never committed)
├── .env.example                      # Template with placeholder values
├── config.py                         # Pydantic BaseSettings config
├── requirements.txt                  # Python dependencies
├── es_setup.py                       # Standalone Elasticsearch index bootstrap
├── run.py                            # Main entry point (orchestrator)
├── docker-compose.yml                # ES + backend + frontend services
├── Dockerfile                        # Backend container
│
│
│  ══════════════════════════════════════════════════════════
│  BACKEND — Python / FastAPI / Telethon / Elasticsearch
│  ══════════════════════════════════════════════════════════
│
├── crawler/                          # Telegram data acquisition layer
│   ├── __init__.py
│   ├── telethon_client.py            # Singleton Telethon session manager
│   ├── channel_crawler.py            # Channel discovery + lifecycle mgmt
│   └── event_listener.py             # Real-time NewMessage event handler
│
├── processor/                        # Message & channel enrichment pipeline
│   ├── __init__.py
│   ├── message_processor.py          # Transform raw messages -> ES docs
│   ├── channel_processor.py          # Extract channel metadata for ES
│   ├── metadata_extractor.py         # URLs, hashtags, mentions, media, CVEs
│   ├── correlation_engine.py         # Forward-chain + URL co-occurrence graphs
│   └── deduplicator.py              # Content-hash LRU deduplication
│
├── indexer/                          # Elasticsearch persistence layer
│   ├── __init__.py
│   ├── es_client.py                  # Async ES connection pool (singleton)
│   ├── index_manager.py              # Index creation + mapping definitions
│   └── bulk_indexer.py               # Buffered async bulk indexing
│
├── api/                              # FastAPI REST + WebSocket layer
│   ├── __init__.py
│   ├── main.py                       # FastAPI app factory + lifespan
│   ├── websocket_manager.py          # WebSocket connection broadcaster
│   ├── models.py                     # Pydantic request/response schemas
│   └── routes/
│       ├── __init__.py
│       ├── search.py                 # GET /api/search — full-text + filters
│       ├── channels.py               # GET/POST /api/channels — CRUD + approval
│       └── stats.py                  # GET /api/stats — dashboard aggregations
│
├── utils/                            # Shared utilities
│   ├── __init__.py
│   ├── logger.py                     # Structured logging (structlog + rich)
│   ├── queue_manager.py              # Async inter-module queues
│   └── helpers.py                    # Regex extractors, hashing, date utils
│
│
│  ══════════════════════════════════════════════════════════
│  FRONTEND — React / Tailwind CSS / WebSocket
│  ══════════════════════════════════════════════════════════
│
└── frontend/
    ├── package.json
    ├── tailwind.config.js
    ├── postcss.config.js
    ├── vite.config.js                # Vite dev server + proxy to FastAPI
    ├── index.html
    │
    ├── public/
    │   └── favicon.ico
    │
    └── src/
        ├── main.jsx                  # React root + router mount
        ├── App.jsx                   # Top-level layout + route definitions
        ├── index.css                 # Tailwind directives + global styles
        │
        ├── components/               # Reusable UI components
        │   ├── Navbar.jsx
        │   ├── Sidebar.jsx
        │   ├── MessageCard.jsx       # Single message display
        │   ├── ChannelCard.jsx       # Channel info tile
        │   ├── SearchBar.jsx         # Full-text search input + filters
        │   ├── LiveFeed.jsx          # WebSocket-driven real-time feed
        │   └── StatsPanel.jsx        # Dashboard metric cards
        │
        ├── pages/                    # Route-level page components
        │   ├── Dashboard.jsx         # Overview: live feed + stats
        │   ├── Search.jsx            # Advanced search + results
        │   ├── Channels.jsx          # Channel management + approval
        │   └── ChannelDetail.jsx     # Single channel deep-dive
        │
        ├── hooks/                    # Custom React hooks
        │   ├── useWebSocket.js       # Auto-reconnecting WS connection
        │   └── useSearch.js          # Debounced search w/ ES backend
        │
        ├── services/                 # API client layer
        │   ├── api.js                # Axios/fetch wrapper for REST calls
        │   └── ws.js                 # WebSocket connection manager
        │
        └── utils/
            ├── constants.js          # API base URL, WS URL, etc.
            └── formatters.js         # Date, number, text formatting helpers
```

## Data Flow

```
Telegram Channels
       |
       v
  [crawler/event_listener]  -- real-time messages via Telethon -->
       |
       v
  [processor/message_processor]  -- enrich, deduplicate, extract entities -->
       |
       v
  [indexer/bulk_indexer]  -- buffered bulk writes -->
       |
       v
  Elasticsearch (telegram_messages, telegram_channels)
       |
       ├──> [api/routes/search]  -- REST queries --> React Search page
       ├──> [api/routes/channels] -- channel CRUD --> React Channels page
       ├──> [api/routes/stats]   -- aggregations --> React Dashboard
       └──> [api/websocket_manager] -- live push --> React LiveFeed component
```
