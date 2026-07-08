# 🌆 REMNANT
### Event-Driven Post-Apocalyptic Game State Engine

[![CI](https://github.com/Bteja272/remnant/actions/workflows/ci.yml/badge.svg)](https://github.com/Bteja272/remnant/actions)
![TypeScript](https://img.shields.io/badge/TypeScript-5.0%2B-3178C6?style=flat&logo=typescript&logoColor=white)
![C++](https://img.shields.io/badge/C%2B%2B-Consequence_Engine-00599C?style=flat&logo=cplusplus&logoColor=white)
![Kafka](https://img.shields.io/badge/Kafka-Event_Streaming-231F20?style=flat&logo=apachekafka&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-TimescaleDB-336791?style=flat&logo=postgresql&logoColor=white)
![Redis](https://img.shields.io/badge/Redis-Cache_%2B_PubSub-DC382D?style=flat&logo=redis&logoColor=white)
![Prometheus](https://img.shields.io/badge/Prometheus-Metrics-E6522C?style=flat&logo=prometheus&logoColor=white)
![React](https://img.shields.io/badge/React-TypeScript_Dashboard-61DAFB?style=flat&logo=react&logoColor=black)
![License](https://img.shields.io/badge/License-MIT-green?style=flat)

> You donate medicine to Elias. A Kafka event fires. The TypeScript worker calls the **C++ consequence engine**. Faction reputations propagate. Elias stores a memory. A world flag flips. PostgreSQL commits. Redis refreshes. A WebSocket pushes the update. Prometheus records it. The map changes.
>
> All in under a second.

---

## What Is This

REMNANT is a **distributed game-state simulation engine** — not a static mockup, not a CRUD app.

The architecture uses **TypeScript for service orchestration** and a **C++ consequence engine for game-state simulation** — a hybrid pattern used in production game backends where performance-critical logic is separated from service coordination.

Every player choice flows through the full pipeline:

```
React Dashboard
     ↓  POST /actions
Fastify TypeScript API
     ↓  Kafka: player.action.created
TypeScript Consequence Worker
     ↓  child_process JSON IPC
C++ Consequence Engine          ← if engine fails → TypeScript fallback
     ↓
PostgreSQL / TimescaleDB        ← durable state
Redis Cache + Pub/Sub           ← active state + live push
     ↓  WebSocket
React Live Dashboard
```

Prometheus metrics are instrumented on both the API and worker — tracking request volume, C++ engine usage, fallback frequency, and processing latency.

---

## The C++ Engine

The consequence engine lives at `engines/consequence-cpp/src/main.cpp`.

The TypeScript worker calls it via Node's `child_process` API, passing the player action event as JSON:

**Input:**
```json
{
  "actionType": "DONATE_RESOURCE",
  "targetFaction": "Survivors",
  "npcId": "elias",
  "currentReputation": { "survivors": 20, "raiders": -10, "traders": 5, "order": 0 },
  "metadata": { "resource": "medicine", "amount": 2 }
}
```

**Output:**
```json
{
  "engine": "cpp",
  "directReputationDelta": 10,
  "npcMemory": {
    "memoryType": "DONATED_RESOURCE",
    "description": "Player donated medicine.",
    "intensity": 7
  },
  "worldFlags": [
    {
      "flagKey": "survivors_clinic_supplied",
      "flagValue": true,
      "description": "Dusthaven Clinic has medicine because the player donated to Survivors."
    }
  ]
}
```

**If the C++ engine fails, times out, or returns malformed output, the worker automatically falls back to TypeScript consequence logic** — the pipeline never stops processing because of a simulation-layer failure.

---

## One Choice. Many Consequences.

```
Player donates medicine to Elias
     ↓
C++ engine calculates direct delta:    Survivors +10
Worker propagates faction relations:   Raiders   -7  (hostile to Survivors)
                                       Traders   +3  (allied with Survivors)
Elias memory:                          DONATED_RESOURCE → behavior: OFFERS_HELP
World flag:                            survivors_clinic_supplied = true
Redis cache:                           refreshed
WebSocket:                             PLAYER_STATE_UPDATED pushed
Prometheus:                            remnant_worker_consequence_engine_total +1
```

---

## Observability

Both the API and worker expose Prometheus-compatible `/metrics` endpoints.

**API metrics:**
```
remnant_api_http_requests_total
remnant_api_actions_submitted_total
remnant_api_action_previews_total
```

**Worker metrics:**
```
remnant_worker_events_processed_total
remnant_worker_consequence_engine_total    ← C++ engine calls
remnant_worker_fallback_total              ← TypeScript fallback triggers
remnant_worker_processing_duration_seconds
```

These answer: Is the pipeline processing events? Is C++ being used? How often does fallback trigger? How long does consequence processing take?

Grafana dashboard provisioning is the next milestone.

---

## 🛠️ Tech Stack

| Layer | Technology |
|---|---|
| Frontend | React · TypeScript · Vite · SVG world map |
| API | Node.js · TypeScript · Fastify · Zod |
| Event Streaming | Apache Kafka · KafkaJS |
| Consequence Engine | **C++** (JSON IPC via child_process) |
| Engine Fallback | TypeScript consequence logic |
| Worker | TypeScript async event processor |
| Database | PostgreSQL · TimescaleDB hypertable |
| Cache / PubSub | Redis |
| Live Updates | WebSockets + Redis Pub/Sub |
| Observability | Prometheus-compatible metrics (prom-client) |
| Infrastructure | Docker Compose · npm workspaces |

---

## 🏗️ Key Technical Decisions

**Why C++ for consequence calculation?**
Game consequence logic — reputation deltas, NPC memory scoring, world flag evaluation — is computation, not I/O. C++ handles this faster than TypeScript and separates simulation logic from service orchestration. This mirrors real game backends where engines are written in C++ and wrapped by higher-level service layers.

**Why TypeScript fallback?**
C++ processes can crash, timeout, or return malformed output. A fallback that silently catches these failures and continues processing means the pipeline is fault-tolerant at the simulation layer. Prometheus tracks fallback frequency — if it spikes, something is wrong with the C++ build.

**Why Kafka instead of direct DB writes?**
Decouples ingestion from consequence processing. The API accepts an action and returns immediately — the worker handles reputation propagation, NPC memory, Redis refresh, and WebSocket notification asynchronously. No blocking on the request path.

**Why Redis pub/sub for WebSocket delivery?**
After the worker commits state, it publishes to `channel:player_001:updates`. The API WebSocket handler subscribes and forwards immediately — no polling loop, no timed refresh. The dashboard updates the moment the worker finishes.

**Why TimescaleDB for choice events?**
Choice history is time-series data. TimescaleDB hypertables partition on `occurred_at` automatically, keeping historical queries fast as event volume grows.

**Why Prometheus on both API and worker?**
A metric that only covers the API doesn't tell you if Kafka events are being processed. Instrumenting both services independently means you can distinguish between "API is down" and "worker is down" — two very different failure modes.

---

## ✅ What's Built

**Backend pipeline**
- Fastify API with Zod-validated shared TypeScript event schemas
- Kafka producer publishing `player.action.created` events
- TypeScript async worker consuming Kafka events
- **C++ consequence engine** called via child_process JSON IPC
- TypeScript fallback if C++ fails or times out
- Faction reputation propagation through relationship graph
- NPC memory system with behavior state machine
- World-state flag system
- Consequence preview API — see impacts before committing
- Prometheus metrics on API and worker

**Live infrastructure**
- Redis active-state cache refreshed after every event
- Redis pub/sub channel per player
- WebSocket endpoint subscribing to Redis and forwarding live

**Frontend**
- React + TypeScript playable dashboard
- Branching text-choice scenario (Dusthaven Clinic)
- SVG faction world map with territory markers and world flags
- NPC behavior panel, world-state panel, choice timeline
- Consequence preview before committing
- Live event feed without page refresh

---

## 🎮 Playable Scenario

```
The Clinic Is Running Out of Medicine
    ├── Give medicine to Elias  → The Clinic Survives the Night
    ├── Rob Mara's supplies     → The Trade Post Locks Down
    ├── Spare Knox              → Knox Leaves a Warning Mark
    └── Attack the checkpoint   → The Checkpoint Burns
```

Each branch submits a real backend action. The world state persists across resets.

---

## 🔌 API Reference

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/health` | API, DB, Redis, Kafka, WebSocket status |
| `POST` | `/actions` | Submit player choice → Kafka event |
| `POST` | `/actions/preview` | Preview consequences without committing |
| `GET` | `/players/:id/state` | Full player state |
| `GET` | `/players/:id/reputation` | Faction reputation scores |
| `GET` | `/players/:id/timeline` | Choice history |
| `GET` | `/players/:id/world-state` | World-state flags |
| `GET` | `/npcs/:id/behavior?playerId=` | NPC behavior from memory |
| `GET` | `/world/state` | Global world-state flags |
| `GET` | `/metrics` | Prometheus metrics |
| `WS` | `/ws/players/:id` | Live consequence updates |

---

## 📁 Repository Structure

```
remnant/
  apps/
    api/           Fastify API · Kafka producer · WebSockets · Prometheus metrics
    worker/        Kafka consumer · C++ engine caller · fallback · Redis refresh
    dashboard/     React · TypeScript · SVG map · branching scenario
  packages/
    shared/        Zod event schemas shared between API and worker
  engines/
    consequence-cpp/
      src/main.cpp              C++ consequence engine
      build/remnant_consequence_engine
  infra/
    db/init.sql    PostgreSQL / TimescaleDB schema and seed data
  docker-compose.yml
```

---

## 🚀 Quick Start

```bash
# Install and build
npm install && npm run build

# Start full stack
docker compose up --build
```

Dashboard: `http://localhost:5173`
API: `http://localhost:3000`
Metrics: `http://localhost:3000/metrics`

```bash
# Health check
curl http://localhost:3000/health

# Submit a choice
curl -X POST http://localhost:3000/actions \
  -H "Content-Type: application/json" \
  -d '{"playerId":"player_001","actionType":"DONATE_RESOURCE","targetFaction":"Survivors","npcId":"elias","metadata":{"resource":"medicine","amount":2}}'

# Check NPC behavior changed
curl "http://localhost:3000/npcs/elias/behavior?playerId=player_001"

# Check Prometheus metrics
curl http://localhost:3000/metrics | grep remnant
```

---

## 🗺️ Roadmap

- [ ] Prometheus + Grafana dashboard provisioning
- [ ] Multi-player shared world state
- [ ] Scheduled world events independent of player actions
- [ ] GitHub Actions CI — TypeScript build, C++ build, API smoke tests
- [ ] Additional factions, NPCs, and scenario branches
- [ ] Demo GIF / video walkthrough

---

## 📝 License

MIT

---

> REMNANT demonstrates that game-state simulation is a distributed systems problem. Kafka event streaming, TypeScript service orchestration, C++ consequence simulation with fallback resilience, PostgreSQL/TimescaleDB persistence, Redis caching and pub/sub, WebSocket live delivery, and Prometheus observability — applied to a consequence-driven post-apocalyptic world.