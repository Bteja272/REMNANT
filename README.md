# 🌆 REMNANT
### Event-Driven Post-Apocalyptic Game State Engine

[![CI](https://github.com/Bteja272/remnant/actions/workflows/ci.yml/badge.svg)](https://github.com/Bteja272/remnant/actions)
![TypeScript](https://img.shields.io/badge/TypeScript-5.0%2B-3178C6?style=flat&logo=typescript&logoColor=white)
![C++](https://img.shields.io/badge/C%2B%2B-Consequence_Engine-00599C?style=flat&logo=cplusplus&logoColor=white)
![Kafka](https://img.shields.io/badge/Kafka-Event_Streaming-231F20?style=flat&logo=apachekafka&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-TimescaleDB-336791?style=flat&logo=postgresql&logoColor=white)
![Redis](https://img.shields.io/badge/Redis-Cache_%2B_PubSub-DC382D?style=flat&logo=redis&logoColor=white)
![Prometheus](https://img.shields.io/badge/Prometheus-Metrics-E6522C?style=flat&logo=prometheus&logoColor=white)
![Grafana](https://img.shields.io/badge/Grafana-Dashboard-F46800?style=flat&logo=grafana&logoColor=white)
![React](https://img.shields.io/badge/React-TypeScript_Dashboard-61DAFB?style=flat&logo=react&logoColor=black)
![License](https://img.shields.io/badge/License-MIT-green?style=flat)

> You donate medicine to Elias. A Kafka event fires. The TypeScript worker calls the **C++ consequence engine**. Faction reputations propagate. Elias stores a memory. A world flag flips. PostgreSQL commits. Redis refreshes. A WebSocket pushes the update. Prometheus records it. Grafana visualizes it. The map changes.
>
> Every player choice is a distributed systems event. REMNANT makes that visible.

---

## What Is This

REMNANT is a **distributed game-state simulation engine** for a consequence-driven post-apocalyptic world — not a static mockup, not a tutorial CRUD app.

The architecture uses **TypeScript for service orchestration** and a **C++ executable for game-state consequence simulation** — a hybrid pattern that mirrors real game backends where performance-critical logic is separated from service coordination.

The system is fully observable — Prometheus scrapes metrics from both the API and worker, Grafana visualizes pipeline health, C++ engine usage, fallback frequency, and processing latency.

```
React Dashboard
     ↓  POST /actions
Fastify TypeScript API  ←→  WebSocket /ws/players/:id
     ↓  Kafka: player.action.created
TypeScript Consequence Worker
     ↓  child_process JSON IPC
C++ Consequence Engine       ← fails → TypeScript fallback
     ↓
PostgreSQL / TimescaleDB     ← durable state
Redis Cache + Pub/Sub        ← active state + live push
     ↓  WebSocket
React Live Dashboard

Prometheus ← /metrics (API + Worker)
     ↓
Grafana Dashboard
```

---

## One Choice. Many Consequences.

```
Player donates medicine to Elias (Survivors)
     ↓
C++ engine calculates direct delta:    Survivors  +10
Worker propagates faction relations:   Raiders    -7  (hostile to Survivors)
                                       Traders    +3  (allied with Survivors)
                                       The Order   0
Elias memory:                          DONATED_RESOURCE → behavior: OFFERS_HELP
World flag:                            survivors_clinic_supplied = true
Redis cache:                           player_001:reputation refreshed
WebSocket:                             PLAYER_STATE_UPDATED pushed to dashboard
Prometheus:                            remnant_worker_consequence_engine_total +1
Grafana:                               processing duration recorded
```

Rob Mara the next session:
```
Traders reputation     -20
Mara memory            ROBBED_BY_PLAYER → behavior: LOCK_DOORS
World flag             trader_market_unstable = true
```

The world never resets. Every NPC remembers. Every flag persists.

---

## 🛠️ Tech Stack

| Layer | Technology |
|---|---|
| Frontend | React · TypeScript · Vite · Recharts · SVG world map |
| API | Node.js · TypeScript · Fastify · Zod |
| Event Streaming | Apache Kafka · KafkaJS |
| Consequence Engine | **C++** (JSON IPC via child_process) |
| Engine Fallback | TypeScript consequence logic |
| Worker | TypeScript async event processor |
| Database | PostgreSQL · TimescaleDB hypertable |
| Cache / PubSub | Redis |
| Live Updates | WebSockets + Redis Pub/Sub |
| Observability | Prometheus · Grafana · prom-client |
| Testing | Vitest |
| CI/CD | GitHub Actions |
| Infrastructure | Docker Compose · npm workspaces |

---

## ✅ What's Built

**Backend pipeline**
- Fastify API with Zod-validated shared TypeScript event schemas
- Kafka producer publishing `player.action.created` events
- TypeScript async worker consuming Kafka events
- C++ consequence engine called via child_process JSON IPC
- TypeScript fallback if C++ fails, times out, or returns malformed output
- Faction reputation propagation through configurable relationship graph
- NPC memory system with behavior state machine
- World-state flag system with persistent consequences
- Scheduled world events triggering system-generated consequences
- Consequence preview API — see impacts before committing

**Multiplayer**
- Multiple independent players with separate reputation, memory, and world flags
- Player switching in the dashboard
- Shared world API aggregating global activity across all players
- Per-player WebSocket channels

**Observability**
- Prometheus metrics on API and worker independently
- Grafana dashboard provisioned via Docker Compose
- Tracks: request volume, action submissions, C++ engine calls, fallback triggers, processing latency

**Testing and CI**
- Vitest unit tests covering shared schemas, consequence rules, and C++ engine output behavior
- GitHub Actions CI pipeline: install → build shared → build C++ → run tests → build TypeScript

**Frontend**
- React + TypeScript playable dashboard
- Branching text-choice scenario (Dusthaven Clinic)
- SVG faction world map with territory markers and event flags
- NPC behavior panel, world-state panel, scheduled events panel, choice timeline
- Consequence preview before committing
- Live event feed via WebSocket
- Shared world dashboard showing cross-player activity

---

## 🏗️ Key Technical Decisions

**Why C++ for consequence calculation?**
Game consequence logic — reputation deltas, NPC memory scoring, world flag evaluation — is computation, not I/O. C++ handles this faster than TypeScript and separates simulation logic from service orchestration, mirroring how real game backends are structured.

**Why TypeScript fallback?**
C++ processes can crash, timeout, or return malformed JSON. A fallback that catches these failures silently and continues processing means the pipeline is fault-tolerant at the simulation layer. Prometheus tracks fallback frequency — if it spikes, something is wrong with the C++ build.

**Why Kafka instead of direct DB writes?**
Decouples ingestion from consequence processing. The API accepts an action and returns `accepted` immediately — the worker handles reputation propagation, NPC memory, Redis refresh, and WebSocket notification asynchronously. No blocking on the request path.

**Why Redis pub/sub over polling?**
After the worker commits state, it publishes to `channel:player_001:updates`. The API WebSocket handler subscribes and forwards immediately. The dashboard updates the moment the worker finishes — no polling loop, no timed refresh.

**Why TimescaleDB for choice events?**
Choice history is time-series data. TimescaleDB hypertables partition on `occurred_at` automatically, keeping historical queries fast as event volume grows without changing the PostgreSQL interface.

**Why Prometheus on both API and worker independently?**
A metric that only covers the API cannot tell you whether Kafka events are being processed. Instrumenting both services independently means you can distinguish between "API is down" and "worker is down" — two completely different failure modes requiring different responses.

**Why scheduled world events in the worker?**
Some world consequences are not triggered by player actions — faction conflicts, supply depletions, market shifts. Running these as a scheduled loop inside the worker keeps world simulation active even when no player is interacting, making the world feel alive independently of player presence.

---

## 📊 Observability

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
remnant_worker_processing_duration_seconds
```

Grafana is provisioned automatically via Docker Compose.

Access at `http://localhost:3001` · Login: `admin / admin`

---

## 📋 Database Schema

| Table | Purpose |
|---|---|
| `players` | Player profiles |
| `factions` | Survivors · Raiders · Traders · The Order |
| `faction_relationships` | Reputation propagation rules |
| `player_faction_reputation` | Per-player faction scores |
| `choice_events` | TimescaleDB hypertable — durable player action history |
| `npcs` | NPC metadata |
| `npc_memory` | Per-player NPC memories |
| `world_state_flags` | Persistent world consequences |
| `system_world_events` | Scheduled/system-generated world events |

---

## 🔌 API Reference

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/health` | API · DB · Redis · Kafka · WebSocket · metrics |
| `POST` | `/actions` | Submit player choice → Kafka event |
| `POST` | `/actions/preview` | Preview consequences without committing |
| `GET` | `/players` | List all players |
| `POST` | `/players` | Create or update a player |
| `GET` | `/players/:id/state` | Full player state |
| `GET` | `/players/:id/reputation` | Faction reputation |
| `GET` | `/players/:id/timeline` | Choice history |
| `GET` | `/players/:id/world-state` | Player world-state flags |
| `GET` | `/players/:id/world-events` | Scheduled world events |
| `GET` | `/npcs/:id/behavior?playerId=` | NPC behavior from memory |
| `GET` | `/world/state` | Global world-state flags |
| `GET` | `/world/shared-state` | Cross-player shared activity |
| `GET` | `/metrics` | Prometheus metrics (API) |
| `WS` | `/ws/players/:id` | Live consequence updates |

---

## 🚀 Quick Start

```bash
# Install and build
npm install
npm run build:cpp
npm run build

# Run tests
npm test

# Start full stack
docker compose up --build
```

| Service | URL |
|---|---|
| Dashboard | `http://localhost:5173` |
| API | `http://localhost:3000` |
| API Metrics | `http://localhost:3000/metrics` |
| Worker Metrics | `http://localhost:9101/metrics` |
| Prometheus | `http://localhost:9090` |
| Grafana | `http://localhost:3001` |

---

## 🎬 Demo Walkthrough

```bash
# 1. Submit a choice for player_001
curl -X POST http://localhost:3000/actions \
  -H "Content-Type: application/json" \
  -d '{"playerId":"player_001","actionType":"DONATE_RESOURCE","targetFaction":"Survivors","npcId":"elias","metadata":{"resource":"medicine","amount":2}}'

# 2. Check NPC behavior changed
curl "http://localhost:3000/npcs/elias/behavior?playerId=player_001"

# 3. Create player_002 and submit a different action
curl -X POST http://localhost:3000/players \
  -H "Content-Type: application/json" \
  -d '{"id":"player_002","displayName":"Second Survivor"}'

curl -X POST http://localhost:3000/actions \
  -H "Content-Type: application/json" \
  -d '{"playerId":"player_002","actionType":"ATTACK_FACTION","targetFaction":"Raiders","npcId":"knox","metadata":{}}'

# 4. Check shared world state across both players
curl http://localhost:3000/world/shared-state

# 5. Check Prometheus metrics
curl http://localhost:3000/metrics | grep remnant
```

Open Grafana at `http://localhost:3001` to see API traffic, worker throughput, and C++ engine usage.

---

## 🎮 Playable Scenario

```
The Clinic Is Running Out of Medicine
    ├── Give medicine to Elias  → The Clinic Survives the Night
    ├── Rob Mara's supplies     → The Trade Post Locks Down
    ├── Spare Knox              → Knox Leaves a Warning Mark
    └── Attack the checkpoint   → The Checkpoint Burns
```

Each branch submits a real backend action. Consequences persist across scenario resets.

---

## 📁 Repository Structure

```
remnant/
  apps/
    api/                   Fastify API · Kafka producer · WebSockets · Prometheus
    worker/                Kafka consumer · C++ caller · scheduled events · fallback
    dashboard/             React · TypeScript · SVG map · multiplayer · shared world
  packages/
    shared/                Zod schemas for player actions and world events
  engines/
    consequence-cpp/
      src/main.cpp         C++ consequence simulation engine
      build/               Compiled executable
  infra/
    db/init.sql            PostgreSQL / TimescaleDB schema and seed data
    monitoring/            Prometheus configuration
    grafana/               Grafana provisioning and dashboards
  .github/
    workflows/ci.yml       GitHub Actions CI pipeline
  docker-compose.yml
  vitest.config.ts
```

---

## 🗺️ Roadmap

- [ ] Weighted NPC memory scoring — accumulated history over multiple interactions
- [ ] Faction relationships moved to database config — modifiable without code deploy
- [ ] Event sourcing replay — reconstruct world state from choice history
- [ ] Player session authentication
- [ ] Cloud deployment (AWS ECS or Kubernetes)
- [ ] Additional factions, NPCs, and scenario branches
- [ ] Demo GIF / video walkthrough

---

## 📝 License

MIT

---

> REMNANT demonstrates that consequence-driven game simulation is a distributed systems problem. Kafka event streaming, TypeScript service orchestration, C++ simulation with fallback resilience, PostgreSQL/TimescaleDB persistence, Redis caching and pub/sub, WebSocket live delivery, and Prometheus/Grafana observability — applied to a post-apocalyptic world inspired by Detroit: Become Human and Red Dead Redemption 2.