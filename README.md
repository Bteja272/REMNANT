# 🌆 REMNANT
### Event-Driven Post-Apocalyptic Game State Engine

[![CI](https://github.com/Bteja272/remnant/actions/workflows/ci.yml/badge.svg)](https://github.com/Bteja272/remnant/actions)
![TypeScript](https://img.shields.io/badge/TypeScript-5.0%2B-3178C6?style=flat&logo=typescript&logoColor=white)
![Kafka](https://img.shields.io/badge/Kafka-Event_Streaming-231F20?style=flat&logo=apachekafka&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-TimescaleDB-336791?style=flat&logo=postgresql&logoColor=white)
![Redis](https://img.shields.io/badge/Redis-Cache_%2B_PubSub-DC382D?style=flat&logo=redis&logoColor=white)
![React](https://img.shields.io/badge/React-TypeScript_Dashboard-61DAFB?style=flat&logo=react&logoColor=black)
![License](https://img.shields.io/badge/License-MIT-green?style=flat)

> You donate medicine to Elias. A Kafka event fires. The consequence worker propagates reputation changes across four factions. Elias stores a memory. A world-state flag flips. Redis refreshes. A WebSocket pushes the update. The map changes. All in under a second.
>
> That is REMNANT.

---

## What Is This

REMNANT is a **distributed game-state simulation engine** — not a static mockup, not a CRUD app. Every player choice is a real backend event that flows through Kafka, gets processed asynchronously, and ripples across faction reputations, NPC memory, and world-state flags before being pushed live to a playable React dashboard.

The consequence system is inspired by **Detroit: Become Human** and **Red Dead Redemption 2** — worlds where your decisions permanently shape who trusts you, who fears you, and what the world looks like when you come back.

**Try it in 60 seconds:**
```bash
docker compose up --build
# open http://localhost:5173
# click "Give medicine to Elias"
# watch the world change
```

---

## The Full Pipeline

```
React Playable Scenario
        ↓  POST /actions
Fastify TypeScript API  ←→  WebSocket /ws/players/:id
        ↓  Kafka: player.action.created
TypeScript Consequence Worker
        ↓
┌───────────────────────────────────┐
│  Faction Reputation Propagation   │
│  NPC Memory Creation              │
│  World-State Flag Updates         │
└───────────────────────────────────┘
        ↓                    ↓
PostgreSQL/TimescaleDB    Redis Cache + Pub/Sub
(durable history)         (active state + live push)
        ↓
WebSocket → React Dashboard updates live
```

---

## One Choice. Many Consequences.

```
Player donates medicine to Elias (Survivors)
        ↓
Survivors reputation   +15
Raiders reputation     -11  ← faction relationship propagation
Traders reputation     +3   ← faction relationship propagation
Elias memory           DONATED_RESOURCE → behavior: OFFERS_HELP
World flag             survivors_clinic_supplied = true
Redis cache            refreshed
WebSocket              PLAYER_STATE_UPDATED pushed
SVG map                Survivors territory strengthens
```

Rob Mara the next day:
```
Traders reputation     -20
Mara memory            ROBBED_BY_PLAYER → behavior: LOCK_DOORS
World flag             trader_market_unstable = true
```

Spare Knox the Raider:
```
Raiders reputation     shifts
Knox memory            SPARED_BY_PLAYER → behavior: SHOWS_MERCY
World flag             raider_checkpoint_ambush_disabled = true
```

Every NPC remembers. Every flag persists. The world never resets.

---

## 🛠️ Tech Stack

| Layer | Technology |
|---|---|
| Frontend | React · TypeScript · Vite · Recharts · SVG |
| API | Node.js · TypeScript · Fastify · Zod |
| Event Streaming | Apache Kafka · KafkaJS |
| Consequence Worker | TypeScript async processor |
| Database | PostgreSQL · TimescaleDB hypertable |
| Cache / PubSub | Redis |
| Live Updates | WebSockets + Redis Pub/Sub |
| Infrastructure | Docker Compose · npm workspaces |

---

## ✅ What's Built

**Backend engine**
- Fastify API with Zod-validated shared event schemas
- Kafka producer publishing `player.action.created` events
- Async consequence worker consuming Kafka events
- Faction reputation propagation through relationship graph
- NPC memory system with behavior state machine
- World-state flag system with persistent consequences
- Consequence preview API — see impacts before committing

**Live infrastructure**
- Redis active-state cache refreshed after every event
- Redis pub/sub channel per player (`channel:player_001:updates`)
- WebSocket endpoint subscribing to Redis channel and forwarding live

**Playable frontend**
- React + TypeScript dashboard at `localhost:5173`
- Branching text-choice scenario (Dusthaven Clinic)
- SVG faction world map with territory markers and event flags
- NPC behavior panel, world-state panel, choice timeline
- Consequence preview shown before player commits a choice
- Live event feed updating without page refresh

---

## 🏗️ Key Technical Decisions

**Why Kafka instead of direct DB writes?**
Decouples ingestion speed from consequence processing time. The API accepts a choice instantly and returns — the worker handles reputation propagation, memory creation, and flag updates asynchronously. No blocking, no partial writes visible to the player.

**Why TimescaleDB for choice events?**
Choice history is time-series data. Every query is time-ordered ("what did this player do in the last session?"). TimescaleDB hypertables partition automatically on `occurred_at` — efficient historical queries without changing the PostgreSQL interface.

**Why Redis pub/sub instead of polling?**
The worker publishes to `channel:player_001:updates` after committing state. The WebSocket handler subscribes to that channel and forwards immediately. No polling loop, no latency from timed refreshes — the frontend updates the moment the worker finishes.

**Why a consequence preview API?**
Detroit's genius is showing you the ripple before you commit. `POST /actions/preview` calculates consequence deltas without mutating state — the frontend shows predicted reputation changes before the player confirms. Same logic, zero side effects.

**Why separate the worker from the API?**
Consequence calculation involves multiple sequential writes — reputation updates, NPC memory, world flags, Redis refresh, pub/sub publish. Doing this in the API request handler would block the response and risk partial-write failures visible to the player. The worker handles everything transactionally after the Kafka event is confirmed durable.

---

## 🎮 Playable Scenario — Dusthaven

The dashboard ships with a branching scenario:

```
The Clinic Is Running Out of Medicine
    ├── Give medicine to Elias → The Clinic Survives the Night
    ├── Rob Mara's supplies   → The Trade Post Locks Down
    ├── Spare Knox            → Knox Leaves a Warning Mark
    └── Attack the checkpoint → The Checkpoint Burns
```

Each branch commits a real backend action — not a frontend state toggle. The world remembers even if you reset the scenario view.

---

## 📁 Repository Structure

```
remnant/
  apps/
    api/          Fastify API · Kafka producer · WebSockets · read APIs
    worker/       Kafka consumer · consequence processor · Redis refresh
    dashboard/    React · TypeScript · SVG map · playable scenario
  packages/
    shared/       Zod event schemas shared across API and worker
  infra/
    db/
      init.sql    PostgreSQL / TimescaleDB schema and seed data
  docker-compose.yml
```

---

## 🔌 API Reference

**Submit a choice**
```bash
curl -X POST http://localhost:3000/actions \
  -H "Content-Type: application/json" \
  -d '{"playerId":"player_001","actionType":"DONATE_RESOURCE","targetFaction":"Survivors","npcId":"elias","metadata":{"resource":"medicine","amount":2}}'
```

**Preview consequences before committing**
```bash
curl -X POST http://localhost:3000/actions/preview \
  -H "Content-Type: application/json" \
  -d '{"playerId":"player_001","actionType":"ROB_NPC","targetFaction":"Traders","npcId":"mara","metadata":{"resource":"water","amount":1}}'
```

**Read player state**
```bash
curl http://localhost:3000/players/player_001/state      # full state
curl http://localhost:3000/players/player_001/reputation # faction scores
curl http://localhost:3000/players/player_001/timeline   # choice history
```

**NPC behavior**
```bash
curl "http://localhost:3000/npcs/elias/behavior?playerId=player_001"
curl "http://localhost:3000/npcs/mara/behavior?playerId=player_001"
curl "http://localhost:3000/npcs/knox/behavior?playerId=player_001"
```

**Live WebSocket**
```bash
npx wscat -c ws://localhost:3000/ws/players/player_001
```

Supported actions: `HELP_FACTION` · `ROB_NPC` · `SPARE_ENEMY` · `ATTACK_FACTION` · `DONATE_RESOURCE`

---

## 🗺️ Roadmap

- [ ] C++ consequence simulation engine replacing TypeScript consequence logic
- [ ] Multi-player shared world state — all players affect the same world
- [ ] Grafana observability dashboard — reputation trends, event volume
- [ ] Scheduled world events — faction conflicts firing independently of players
- [ ] GitHub Actions CI — build, test, consequence worker validation
- [ ] Additional factions, NPCs, and branching scenario chapters
- [ ] Demo GIF / video walkthrough

---

## 📝 License

MIT

---

> REMNANT demonstrates that game-state simulation is a distributed systems problem — event streaming, async consequence processing, time-series persistence, active-state caching, and real-time WebSocket delivery — not just a frontend concern. Built with Kafka, TypeScript, PostgreSQL/TimescaleDB, Redis, and React.