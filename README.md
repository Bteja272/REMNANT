# 🌆 REMNANT: Event-Driven Game State Simulation Engine

[![CI](https://github.com/Bteja272/remnant/actions/workflows/ci.yml/badge.svg)](https://github.com/Bteja272/remnant/actions)
![TypeScript](https://img.shields.io/badge/TypeScript-5.0%2B-3178C6?style=flat&logo=typescript&logoColor=white)
![Kafka](https://img.shields.io/badge/Apache_Kafka-Event_Streaming-231F20?style=flat&logo=apachekafka&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-green?style=flat)

> A distributed, event-driven game-state backend where every player choice generates a Kafka event that propagates consequences across faction reputations, NPC memory, and world-state flags — persisted in PostgreSQL/TimescaleDB and cached in Redis. Inspired by the consequence systems in Detroit: Become Human and Red Dead Redemption 2.

---

## 📌 Overview

REMNANT is the backend simulation engine for a choice-driven post-apocalyptic world. It is not a full 3D game — it is the distributed systems layer that makes a consequence-driven world work.

Every player action flows through a complete event pipeline:

```text
Player Choice
     ↓
Fastify TypeScript API
     ↓
Kafka Event Stream (player.action.created)
     ↓
TypeScript Consequence Worker
     ↓
Faction Reputation Propagation
NPC Memory Creation
World-State Flag Updates
     ↓
Read APIs expose live world state
```

**Example:** A player donates medicine to the Survivors near NPC Elias:
- Survivors reputation increases
- Raiders reputation decreases through relationship propagation
- Traders reputation increases slightly
- Elias stores a `DONATED_RESOURCE` memory and offers help on future encounters
- `survivors_clinic_supplied` world-state flag becomes `true`

---

## 🏗️ Key Technical Decisions

**Why Kafka for player actions instead of direct DB writes?**
Player choices need to be decoupled from consequence processing. A direct write approach would tightly couple action ingestion speed to consequence calculation time. Kafka allows the API to accept actions instantly, while the worker processes consequences asynchronously — the same pattern used in real MMO backends.

**Why TimescaleDB for choice events?**
Choice history is fundamentally time-series data — every event has a timestamp and queries are almost always time-ordered ("what did this player do in the last 10 sessions?"). TimescaleDB hypertables provide automatic time-based partitioning for efficient historical queries without changing the PostgreSQL interface.

**Why a separate consequence worker instead of processing in the API?**
Consequence calculation involves multiple database writes — reputation updates, NPC memory creation, world-state flag changes, faction relationship propagation. Processing this synchronously in the API would block the request and create partial-write failure risks. The worker handles consequences transactionally after the Kafka event is confirmed.

**Why Zod for event schema validation?**
Event schemas shared between the API and worker are the most common source of bugs in event-driven systems. Defining schemas once in the shared package and validating at both the API boundary and worker input prevents schema drift and makes debugging significantly easier.

---

## 🛠️ Tech Stack

| Layer | Technology |
|---|---|
| API | Node.js · TypeScript · Fastify |
| Event Streaming | Apache Kafka · KafkaJS |
| Consequence Worker | TypeScript |
| Validation | Zod |
| Database | PostgreSQL · TimescaleDB |
| Cache | Redis |
| Infrastructure | Docker Compose |

---

## 📡 System Architecture

```text
Client / curl / React UI (planned)
        ↓
POST /actions
        ↓
Fastify API → Zod validation
        ↓
Kafka topic: player.action.created
        ↓
TypeScript Consequence Worker
        ↓
┌───────────────────────────────────┐
│  Faction Reputation Propagation   │
│  NPC Memory Creation              │
│  World-State Flag Updates         │
│  Choice Event Persistence         │
└───────────────────────────────────┘
        ↓
PostgreSQL / TimescaleDB
        ↓
Redis Cache (active world state)
        ↓
Read APIs → GET /players/:id/state
```

---

## ✨ Features

### ⚡ Event-Driven Choice Pipeline
- Player actions publish typed Kafka events validated with Zod
- Consequence worker processes events asynchronously
- Choice events persisted as TimescaleDB hypertable for time-ordered history
- Full end-to-end flow from API call to world state update

### 🏴 Faction Reputation System
- Direct reputation changes applied to target faction on player action
- Secondary effects propagate automatically through faction relationship graph
- Four factions with configurable relationship weights: Survivors, Raiders, Traders, The Order

### 🧠 NPC Memory System
- NPCs store typed memories of significant player interactions
- Memories drive behavior changes on future encounters
- Behavior API returns current NPC disposition based on accumulated memory

### 🌍 World-State Flag System
- Persistent boolean flags capturing global world consequences
- Actions trigger flag changes that affect future game scenarios
- Player and global world-state queryable via dedicated APIs

### 📊 Player State APIs
- Combined player state endpoint returning reputation, timeline, and world flags in one call
- Choice history timeline for reviewing player decisions
- Per-faction reputation breakdown

---

## 🎮 Consequence Rules

### Faction Reputation Propagation

Player actions affect the target faction directly, then propagate through faction relationships:

```
HELP_FACTION Survivors
    → Survivors  +15
    → Raiders    -11  (enemies of Survivors)
    → Traders    +3   (allied with Survivors)
    → The Order  0    (neutral)
```

### NPC Memory

| Action | NPC | Memory | Behavior |
|---|---|---|---|
| `DONATE_RESOURCE` near Elias | Elias | `DONATED_RESOURCE` | `OFFERS_HELP` |
| `ROB_NPC` Mara | Mara | `ROBBED_BY_PLAYER` | `LOCK_DOORS` |
| `SPARE_ENEMY` Knox | Knox | `SPARED_BY_PLAYER` | `SHOWS_MERCY` |

### World-State Flags

| Action | Flag | Value |
|---|---|---|
| Donate medicine to Survivors | `survivors_clinic_supplied` | `true` |
| Rob Mara | `trader_market_unstable` | `true` |
| Spare Knox | `raider_checkpoint_ambush_disabled` | `true` |
| Attack Raiders | `raider_checkpoint_hostile` | `true` |

---

## 🗃️ Database Schema

```text
players
factions
faction_relationships
player_faction_reputation
choice_events              ← TimescaleDB hypertable (occurred_at)
npcs
npc_memory
world_state_flags
```

**Seeded data:**
- Player: `player_001`
- Factions: `survivors` · `raiders` · `traders` · `order`
- NPCs: `elias` · `mara` · `knox`

---

## 📁 Repository Structure

```text
remnant/
  apps/
    api/                   Fastify TypeScript API
    worker/                Kafka consequence worker
  packages/
    shared/                Typed event schemas (Zod)
  infra/
    db/
      init.sql             Database schema
  docker-compose.yml
  README.md
```

---

## 🚀 Quick Start

### Prerequisites
- Docker Desktop
- Node.js 20+ (via nvm in WSL recommended)

```bash
# Install dependencies
npm install

# Build all workspaces
npm run build

# Start full stack
docker compose up --build
```

**Health check:**
```bash
curl http://localhost:3000/health
```
```json
{
  "status": "ok",
  "service": "remnant-api",
  "kafkaBroker": "remnant-kafka:29092",
  "database": "connected"
}
```

> If you change the database schema, reset the volume with `docker compose down -v` before rebuilding.

---

## 🔌 API Reference

### Actions

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/actions` | Submit a player choice |

```bash
curl -X POST http://localhost:3000/actions \
  -H "Content-Type: application/json" \
  -d '{
    "playerId": "player_001",
    "actionType": "DONATE_RESOURCE",
    "targetFaction": "Survivors",
    "npcId": "elias",
    "metadata": { "resource": "medicine", "amount": 2 }
  }'
```

**Supported action types:** `HELP_FACTION` · `ROB_NPC` · `SPARE_ENEMY` · `ATTACK_FACTION` · `DONATE_RESOURCE`

### Player State

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/players/:id/state` | Full player state (reputation + timeline + world flags) |
| `GET` | `/players/:id/reputation` | Current faction reputation |
| `GET` | `/players/:id/timeline` | Choice history |
| `GET` | `/players/:id/world-state` | Player world-state flags |

### NPCs and World

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/npcs/:id/behavior?playerId=` | NPC behavior based on player memory |
| `GET` | `/world/state` | Global world-state flags |

---

## 🎬 Demo Scenario

Run these three actions to demonstrate the full consequence pipeline:

```bash
# 1. Donate medicine to Survivors (Elias remembers, clinic opens)
curl -X POST http://localhost:3000/actions \
  -H "Content-Type: application/json" \
  -d '{"playerId":"player_001","actionType":"DONATE_RESOURCE","targetFaction":"Survivors","npcId":"elias","metadata":{"resource":"medicine","amount":2}}'

# 2. Rob Mara the trader (market destabilizes, Mara locks doors)
curl -X POST http://localhost:3000/actions \
  -H "Content-Type: application/json" \
  -d '{"playerId":"player_001","actionType":"ROB_NPC","targetFaction":"Traders","npcId":"mara","metadata":{"resource":"water","amount":1}}'

# 3. Spare Knox the raider (checkpoint disarms, Knox shows mercy)
curl -X POST http://localhost:3000/actions \
  -H "Content-Type: application/json" \
  -d '{"playerId":"player_001","actionType":"SPARE_ENEMY","targetFaction":"Raiders","npcId":"knox","metadata":{"location":"raider_checkpoint"}}'

# Verify NPC behaviors changed
curl "http://localhost:3000/npcs/elias/behavior?playerId=player_001"   # OFFERS_HELP
curl "http://localhost:3000/npcs/mara/behavior?playerId=player_001"    # LOCK_DOORS
curl "http://localhost:3000/npcs/knox/behavior?playerId=player_001"    # SHOWS_MERCY

# Verify full world state
curl http://localhost:3000/players/player_001/state
curl http://localhost:3000/world/state
```

---

## 🗺️ Roadmap

- [ ] Redis-backed active player/world cache for sub-millisecond state reads
- [ ] WebSocket updates pushing live consequence events to frontend
- [ ] React + TypeScript playable dashboard with choice UI and live world map
- [ ] C++ consequence simulation engine replacing TypeScript consequence logic
- [ ] GitHub Actions CI pipeline
- [ ] Multi-player shared world state
- [ ] Additional factions, NPCs, and consequence rules
- [ ] Demo video/GIF

---

## 📝 License

MIT License

---

> Built as a production-style distributed game-state simulation engine demonstrating event-driven architecture with Kafka, TypeScript, PostgreSQL/TimescaleDB, Redis, and consequence-propagation systems inspired by Detroit: Become Human and Red Dead Redemption 2.