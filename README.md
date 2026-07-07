# REMNANT: Event-Driven Game State Simulation Engine

REMNANT is an event-driven post-apocalyptic game-state simulation backend where player choices create persistent consequences across factions, world state, and future NPC behavior.

The current milestone implements the first working vertical slice:

```text
POST /actions
    ↓
Fastify TypeScript API
    ↓
Kafka topic: player.action.created
    ↓
TypeScript worker
    ↓
PostgreSQL / TimescaleDB event persistence
    ↓
Faction reputation update
Project Goal

The long-term goal is to build a game-systems backend inspired by consequence-heavy games like Detroit: Become Human and Red Dead Redemption 2.

Each player action should ripple through the world:

Helping one faction can make another faction hostile.
NPCs can remember previous interactions.
World-state flags can change based on accumulated decisions.
Choice history can be replayed over time.
A future dashboard will visualize live faction reputation and world-state changes.

This is not a full playable game yet. The current focus is the distributed backend architecture behind a reactive game world.

Current Features
TypeScript Fastify API
Kafka event publishing for player actions
TypeScript Kafka worker
PostgreSQL / TimescaleDB persistence
Redis service running for future active-session state
Docker Compose local infrastructure
Shared TypeScript event schema package
Basic faction reputation mutation
Seeded demo player and factions

Current working behavior:

HELP_FACTION Survivors
    → Kafka event created
    → event saved to choice_events
    → Survivors reputation increases by +15
Tech Stack
Layer	Technology
API	Node.js, TypeScript, Fastify
Event Streaming	Kafka, KafkaJS
Worker	TypeScript
Database	PostgreSQL, TimescaleDB
Cache	Redis
Validation	Zod
Infrastructure	Docker Compose
Future Simulation Engine	C++
Future Frontend	React, TypeScript, WebSockets
Repository Structure
remnant/
  apps/
    api/
      src/
        index.ts
      package.json
      tsconfig.json

    worker/
      src/
        index.ts
      package.json
      tsconfig.json

  packages/
    shared/
      src/
        events/
          playerActionCreated.ts
        index.ts
      package.json
      tsconfig.json

  infra/
    db/
      init.sql

  docker-compose.yml
  package.json
  README.md
  .gitignore
Architecture
Client / curl / future dashboard
        ↓
POST /actions
        ↓
Fastify API
        ↓
Kafka topic: player.action.created
        ↓
Consequence Worker
        ↓
PostgreSQL / TimescaleDB
        ↓
Faction reputation table

Future architecture:

Player Action
   ↓
TypeScript API
   ↓
Kafka Event Stream
   ↓
TypeScript Consequence Worker
   ↓
C++ Consequence Engine
   ↓
PostgreSQL / TimescaleDB / Redis
   ↓
WebSocket Updates
   ↓
React + TypeScript Dashboard
Event Schema

Current Kafka topic:

player.action.created

Example event:

{
  "eventId": "generated-uuid",
  "eventType": "player.action.created",
  "occurredAt": "2026-07-07T01:46:44.364Z",
  "playerId": "player_001",
  "actionType": "HELP_FACTION",
  "targetFaction": "Survivors",
  "npcId": "elias",
  "metadata": {
    "resource": "medicine",
    "amount": 2
  }
}

Supported action types:

HELP_FACTION
ROB_NPC
SPARE_ENEMY
ATTACK_FACTION
DONATE_RESOURCE
Database Tables

Current tables:

players
factions
player_faction_reputation
choice_events

The choice_events table is configured as a TimescaleDB hypertable using occurred_at.

Seeded factions:

survivors
raiders
traders
order

Seeded player:

player_001
Running Locally
Prerequisites

Install:

Docker Desktop
WSL 2
Node.js inside WSL, preferably through nvm
npm

Verify WSL is using Linux Node, not Windows Node:

which node
which npm

Expected output should look like:

/home/<user>/.nvm/versions/node/v20.x.x/bin/node
/home/<user>/.nvm/versions/node/v20.x.x/bin/npm

It should not point to:

/mnt/c/Program Files/nodejs/
Install Dependencies

From the project root:

npm install

Build all workspaces:

npm run build

Or build individually:

npm run build -w packages/shared
npm run build -w apps/api
npm run build -w apps/worker
Start the Full Stack

From the project root:

docker compose up --build

This starts:

Kafka
Zookeeper
PostgreSQL / TimescaleDB
Redis
API service
Worker service

If the database schema changes and the local volume needs to be reset:

docker compose down -v
docker compose up --build

Use down -v carefully because it deletes the local database volume.

Health Check

In a second terminal:

curl http://localhost:3000/health

Expected response:

{
  "status": "ok",
  "service": "remnant-api",
  "kafkaBroker": "remnant-kafka:29092"
}
Send a Player Action
curl -X POST http://localhost:3000/actions \
  -H "Content-Type: application/json" \
  -d '{
    "playerId": "player_001",
    "actionType": "HELP_FACTION",
    "targetFaction": "Survivors",
    "npcId": "elias",
    "metadata": {
      "resource": "medicine",
      "amount": 2
    }
  }'

Expected response:

{
  "status": "accepted",
  "eventType": "player.action.created",
  "eventId": "generated-event-id"
}

Expected worker logs:

[worker] Received player.action.created event
[worker] Reputation updated: player=player_001, faction=survivors, delta=15
[worker] Event persisted and consequences applied
Verify Database State

Open psql inside the Postgres container:

docker exec -it remnant-postgres psql -U remnant -d remnant

View recent choice events:

SELECT event_id, player_id, action_type, target_faction, occurred_at
FROM choice_events
ORDER BY occurred_at DESC
LIMIT 5;

View faction reputation:

SELECT *
FROM player_faction_reputation
ORDER BY player_id, faction_id;

Expected result after one HELP_FACTION action targeting Survivors:

player_001 | survivors | 15

Exit psql:

\q
Current Milestone Status

Completed:

Monorepo setup
Shared TypeScript event schema
Fastify API
Kafka producer
Kafka worker consumer
PostgreSQL / TimescaleDB schema
Demo player/faction seed data
Choice event persistence
Direct faction reputation update

Next milestone:

Faction relationship propagation

Example target behavior:

HELP_FACTION Survivors
    → Survivors +15
    → Raiders -10
    → Traders +3
    → Order 0

Future milestones:

NPC memory system
Redis-backed active world state
WebSocket world updates
React + TypeScript dashboard
C++ consequence simulation engine
Dockerized full demo
GitHub Actions CI
Demo video/GIF
Planned C++ Integration

The C++ engine will be added as a standalone consequence simulation executable.

Planned flow:

TypeScript worker
    ↓
C++ consequence engine
    ↓
JSON consequence output
    ↓
PostgreSQL / Redis updates

The first C++ version will likely run as a CLI executable:

./remnant_engine input.json

It will return structured consequence output:

{
  "reputationDeltas": {
    "survivors": 15,
    "raiders": -10,
    "traders": 3,
    "order": 0
  },
  "worldFlags": [
    "survivors_clinic_supplied"
  ],
  "npcMemories": [
    {
      "npcId": "elias",
      "memoryType": "PLAYER_DONATED_MEDICINE",
      "intensity": 7
    }
  ]
}