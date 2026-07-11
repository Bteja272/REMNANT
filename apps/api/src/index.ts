import Fastify from "fastify";
import cors from "@fastify/cors";
import Redis from "ioredis";
import client from "prom-client";
import { WebSocket, WebSocketServer } from "ws";
import { Kafka, Producer } from "kafkajs";
import { Pool } from "pg";
import {
  CreatePlayerActionRequestSchema,
  PLAYER_ACTION_CREATED_TOPIC,
  PlayerActionCreatedEvent
} from "@remnant/shared";

const PORT = Number(process.env.PORT ?? 3000);
const HOST = process.env.HOST ?? "0.0.0.0";
const KAFKA_BROKER = process.env.KAFKA_BROKER ?? "localhost:9092";

const DATABASE_URL =
  process.env.DATABASE_URL ?? "postgres://remnant:remnant@localhost:5432/remnant";

const REDIS_URL = process.env.REDIS_URL ?? "redis://localhost:6379";

const app = Fastify({
  logger: true
});

const db = new Pool({
  connectionString: DATABASE_URL
});

const redis = new Redis(REDIS_URL);

let wss: WebSocketServer;
let producer: Producer;

/**
 * Prometheus metrics registry for the API.
 * These metrics are exposed at GET /metrics and scraped by Prometheus.
 */
const metricsRegister = new client.Registry();

client.collectDefaultMetrics({
  register: metricsRegister,
  prefix: "remnant_api_"
});

const apiHttpRequestsTotal = new client.Counter({
  name: "remnant_api_http_requests_total",
  help: "Total HTTP requests received by the REMNANT API",
  labelNames: ["method", "route", "status_code"],
  registers: [metricsRegister]
});

const apiActionsSubmittedTotal = new client.Counter({
  name: "remnant_api_actions_submitted_total",
  help: "Total committed player actions submitted to the API",
  labelNames: ["action_type", "result"],
  registers: [metricsRegister]
});

const apiActionPreviewsTotal = new client.Counter({
  name: "remnant_api_action_previews_total",
  help: "Total consequence preview requests submitted to the API",
  labelNames: ["action_type", "result"],
  registers: [metricsRegister]
});

async function buildKafkaProducer(): Promise<Producer> {
  const kafka = new Kafka({
    clientId: "remnant-api",
    brokers: [KAFKA_BROKER]
  });

  const kafkaProducer = kafka.producer();
  await kafkaProducer.connect();

  app.log.info(`Kafka producer connected to ${KAFKA_BROKER}`);
  return kafkaProducer;
}

app.register(cors, {
  origin: true
});

/**
 * Tracks all API HTTP responses by method, route, and status code.
 */
app.addHook("onResponse", async (request, reply) => {
  apiHttpRequestsTotal.inc({
    method: request.method,
    route: request.routeOptions.url ?? request.url,
    status_code: String(reply.statusCode)
  });
});

/**
 * Prometheus scrape endpoint.
 */
app.get("/metrics", async (_request, reply) => {
  reply.header("Content-Type", metricsRegister.contentType);
  return metricsRegister.metrics();
});

app.get("/health", async () => {
  await db.query("SELECT 1");
  await redis.ping();

  return {
    status: "ok",
    service: "remnant-api",
    kafkaBroker: KAFKA_BROKER,
    database: "connected",
    redis: "connected",
    websocket: "enabled",
    metrics: "enabled"
  };
});

type PreviewReputationUpdate = {
  factionId: string;
  factionName?: string;
  delta: number;
  reason: "DIRECT_ACTION" | "FACTION_RELATIONSHIP";
};

type PreviewNpcMemory = {
  npcId: string;
  memoryType: string;
  description: string;
  intensity: number;
} | null;

type PreviewWorldFlag = {
  flagKey: string;
  flagValue: boolean;
  description: string;
};

function normalizeFactionId(factionName?: string): string | null {
  if (!factionName) return null;

  const normalized = factionName.trim().toLowerCase();

  const knownFactions: Record<string, string> = {
    survivors: "survivors",
    raiders: "raiders",
    traders: "traders",
    "the order": "order",
    order: "order"
  };

  return knownFactions[normalized] ?? normalized.replace(/\s+/g, "_");
}

function getBaseReputationDelta(actionType: string): number {
  switch (actionType) {
    case "HELP_FACTION":
      return 15;
    case "DONATE_RESOURCE":
      return 10;
    case "ROB_NPC":
      return -20;
    case "ATTACK_FACTION":
      return -25;
    case "SPARE_ENEMY":
      return 8;
    default:
      return 0;
  }
}

function buildNpcMemoryPreview(action: {
  actionType: string;
  targetFaction?: string;
  npcId?: string;
  metadata?: {
    resource?: string;
    amount?: number;
    location?: string;
    notes?: string;
  };
}): PreviewNpcMemory {
  if (!action.npcId) return null;

  const npcId = action.npcId.toLowerCase();

  switch (action.actionType) {
    case "HELP_FACTION":
      return {
        npcId,
        memoryType: "HELPED_ALLIED_FACTION",
        description: `NPC may remember that the player helped ${
          action.targetFaction ?? "an allied faction"
        }.`,
        intensity: 6
      };

    case "DONATE_RESOURCE":
      return {
        npcId,
        memoryType: "DONATED_RESOURCE",
        description: `NPC may remember that the player donated ${
          action.metadata?.amount ?? "some"
        } ${action.metadata?.resource ?? "resources"}.`,
        intensity: 7
      };

    case "ROB_NPC":
      return {
        npcId,
        memoryType: "ROBBED_BY_PLAYER",
        description: "NPC may remember being robbed by the player.",
        intensity: 9
      };

    case "SPARE_ENEMY":
      return {
        npcId,
        memoryType: "SPARED_BY_PLAYER",
        description: "NPC may remember that the player spared them.",
        intensity: 8
      };

    case "ATTACK_FACTION":
      return {
        npcId,
        memoryType: "ATTACKED_FACTION",
        description: `NPC may remember that the player attacked ${
          action.targetFaction ?? "their faction"
        }.`,
        intensity: 8
      };

    default:
      return null;
  }
}

function buildWorldFlagPreviews(action: {
  actionType: string;
  targetFaction?: string;
  npcId?: string;
  metadata?: {
    resource?: string;
    amount?: number;
    location?: string;
    notes?: string;
  };
}): PreviewWorldFlag[] {
  const targetFactionId = normalizeFactionId(action.targetFaction);
  const npcId = action.npcId?.toLowerCase();
  const resource = action.metadata?.resource?.toLowerCase();

  const flags: PreviewWorldFlag[] = [];

  if (
    action.actionType === "DONATE_RESOURCE" &&
    targetFactionId === "survivors" &&
    resource === "medicine"
  ) {
    flags.push({
      flagKey: "survivors_clinic_supplied",
      flagValue: true,
      description:
        "Dusthaven Clinic may become supplied if medicine is donated to the Survivors."
    });
  }

  if (action.actionType === "ROB_NPC" && npcId === "mara") {
    flags.push({
      flagKey: "trader_market_unstable",
      flagValue: true,
      description:
        "Mara's trade post may become unstable if the player robs her."
    });
  }

  if (action.actionType === "SPARE_ENEMY" && npcId === "knox") {
    flags.push({
      flagKey: "raider_checkpoint_ambush_disabled",
      flagValue: true,
      description:
        "The Raider checkpoint ambush may be disabled if Knox is spared."
    });
  }

  if (action.actionType === "ATTACK_FACTION" && targetFactionId === "raiders") {
    flags.push({
      flagKey: "raider_checkpoint_hostile",
      flagValue: true,
      description:
        "The Raider checkpoint may become hostile if the player attacks Raiders."
    });
  }

  return flags;
}

async function buildReputationPreview(action: {
  actionType: string;
  targetFaction?: string;
}): Promise<PreviewReputationUpdate[]> {
  const targetFactionId = normalizeFactionId(action.targetFaction);

  if (!targetFactionId) {
    return [];
  }

  const baseDelta = getBaseReputationDelta(action.actionType);

  const factionResult = await db.query(
    `
    SELECT id, name
    FROM factions
    WHERE id = $1
    `,
    [targetFactionId]
  );

  const targetFactionName = factionResult.rows[0]?.name;

  const directUpdate: PreviewReputationUpdate = {
    factionId: targetFactionId,
    factionName: targetFactionName,
    delta: baseDelta,
    reason: "DIRECT_ACTION"
  };

  const relationshipResult = await db.query<{
    affected_faction_id: string;
    faction_name: string;
    influence_multiplier: string;
  }>(
    `
    SELECT
      fr.affected_faction_id,
      f.name AS faction_name,
      fr.influence_multiplier
    FROM faction_relationships fr
    JOIN factions f
      ON f.id = fr.affected_faction_id
    WHERE fr.source_faction_id = $1
    `,
    [targetFactionId]
  );

  const relationshipUpdates: PreviewReputationUpdate[] = relationshipResult.rows
    .map((row) => {
      const multiplier = Number(row.influence_multiplier);
      const delta = Math.round(baseDelta * multiplier);

      return {
        factionId: row.affected_faction_id,
        factionName: row.faction_name,
        delta,
        reason: "FACTION_RELATIONSHIP" as const
      };
    })
    .filter((update) => update.delta !== 0);

  return [directUpdate, ...relationshipUpdates];
}

app.post("/actions/preview", async (request, reply) => {
  const parsed = CreatePlayerActionRequestSchema.safeParse(request.body);

  if (!parsed.success) {
    apiActionPreviewsTotal.inc({
      action_type: "invalid",
      result: "rejected"
    });

    return reply.status(400).send({
      error: "Invalid player action preview request",
      details: parsed.error.flatten()
    });
  }

  const action = parsed.data;

  const reputationPreview = await buildReputationPreview({
    actionType: action.actionType,
    targetFaction: action.targetFaction
  });

  const npcMemoryPreview = buildNpcMemoryPreview({
    actionType: action.actionType,
    targetFaction: action.targetFaction,
    npcId: action.npcId,
    metadata: action.metadata
  });

  const worldFlagsPreview = buildWorldFlagPreviews({
    actionType: action.actionType,
    targetFaction: action.targetFaction,
    npcId: action.npcId,
    metadata: action.metadata
  });

  apiActionPreviewsTotal.inc({
    action_type: action.actionType,
    result: "success"
  });

  return reply.status(200).send({
    playerId: action.playerId,
    actionType: action.actionType,
    targetFaction: action.targetFaction ?? null,
    npcId: action.npcId ?? null,
    reputationPreview,
    npcMemoryPreview,
    worldFlagsPreview,
    committed: false
  });
});

app.post("/actions", async (request, reply) => {
  const parsed = CreatePlayerActionRequestSchema.safeParse(request.body);

  if (!parsed.success) {
    apiActionsSubmittedTotal.inc({
      action_type: "invalid",
      result: "rejected"
    });

    return reply.status(400).send({
      error: "Invalid player action request",
      details: parsed.error.flatten()
    });
  }

  const event: PlayerActionCreatedEvent = {
    eventId: crypto.randomUUID(),
    eventType: "player.action.created",
    occurredAt: new Date().toISOString(),
    ...parsed.data
  };

  try {
    await producer.send({
      topic: PLAYER_ACTION_CREATED_TOPIC,
      messages: [
        {
          key: event.playerId,
          value: JSON.stringify(event)
        }
      ]
    });

    apiActionsSubmittedTotal.inc({
      action_type: event.actionType,
      result: "accepted"
    });

    return reply.status(200).send({
      status: "accepted",
      eventType: event.eventType,
      eventId: event.eventId
    });
  } catch (error) {
    apiActionsSubmittedTotal.inc({
      action_type: event.actionType,
      result: "publish_failed"
    });

    app.log.error(error, "Failed to publish player action event");

    return reply.status(503).send({
      error: "Kafka publish failed",
      message:
        "The action was not accepted because the event could not be published."
    });
  }
});

app.get("/players/:playerId/reputation", async (request, reply) => {
  const { playerId } = request.params as { playerId: string };

  const result = await db.query(
    `
    SELECT
      pfr.player_id,
      pfr.faction_id,
      f.name AS faction_name,
      pfr.reputation_score,
      pfr.updated_at
    FROM player_faction_reputation pfr
    JOIN factions f
      ON f.id = pfr.faction_id
    WHERE pfr.player_id = $1
    ORDER BY pfr.faction_id
    `,
    [playerId]
  );

  return reply.status(200).send({
    playerId,
    reputation: result.rows
  });
});

app.get("/players/:playerId/timeline", async (request, reply) => {
  const { playerId } = request.params as { playerId: string };

  const result = await db.query(
    `
    SELECT
      event_id,
      occurred_at,
      player_id,
      action_type,
      target_faction,
      npc_id,
      metadata
    FROM choice_events
    WHERE player_id = $1
    ORDER BY occurred_at DESC
    LIMIT 25
    `,
    [playerId]
  );

  return reply.status(200).send({
    playerId,
    timeline: result.rows
  });
});

app.get("/players/:playerId/world-events", async (request, reply) => {
  const { playerId } = request.params as { playerId: string };

  const result = await db.query(
    `
    SELECT
      event_id,
      event_type,
      occurred_at,
      player_id,
      world_event_type,
      source,
      severity,
      description,
      affected_faction,
      affected_npc_id,
      flag_key,
      metadata,
      created_at
    FROM system_world_events
    WHERE player_id = $1
    ORDER BY occurred_at DESC
    LIMIT 25
    `,
    [playerId]
  );

  return reply.status(200).send({
    playerId,
    worldEvents: result.rows
  });
});

app.get("/players", async (_request, reply) => {
  const result = await db.query(
    `
    SELECT
      id,
      display_name,
      created_at
    FROM players
    ORDER BY created_at ASC
    `
  );

  return reply.status(200).send({
    players: result.rows
  });
});

app.post("/players", async (request, reply) => {
  const body = request.body as {
    id?: string;
    displayName?: string;
  };

  const playerId =
    body.id?.trim() || `player_${crypto.randomUUID().slice(0, 8)}`;

  const displayName = body.displayName?.trim() || playerId;

  const client = await db.connect();

  try {
    await client.query("BEGIN");

    const playerResult = await client.query(
      `
      INSERT INTO players (id, display_name)
      VALUES ($1, $2)
      ON CONFLICT (id)
      DO UPDATE SET display_name = EXCLUDED.display_name
      RETURNING id, display_name, created_at
      `,
      [playerId, displayName]
    );

    await client.query(
      `
      INSERT INTO player_faction_reputation (
        player_id,
        faction_id,
        reputation_score
      )
      SELECT
        $1,
        id,
        0
      FROM factions
      ON CONFLICT (player_id, faction_id)
      DO NOTHING
      `,
      [playerId]
    );

    await client.query("COMMIT");

    return reply.status(201).send({
      player: playerResult.rows[0]
    });
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
});

app.get("/world/shared-state", async (_request, reply) => {
  const playersResult = await db.query(
    `
    SELECT
      id,
      display_name,
      created_at
    FROM players
    ORDER BY created_at ASC
    `
  );

  const actionCountsResult = await db.query(
    `
    SELECT
      action_type,
      COUNT(*)::INT AS count
    FROM choice_events
    GROUP BY action_type
    ORDER BY count DESC, action_type ASC
    `
  );

  const factionActivityResult = await db.query(
    `
    SELECT
      LOWER(target_faction) AS faction,
      COUNT(*)::INT AS count
    FROM choice_events
    WHERE target_faction IS NOT NULL
    GROUP BY LOWER(target_faction)
    ORDER BY count DESC
    `
  );

  const latestEventsResult = await db.query(
    `
    SELECT
      event_id,
      occurred_at,
      player_id,
      action_type,
      target_faction,
      npc_id,
      metadata
    FROM choice_events
    ORDER BY occurred_at DESC
    LIMIT 15
    `
  );

  const worldFlagsResult = await db.query(
    `
    SELECT
      player_id,
      flag_key,
      flag_value,
      description,
      source_event_id,
      updated_at
    FROM world_state_flags
    ORDER BY updated_at DESC
    LIMIT 30
    `
  );

  const systemEventsResult = await db.query(
    `
    SELECT
      event_id,
      occurred_at,
      player_id,
      world_event_type,
      severity,
      description,
      affected_faction,
      affected_npc_id,
      flag_key
    FROM system_world_events
    ORDER BY occurred_at DESC
    LIMIT 15
    `
  );

  return reply.status(200).send({
    players: playersResult.rows,
    actionCounts: actionCountsResult.rows,
    factionActivity: factionActivityResult.rows,
    latestEvents: latestEventsResult.rows,
    worldFlags: worldFlagsResult.rows,
    systemEvents: systemEventsResult.rows
  });
});


app.get("/players/:playerId/state", async (request, reply) => {
  const { playerId } = request.params as { playerId: string };

  const playerResult = await db.query(
    `
    SELECT id, display_name, created_at
    FROM players
    WHERE id = $1
    `,
    [playerId]
  );

  if (playerResult.rowCount === 0) {
    return reply.status(404).send({
      error: "Player not found",
      playerId
    });
  }

  const reputationResult = await db.query(
    `
    SELECT
      pfr.faction_id,
      f.name AS faction_name,
      pfr.reputation_score,
      pfr.updated_at
    FROM player_faction_reputation pfr
    JOIN factions f
      ON f.id = pfr.faction_id
    WHERE pfr.player_id = $1
    ORDER BY pfr.faction_id
    `,
    [playerId]
  );

  const timelineResult = await db.query(
    `
    SELECT
      event_id,
      occurred_at,
      action_type,
      target_faction,
      npc_id,
      metadata
    FROM choice_events
    WHERE player_id = $1
    ORDER BY occurred_at DESC
    LIMIT 10
    `,
    [playerId]
  );

  const worldStateResult = await db.query(
    `
    SELECT
      flag_key,
      flag_value,
      description,
      source_event_id,
      updated_at
    FROM world_state_flags
    WHERE player_id = $1
    ORDER BY updated_at DESC
    `,
    [playerId]
  );

  const worldEventsResult = await db.query(
    `
    SELECT
      event_id,
      event_type,
      occurred_at,
      world_event_type,
      source,
      severity,
      description,
      affected_faction,
      affected_npc_id,
      flag_key,
      metadata
    FROM system_world_events
    WHERE player_id = $1
    ORDER BY occurred_at DESC
    LIMIT 10
    `,
    [playerId]
  );

  return reply.status(200).send({
    player: playerResult.rows[0],
    reputation: reputationResult.rows,
    recentTimeline: timelineResult.rows,
    worldState: worldStateResult.rows,
    worldEvents: worldEventsResult.rows
  });
});

app.get("/npcs/:npcId/behavior", async (request, reply) => {
  const { npcId } = request.params as { npcId: string };
  const { playerId } = request.query as { playerId?: string };

  if (!playerId) {
    return reply.status(400).send({
      error: "Missing required query parameter: playerId"
    });
  }

  const npcResult = await db.query(
    `
    SELECT
      n.id,
      n.name,
      n.faction_id,
      f.name AS faction_name,
      n.description
    FROM npcs n
    LEFT JOIN factions f
      ON f.id = n.faction_id
    WHERE n.id = $1
    `,
    [npcId.toLowerCase()]
  );

  if (npcResult.rowCount === 0) {
    return reply.status(404).send({
      error: "NPC not found",
      npcId
    });
  }

  const memoryResult = await db.query(
    `
    SELECT
      id,
      npc_id,
      player_id,
      memory_type,
      description,
      intensity,
      related_event_id,
      created_at
    FROM npc_memory
    WHERE npc_id = $1
      AND player_id = $2
    ORDER BY intensity DESC, created_at DESC
    LIMIT 10
    `,
    [npcId.toLowerCase(), playerId]
  );

  const npc = npcResult.rows[0];
  const memories = memoryResult.rows;

  let behavior = "NEUTRAL";
  let reason = "This NPC has no strong memory of the player yet.";

  const strongestMemory = memories[0];

  if (strongestMemory) {
    switch (strongestMemory.memory_type) {
      case "ROBBED_BY_PLAYER":
        behavior = "LOCK_DOORS";
        reason = "This NPC remembers being robbed by the player.";
        break;

      case "SPARED_BY_PLAYER":
        behavior = "SHOWS_MERCY";
        reason = "This NPC remembers that the player spared them.";
        break;

      case "DONATED_RESOURCE":
        behavior = "OFFERS_HELP";
        reason = "This NPC remembers that the player donated useful resources.";
        break;

      case "HELPED_ALLIED_FACTION":
        behavior = "FRIENDLY";
        reason = "This NPC remembers the player helping an allied faction.";
        break;

      case "ATTACKED_FACTION":
        behavior = "HOSTILE";
        reason = "This NPC remembers the player attacking their faction.";
        break;

      default:
        behavior = "WATCHFUL";
        reason = "This NPC remembers the player, but the memory is ambiguous.";
        break;
    }
  }

  return reply.status(200).send({
    npc,
    playerId,
    behavior,
    reason,
    memories
  });
});

app.get("/players/:playerId/world-state", async (request, reply) => {
  const { playerId } = request.params as { playerId: string };

  const result = await db.query(
    `
    SELECT
      player_id,
      flag_key,
      flag_value,
      description,
      source_event_id,
      updated_at
    FROM world_state_flags
    WHERE player_id = $1
    ORDER BY updated_at DESC
    `,
    [playerId]
  );

  return reply.status(200).send({
    playerId,
    worldState: result.rows
  });
});

app.get("/world/state", async (_request, reply) => {
  const result = await db.query(
    `
    SELECT
      player_id,
      flag_key,
      flag_value,
      description,
      source_event_id,
      updated_at
    FROM world_state_flags
    ORDER BY updated_at DESC
    LIMIT 100
    `
  );

  return reply.status(200).send({
    worldState: result.rows
  });
});

function setupWebSocketServer() {
  wss = new WebSocketServer({
    server: app.server
  });

  wss.on("connection", async (socket, request) => {
    const url = new URL(request.url ?? "", "http://localhost");
    const match = url.pathname.match(/^\/ws\/players\/([^/]+)$/);

    if (!match) {
      socket.close(1008, "Invalid WebSocket path");
      return;
    }

    const playerId = match[1];
    const channel = `channel:${playerId}:updates`;
    const subscriber = redis.duplicate();

    console.log(`[ws] Client connected for player=${playerId}`);

    socket.send(
      JSON.stringify({
        type: "CONNECTED",
        playerId,
        channel,
        message: "WebSocket connection established"
      })
    );

    try {
      const [cachedReputation, cachedWorldState, latestEvents] =
        await Promise.all([
          redis.get(`player:${playerId}:reputation`),
          redis.get(`player:${playerId}:world_state`),
          redis.lrange(`player:${playerId}:latest_events`, 0, 24)
        ]);

      if (socket.readyState === WebSocket.OPEN) {
        socket.send(
          JSON.stringify({
            type: "INITIAL_STATE",
            playerId,
            reputation: cachedReputation ? JSON.parse(cachedReputation) : [],
            worldState: cachedWorldState ? JSON.parse(cachedWorldState) : [],
            latestEvents: latestEvents.map((event) => JSON.parse(event))
          })
        );
      }

      await subscriber.subscribe(channel);

      subscriber.on("message", (receivedChannel, message) => {
        if (receivedChannel !== channel) {
          return;
        }

        if (socket.readyState === WebSocket.OPEN) {
          socket.send(message);
        }
      });
    } catch (error) {
      console.error("[ws] Failed to initialize WebSocket subscription", {
        playerId,
        error
      });

      if (socket.readyState === WebSocket.OPEN) {
        socket.send(
          JSON.stringify({
            type: "ERROR",
            message: "Failed to initialize live updates"
          })
        );
      }
    }

    socket.on("close", async () => {
      console.log(`[ws] Client disconnected for player=${playerId}`);

      try {
        await subscriber.unsubscribe(channel);
        await subscriber.quit();
      } catch (error) {
        console.error("[ws] Failed to clean up Redis subscriber", {
          playerId,
          error
        });
      }
    });

    socket.on("error", (error) => {
      console.error("[ws] WebSocket error", {
        playerId,
        error
      });
    });
  });

  console.log("[ws] WebSocket server attached to Fastify HTTP server");
}

async function start() {
  try {
    await db.query("SELECT 1");
    app.log.info("PostgreSQL connection verified");

    await redis.ping();
    app.log.info("Redis connection verified");

    producer = await buildKafkaProducer();

    setupWebSocketServer();

    await app.listen({
      port: PORT,
      host: HOST
    });

    app.log.info(`REMNANT API running on http://${HOST}:${PORT}`);
  } catch (error) {
    app.log.error(error, "Failed to start API");
    process.exit(1);
  }
}

process.on("SIGINT", async () => {
  app.log.info("Shutting down API");
  await producer?.disconnect();
  await redis.quit();
  wss?.close();
  await db.end();
  await app.close();
  process.exit(0);
});

start();