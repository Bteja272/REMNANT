import { spawn } from "node:child_process";
import http from "node:http";
import Redis from "ioredis";
import { Kafka } from "kafkajs";
import { Pool } from "pg";
import client from "prom-client";
import { runScheduledWorldEventCheck } from "./scheduledWorldEvents";
import {
  PLAYER_ACTION_CREATED_TOPIC,
  PlayerActionCreatedEvent,
  PlayerActionCreatedEventSchema
} from "@remnant/shared";

const WORKER_METRICS_PORT = Number(process.env.WORKER_METRICS_PORT ?? 9101);
const KAFKA_BROKER = process.env.KAFKA_BROKER ?? "localhost:9092";
const CONSUMER_GROUP_ID =
  process.env.CONSUMER_GROUP_ID ?? "remnant-consequence-worker";

const DATABASE_URL =
  process.env.DATABASE_URL ?? "postgres://remnant:remnant@localhost:5432/remnant";

const REDIS_URL = process.env.REDIS_URL ?? "redis://localhost:6379";

const CONSEQUENCE_ENGINE_PATH =
  process.env.CONSEQUENCE_ENGINE_PATH ??
  "engines/consequence-cpp/build/remnant_consequence_engine";

const SCHEDULED_WORLD_EVENTS_ENABLED =
  process.env.SCHEDULED_WORLD_EVENTS_ENABLED !== "false";

const SCHEDULED_WORLD_EVENTS_INTERVAL_MS = Number(
  process.env.SCHEDULED_WORLD_EVENTS_INTERVAL_MS ?? 60000
);

const db = new Pool({
  connectionString: DATABASE_URL
});

const redis = new Redis(REDIS_URL);

/**
 * Prometheus metrics registry for the worker.
 * Exposed at GET /metrics on WORKER_METRICS_PORT.
 */
const workerMetricsRegister = new client.Registry();

client.collectDefaultMetrics({
  register: workerMetricsRegister,
  prefix: "remnant_worker_"
});

const workerEventsProcessedTotal = new client.Counter({
  name: "remnant_worker_events_processed_total",
  help: "Total Kafka player action events processed by the worker",
  labelNames: ["action_type", "engine", "status"],
  registers: [workerMetricsRegister]
});

const workerConsequenceEngineTotal = new client.Counter({
  name: "remnant_worker_consequence_engine_total",
  help: "Total consequence engine executions by engine type",
  labelNames: ["engine", "status"],
  registers: [workerMetricsRegister]
});

const workerProcessingDurationSeconds = new client.Histogram({
  name: "remnant_worker_processing_duration_seconds",
  help: "Time spent processing player action events",
  labelNames: ["action_type", "engine"],
  buckets: [0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5],
  registers: [workerMetricsRegister]
});

type ReputationUpdate = {
  factionId: string;
  delta: number;
  reason: "DIRECT_ACTION" | "FACTION_RELATIONSHIP";
};

type CppNpcMemory = {
  memoryType: string;
  description: string;
  intensity: number;
};

type CppWorldFlag = {
  flagKey: string;
  flagValue: boolean;
  description: string;
};

type ConsequencePlan = {
  engine: "cpp" | "typescript-fallback";
  directReputationDelta: number;
  npcMemory: CppNpcMemory | null;
  worldFlags: CppWorldFlag[];
};

type ProcessResult = {
  engine: "cpp" | "typescript-fallback";
  reputationUpdated: boolean;
  npcMemoryUpdated: boolean;
  worldStateUpdated: boolean;
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

function getFallbackBaseReputationDelta(
  event: PlayerActionCreatedEvent
): number {
  switch (event.actionType) {
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

function buildFallbackNpcMemory(
  event: PlayerActionCreatedEvent
): CppNpcMemory | null {
  if (!event.npcId) {
    return null;
  }

  switch (event.actionType) {
    case "HELP_FACTION":
      return {
        memoryType: "HELPED_ALLIED_FACTION",
        description: `Player helped ${
          event.targetFaction ?? "an allied faction"
        } near this NPC.`,
        intensity: 6
      };

    case "DONATE_RESOURCE":
      return {
        memoryType: "DONATED_RESOURCE",
        description: `Player donated ${
          event.metadata?.amount ?? "some"
        } ${event.metadata?.resource ?? "resources"}.`,
        intensity: 7
      };

    case "ROB_NPC":
      return {
        memoryType: "ROBBED_BY_PLAYER",
        description: "Player robbed this NPC during an encounter.",
        intensity: 9
      };

    case "SPARE_ENEMY":
      return {
        memoryType: "SPARED_BY_PLAYER",
        description:
          "Player spared this NPC instead of killing or capturing them.",
        intensity: 8
      };

    case "ATTACK_FACTION":
      return {
        memoryType: "ATTACKED_FACTION",
        description: `Player attacked ${
          event.targetFaction ?? "this NPC's faction"
        }.`,
        intensity: 8
      };

    default:
      return null;
  }
}

function buildFallbackWorldFlags(
  event: PlayerActionCreatedEvent
): CppWorldFlag[] {
  const targetFactionId = normalizeFactionId(event.targetFaction);
  const npcId = event.npcId?.toLowerCase();
  const resource = event.metadata?.resource?.toLowerCase();

  const flags: CppWorldFlag[] = [];

  if (
    event.actionType === "DONATE_RESOURCE" &&
    targetFactionId === "survivors" &&
    resource === "medicine"
  ) {
    flags.push({
      flagKey: "survivors_clinic_supplied",
      flagValue: true,
      description:
        "Dusthaven Clinic has enough medicine because the player donated supplies to the Survivors."
    });
  }

  if (event.actionType === "ROB_NPC" && npcId === "mara") {
    flags.push({
      flagKey: "trader_market_unstable",
      flagValue: true,
      description: "Mara's trade post is unstable after the player robbed her."
    });
  }

  if (event.actionType === "SPARE_ENEMY" && npcId === "knox") {
    flags.push({
      flagKey: "raider_checkpoint_ambush_disabled",
      flagValue: true,
      description:
        "Knox remembers being spared, reducing the chance of a Raider checkpoint ambush."
    });
  }

  if (event.actionType === "ATTACK_FACTION" && targetFactionId === "raiders") {
    flags.push({
      flagKey: "raider_checkpoint_hostile",
      flagValue: true,
      description:
        "The Raider checkpoint has become hostile after the player attacked Raiders."
    });
  }

  return flags;
}

function buildFallbackConsequencePlan(
  event: PlayerActionCreatedEvent
): ConsequencePlan {
  return {
    engine: "typescript-fallback",
    directReputationDelta: getFallbackBaseReputationDelta(event),
    npcMemory: buildFallbackNpcMemory(event),
    worldFlags: buildFallbackWorldFlags(event)
  };
}

function parseCppConsequencePlan(output: string): ConsequencePlan {
  const parsed = JSON.parse(output) as {
    engine?: string;
    directReputationDelta?: number;
    npcMemory?: CppNpcMemory | null;
    worldFlags?: CppWorldFlag[];
  };

  if (typeof parsed.directReputationDelta !== "number") {
    throw new Error("C++ engine response missing directReputationDelta");
  }

  return {
    engine: "cpp",
    directReputationDelta: parsed.directReputationDelta,
    npcMemory: parsed.npcMemory ?? null,
    worldFlags: parsed.worldFlags ?? []
  };
}

async function runCppConsequenceEngine(
  event: PlayerActionCreatedEvent
): Promise<string> {
  return new Promise((resolve, reject) => {
    const child = spawn(CONSEQUENCE_ENGINE_PATH, [], {
      stdio: ["pipe", "pipe", "pipe"]
    });

    let stdout = "";
    let stderr = "";

    const timeout = setTimeout(() => {
      child.kill("SIGKILL");
      reject(new Error("C++ consequence engine timed out"));
    }, 3000);

    child.stdout.on("data", (chunk) => {
      stdout += chunk.toString();
    });

    child.stderr.on("data", (chunk) => {
      stderr += chunk.toString();
    });

    child.on("error", (error) => {
      clearTimeout(timeout);
      reject(error);
    });

    child.on("close", (code) => {
      clearTimeout(timeout);

      if (code !== 0) {
        reject(
          new Error(
            `C++ consequence engine exited with code ${code}. stderr=${stderr}`
          )
        );
        return;
      }

      resolve(stdout);
    });

    child.stdin.write(JSON.stringify(event));
    child.stdin.end();
  });
}

async function buildConsequencePlan(
  event: PlayerActionCreatedEvent
): Promise<ConsequencePlan> {
  try {
    const stdout = await runCppConsequenceEngine(event);
    const plan = parseCppConsequencePlan(stdout);

    workerConsequenceEngineTotal.inc({
      engine: "cpp",
      status: "success"
    });

    console.log(
      `[worker] C++ consequence engine succeeded: event=${event.eventId}, delta=${plan.directReputationDelta}`
    );

    return plan;
  } catch (error) {
    workerConsequenceEngineTotal.inc({
      engine: "typescript-fallback",
      status: "used"
    });

    console.error(
      "[worker] C++ consequence engine failed. Falling back to TypeScript rules.",
      {
        eventId: event.eventId,
        error
      }
    );

    return buildFallbackConsequencePlan(event);
  }
}

async function saveChoiceEvent(event: PlayerActionCreatedEvent): Promise<void> {
  await db.query(
    `
    INSERT INTO choice_events (
      event_id,
      occurred_at,
      player_id,
      action_type,
      target_faction,
      npc_id,
      metadata
    )
    VALUES ($1, $2, $3, $4, $5, $6, $7)
    ON CONFLICT (event_id, occurred_at) DO NOTHING
    `,
    [
      event.eventId,
      event.occurredAt,
      event.playerId,
      event.actionType,
      event.targetFaction ?? null,
      event.npcId ?? null,
      event.metadata ?? {}
    ]
  );
}

async function getRelationshipBasedUpdates(
  sourceFactionId: string,
  baseDelta: number
): Promise<ReputationUpdate[]> {
  const result = await db.query<{
    affected_faction_id: string;
    relationship_type: string;
    influence_multiplier: string;
  }>(
    `
    SELECT
      affected_faction_id,
      relationship_type,
      influence_multiplier
    FROM faction_relationships
    WHERE source_faction_id = $1
    `,
    [sourceFactionId]
  );

  return result.rows
    .map((row) => {
      const multiplier = Number(row.influence_multiplier);
      const delta = Math.round(baseDelta * multiplier);

      return {
        factionId: row.affected_faction_id,
        delta,
        reason: "FACTION_RELATIONSHIP" as const
      };
    })
    .filter((update) => update.delta !== 0);
}

async function applySingleReputationUpdate(
  playerId: string,
  update: ReputationUpdate
): Promise<void> {
  await db.query(
    `
    INSERT INTO player_faction_reputation (
      player_id,
      faction_id,
      reputation_score,
      updated_at
    )
    VALUES ($1, $2, $3, NOW())
    ON CONFLICT (player_id, faction_id)
    DO UPDATE SET
      reputation_score = player_faction_reputation.reputation_score + EXCLUDED.reputation_score,
      updated_at = NOW()
    `,
    [playerId, update.factionId, update.delta]
  );

  console.log(
    `[worker] Reputation updated: player=${playerId}, faction=${update.factionId}, delta=${update.delta}, reason=${update.reason}`
  );
}

async function applyReputationUpdates(
  event: PlayerActionCreatedEvent,
  directReputationDelta: number
): Promise<boolean> {
  const targetFactionId = normalizeFactionId(event.targetFaction);

  if (!targetFactionId) {
    console.log("[worker] No target faction present. Skipping reputation update.");
    return false;
  }

  const directUpdate: ReputationUpdate = {
    factionId: targetFactionId,
    delta: directReputationDelta,
    reason: "DIRECT_ACTION"
  };

  const relationshipUpdates = await getRelationshipBasedUpdates(
    targetFactionId,
    directReputationDelta
  );

  const allUpdates = [directUpdate, ...relationshipUpdates];

  for (const update of allUpdates) {
    await applySingleReputationUpdate(event.playerId, update);
  }

  return allUpdates.length > 0;
}

async function saveNpcMemory(
  event: PlayerActionCreatedEvent,
  memory: CppNpcMemory | null
): Promise<boolean> {
  if (!memory || !event.npcId) {
    console.log("[worker] No NPC memory generated for this event.");
    return false;
  }

  const npcId = event.npcId.toLowerCase();

  await db.query(
    `
    INSERT INTO npc_memory (
      npc_id,
      player_id,
      memory_type,
      description,
      intensity,
      related_event_id
    )
    VALUES ($1, $2, $3, $4, $5, $6)
    `,
    [
      npcId,
      event.playerId,
      memory.memoryType,
      memory.description,
      memory.intensity,
      event.eventId
    ]
  );

  console.log(
    `[worker] NPC memory saved: npc=${npcId}, player=${event.playerId}, memory=${memory.memoryType}, intensity=${memory.intensity}`
  );

  return true;
}

async function saveWorldStateFlags(
  event: PlayerActionCreatedEvent,
  flags: CppWorldFlag[]
): Promise<boolean> {
  if (flags.length === 0) {
    console.log("[worker] No world state flags generated for this event.");
    return false;
  }

  for (const flag of flags) {
    await db.query(
      `
      INSERT INTO world_state_flags (
        player_id,
        flag_key,
        flag_value,
        description,
        source_event_id,
        updated_at
      )
      VALUES ($1, $2, $3, $4, $5, NOW())
      ON CONFLICT (player_id, flag_key)
      DO UPDATE SET
        flag_value = EXCLUDED.flag_value,
        description = EXCLUDED.description,
        source_event_id = EXCLUDED.source_event_id,
        updated_at = NOW()
      `,
      [
        event.playerId,
        flag.flagKey,
        flag.flagValue,
        flag.description,
        event.eventId
      ]
    );

    console.log(
      `[worker] World state flag updated: player=${event.playerId}, flag=${flag.flagKey}, value=${flag.flagValue}`
    );
  }

  return true;
}

async function refreshRedisReputation(playerId: string): Promise<void> {
  const result = await db.query(
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

  await redis.set(
    `player:${playerId}:reputation`,
    JSON.stringify(result.rows),
    "EX",
    3600
  );

  console.log(`[worker] Redis reputation cache refreshed for player=${playerId}`);
}

async function refreshRedisWorldState(playerId: string): Promise<void> {
  const result = await db.query(
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

  await redis.set(
    `player:${playerId}:world_state`,
    JSON.stringify(result.rows),
    "EX",
    3600
  );

  console.log(`[worker] Redis world-state cache refreshed for player=${playerId}`);
}

async function addRedisLatestEvent(event: PlayerActionCreatedEvent): Promise<void> {
  const key = `player:${event.playerId}:latest_events`;

  await redis.lpush(
    key,
    JSON.stringify({
      eventId: event.eventId,
      eventType: event.eventType,
      occurredAt: event.occurredAt,
      actionType: event.actionType,
      targetFaction: event.targetFaction ?? null,
      npcId: event.npcId ?? null,
      metadata: event.metadata ?? {}
    })
  );

  await redis.ltrim(key, 0, 24);
  await redis.expire(key, 3600);

  console.log(`[worker] Redis latest event list updated for player=${event.playerId}`);
}

async function publishRedisUpdate(
  event: PlayerActionCreatedEvent,
  result: ProcessResult
): Promise<void> {
  const channel = `channel:${event.playerId}:updates`;

  const message = {
    type: "PLAYER_STATE_UPDATED",
    playerId: event.playerId,
    eventId: event.eventId,
    actionType: event.actionType,
    targetFaction: event.targetFaction ?? null,
    npcId: event.npcId ?? null,
    engine: result.engine,
    reputationUpdated: result.reputationUpdated,
    npcMemoryUpdated: result.npcMemoryUpdated,
    worldStateUpdated: result.worldStateUpdated,
    occurredAt: event.occurredAt
  };

  await redis.publish(channel, JSON.stringify(message));

  console.log(`[worker] Redis update published to ${channel}`);
}

async function refreshRedisActiveState(
  event: PlayerActionCreatedEvent,
  result: ProcessResult
): Promise<void> {
  await refreshRedisReputation(event.playerId);
  await refreshRedisWorldState(event.playerId);
  await addRedisLatestEvent(event);
  await publishRedisUpdate(event, result);
}

async function processPlayerAction(
  event: PlayerActionCreatedEvent
): Promise<ProcessResult> {
  const startTime = process.hrtime.bigint();

  let result: ProcessResult | null = null;

  try {
    await saveChoiceEvent(event);

    const consequencePlan = await buildConsequencePlan(event);

    const reputationUpdated = await applyReputationUpdates(
      event,
      consequencePlan.directReputationDelta
    );

    const npcMemoryUpdated = await saveNpcMemory(
      event,
      consequencePlan.npcMemory
    );

    const worldStateUpdated = await saveWorldStateFlags(
      event,
      consequencePlan.worldFlags
    );

    result = {
      engine: consequencePlan.engine,
      reputationUpdated,
      npcMemoryUpdated,
      worldStateUpdated
    };

    await refreshRedisActiveState(event, result);

    workerEventsProcessedTotal.inc({
      action_type: event.actionType,
      engine: result.engine,
      status: "success"
    });

    return result;
  } catch (error) {
    workerEventsProcessedTotal.inc({
      action_type: event.actionType,
      engine: result?.engine ?? "unknown",
      status: "failure"
    });

    throw error;
  } finally {
    const endTime = process.hrtime.bigint();
    const durationSeconds = Number(endTime - startTime) / 1_000_000_000;

    workerProcessingDurationSeconds.observe(
      {
        action_type: event.actionType,
        engine: result?.engine ?? "unknown"
      },
      durationSeconds
    );
  }
}

function startWorkerMetricsServer() {
  const server = http.createServer(async (request, response) => {
    if (request.url !== "/metrics") {
      response.statusCode = 404;
      response.end("Not found");
      return;
    }

    response.setHeader("Content-Type", workerMetricsRegister.contentType);
    response.end(await workerMetricsRegister.metrics());
  });

  server.listen(WORKER_METRICS_PORT, "0.0.0.0", () => {
    console.log(
      `[worker] Metrics server listening on http://0.0.0.0:${WORKER_METRICS_PORT}/metrics`
    );
  });

  return server;
}

function startScheduledWorldEventLoop() {
  if (!SCHEDULED_WORLD_EVENTS_ENABLED) {
    console.log("[worker] Scheduled world events disabled");
    return;
  }

  console.log(
    `[worker] Scheduled world event loop enabled. intervalMs=${SCHEDULED_WORLD_EVENTS_INTERVAL_MS}`
  );

  const runCheck = async () => {
    try {
      const results = await runScheduledWorldEventCheck(db, redis);
      const triggered = results.filter((result) => result.triggered);

      if (triggered.length > 0) {
        console.log(
          `[worker] Scheduled world events triggered: ${triggered.length}`
        );

        for (const result of triggered) {
          console.log("[worker] World event triggered", result.event);
        }
      }
    } catch (error) {
      console.error("[worker] Scheduled world event check failed", error);
    }
  };

  setTimeout(runCheck, 5000);
  setInterval(runCheck, SCHEDULED_WORLD_EVENTS_INTERVAL_MS);
}

async function startWorker() {
  await db.query("SELECT 1");
  console.log("[worker] Connected to PostgreSQL");

  await redis.ping();
  console.log("[worker] Connected to Redis");

  startWorkerMetricsServer();
  startScheduledWorldEventLoop();

  const kafka = new Kafka({
    clientId: "remnant-worker",
    brokers: [KAFKA_BROKER]
  });

  const consumer = kafka.consumer({
    groupId: CONSUMER_GROUP_ID
  });

  await consumer.connect();

  console.log(`[worker] Connected to Kafka broker: ${KAFKA_BROKER}`);
  console.log(`[worker] Consequence engine path: ${CONSEQUENCE_ENGINE_PATH}`);

  await consumer.subscribe({
    topic: PLAYER_ACTION_CREATED_TOPIC,
    fromBeginning: true
  });

  console.log(`[worker] Subscribed to topic: ${PLAYER_ACTION_CREATED_TOPIC}`);

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const rawValue = message.value?.toString();

      if (!rawValue) {
        console.error("[worker] Received empty Kafka message");
        return;
      }

      let parsedJson: unknown;

      try {
        parsedJson = JSON.parse(rawValue);
      } catch (error) {
        console.error("[worker] Failed to parse Kafka message as JSON", {
          topic,
          partition,
          value: rawValue,
          error
        });
        return;
      }

      const parsedEvent = PlayerActionCreatedEventSchema.safeParse(parsedJson);

      if (!parsedEvent.success) {
        console.error("[worker] Invalid player.action.created event", {
          topic,
          partition,
          errors: parsedEvent.error.flatten()
        });
        return;
      }

      const event = parsedEvent.data;

      console.log("\n[worker] Received player.action.created event");
      console.log("------------------------------------------");
      console.log(`Event ID:       ${event.eventId}`);
      console.log(`Occurred At:    ${event.occurredAt}`);
      console.log(`Player ID:      ${event.playerId}`);
      console.log(`Action Type:    ${event.actionType}`);
      console.log(`Target Faction: ${event.targetFaction ?? "N/A"}`);
      console.log(`NPC ID:         ${event.npcId ?? "N/A"}`);
      console.log(`Metadata:       ${JSON.stringify(event.metadata ?? {}, null, 2)}`);
      console.log("------------------------------------------");

      try {
        const result = await processPlayerAction(event);
        console.log("[worker] Event fully processed", result, "\n");
      } catch (error) {
        console.error("[worker] Failed to process event", {
          eventId: event.eventId,
          error
        });
      }
    }
  });
}

process.on("SIGINT", async () => {
  console.log("[worker] Shutting down");
  await redis.quit();
  await db.end();
  process.exit(0);
});

startWorker().catch((error) => {
  console.error("[worker] Failed to start worker", error);
  process.exit(1);
});