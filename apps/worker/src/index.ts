import Redis from "ioredis";
import { Kafka } from "kafkajs";
import { Pool } from "pg";
import {
  PLAYER_ACTION_CREATED_TOPIC,
  PlayerActionCreatedEvent,
  PlayerActionCreatedEventSchema
} from "@remnant/shared";

const KAFKA_BROKER = process.env.KAFKA_BROKER ?? "localhost:9092";
const CONSUMER_GROUP_ID =
  process.env.CONSUMER_GROUP_ID ?? "remnant-consequence-worker";

const DATABASE_URL =
  process.env.DATABASE_URL ?? "postgres://remnant:remnant@localhost:5432/remnant";

const REDIS_URL = process.env.REDIS_URL ?? "redis://localhost:6379";

const db = new Pool({
  connectionString: DATABASE_URL
});

const redis = new Redis(REDIS_URL);

type ReputationUpdate = {
  factionId: string;
  delta: number;
  reason: "DIRECT_ACTION" | "FACTION_RELATIONSHIP";
};

type NpcMemory = {
  npcId: string;
  playerId: string;
  memoryType: string;
  description: string;
  intensity: number;
  relatedEventId: string;
};

type WorldStateFlag = {
  playerId: string;
  flagKey: string;
  flagValue: boolean;
  description: string;
  sourceEventId: string;
};

type ProcessResult = {
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

function getBaseReputationDelta(event: PlayerActionCreatedEvent): number {
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

function buildNpcMemory(event: PlayerActionCreatedEvent): NpcMemory | null {
  if (!event.npcId) {
    return null;
  }

  const npcId = event.npcId.toLowerCase();

  switch (event.actionType) {
    case "HELP_FACTION":
      return {
        npcId,
        playerId: event.playerId,
        memoryType: "HELPED_ALLIED_FACTION",
        description: `Player helped ${event.targetFaction ?? "an allied faction"} near this NPC.`,
        intensity: 6,
        relatedEventId: event.eventId
      };

    case "DONATE_RESOURCE":
      return {
        npcId,
        playerId: event.playerId,
        memoryType: "DONATED_RESOURCE",
        description: `Player donated ${event.metadata?.amount ?? "some"} ${event.metadata?.resource ?? "resources"}.`,
        intensity: 7,
        relatedEventId: event.eventId
      };

    case "ROB_NPC":
      return {
        npcId,
        playerId: event.playerId,
        memoryType: "ROBBED_BY_PLAYER",
        description: "Player robbed this NPC during an encounter.",
        intensity: 9,
        relatedEventId: event.eventId
      };

    case "SPARE_ENEMY":
      return {
        npcId,
        playerId: event.playerId,
        memoryType: "SPARED_BY_PLAYER",
        description: "Player spared this NPC instead of killing or capturing them.",
        intensity: 8,
        relatedEventId: event.eventId
      };

    case "ATTACK_FACTION":
      return {
        npcId,
        playerId: event.playerId,
        memoryType: "ATTACKED_FACTION",
        description: `Player attacked ${event.targetFaction ?? "this NPC's faction"}.`,
        intensity: 8,
        relatedEventId: event.eventId
      };

    default:
      return null;
  }
}

function buildWorldStateFlags(event: PlayerActionCreatedEvent): WorldStateFlag[] {
  const targetFactionId = normalizeFactionId(event.targetFaction);
  const npcId = event.npcId?.toLowerCase();
  const resource = event.metadata?.resource?.toLowerCase();

  const flags: WorldStateFlag[] = [];

  if (
    event.actionType === "DONATE_RESOURCE" &&
    targetFactionId === "survivors" &&
    resource === "medicine"
  ) {
    flags.push({
      playerId: event.playerId,
      flagKey: "survivors_clinic_supplied",
      flagValue: true,
      description:
        "Dusthaven Clinic has enough medicine because the player donated supplies to the Survivors.",
      sourceEventId: event.eventId
    });
  }

  if (event.actionType === "ROB_NPC" && npcId === "mara") {
    flags.push({
      playerId: event.playerId,
      flagKey: "trader_market_unstable",
      flagValue: true,
      description: "Mara's trade post is unstable after the player robbed her.",
      sourceEventId: event.eventId
    });
  }

  if (event.actionType === "SPARE_ENEMY" && npcId === "knox") {
    flags.push({
      playerId: event.playerId,
      flagKey: "raider_checkpoint_ambush_disabled",
      flagValue: true,
      description:
        "Knox remembers being spared, reducing the chance of a Raider checkpoint ambush.",
      sourceEventId: event.eventId
    });
  }

  if (event.actionType === "ATTACK_FACTION" && targetFactionId === "raiders") {
    flags.push({
      playerId: event.playerId,
      flagKey: "raider_checkpoint_hostile",
      flagValue: true,
      description:
        "The Raider checkpoint has become hostile after the player attacked Raiders.",
      sourceEventId: event.eventId
    });
  }

  return flags;
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
  event: PlayerActionCreatedEvent
): Promise<boolean> {
  const targetFactionId = normalizeFactionId(event.targetFaction);

  if (!targetFactionId) {
    console.log("[worker] No target faction present. Skipping reputation update.");
    return false;
  }

  const baseDelta = getBaseReputationDelta(event);

  const directUpdate: ReputationUpdate = {
    factionId: targetFactionId,
    delta: baseDelta,
    reason: "DIRECT_ACTION"
  };

  const relationshipUpdates = await getRelationshipBasedUpdates(
    targetFactionId,
    baseDelta
  );

  const allUpdates = [directUpdate, ...relationshipUpdates];

  for (const update of allUpdates) {
    await applySingleReputationUpdate(event.playerId, update);
  }

  return allUpdates.length > 0;
}

async function saveNpcMemory(event: PlayerActionCreatedEvent): Promise<boolean> {
  const memory = buildNpcMemory(event);

  if (!memory) {
    console.log("[worker] No NPC memory generated for this event.");
    return false;
  }

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
      memory.npcId,
      memory.playerId,
      memory.memoryType,
      memory.description,
      memory.intensity,
      memory.relatedEventId
    ]
  );

  console.log(
    `[worker] NPC memory saved: npc=${memory.npcId}, player=${memory.playerId}, memory=${memory.memoryType}, intensity=${memory.intensity}`
  );

  return true;
}

async function saveWorldStateFlags(
  event: PlayerActionCreatedEvent
): Promise<boolean> {
  const flags = buildWorldStateFlags(event);

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
        flag.playerId,
        flag.flagKey,
        flag.flagValue,
        flag.description,
        flag.sourceEventId
      ]
    );

    console.log(
      `[worker] World state flag updated: player=${flag.playerId}, flag=${flag.flagKey}, value=${flag.flagValue}`
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
  await saveChoiceEvent(event);

  const reputationUpdated = await applyReputationUpdates(event);
  const npcMemoryUpdated = await saveNpcMemory(event);
  const worldStateUpdated = await saveWorldStateFlags(event);

  const result: ProcessResult = {
    reputationUpdated,
    npcMemoryUpdated,
    worldStateUpdated
  };

  await refreshRedisActiveState(event, result);

  return result;
}

async function startWorker() {
  await db.query("SELECT 1");
  console.log("[worker] Connected to PostgreSQL");

  await redis.ping();
  console.log("[worker] Connected to Redis");

  const kafka = new Kafka({
    clientId: "remnant-worker",
    brokers: [KAFKA_BROKER]
  });

  const consumer = kafka.consumer({
    groupId: CONSUMER_GROUP_ID
  });

  await consumer.connect();

  console.log(`[worker] Connected to Kafka broker: ${KAFKA_BROKER}`);

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