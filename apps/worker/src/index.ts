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

const db = new Pool({
  connectionString: DATABASE_URL
});

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
): Promise<void> {
  const targetFactionId = normalizeFactionId(event.targetFaction);

  if (!targetFactionId) {
    console.log("[worker] No target faction present. Skipping reputation update.");
    return;
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
}

async function saveNpcMemory(event: PlayerActionCreatedEvent): Promise<void> {
  const memory = buildNpcMemory(event);

  if (!memory) {
    console.log("[worker] No NPC memory generated for this event.");
    return;
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
}

async function processPlayerAction(event: PlayerActionCreatedEvent): Promise<void> {
  await saveChoiceEvent(event);
  await applyReputationUpdates(event);
  await saveNpcMemory(event);
}

async function startWorker() {
  await db.query("SELECT 1");
  console.log("[worker] Connected to PostgreSQL");

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
        await processPlayerAction(event);
        console.log(
          "[worker] Event persisted, cascading consequences applied, and NPC memory updated\n"
        );
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
  await db.end();
  process.exit(0);
});

startWorker().catch((error) => {
  console.error("[worker] Failed to start worker", error);
  process.exit(1);
});