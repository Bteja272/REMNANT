import { Pool } from "pg";
import Redis from "ioredis";
import {
  WorldEventTriggeredEvent,
  WorldEventTriggeredEventSchema
} from "@remnant/shared";

export type ScheduledWorldEventResult = {
  triggered: boolean;
  event?: WorldEventTriggeredEvent;
  reason: string;
};

type WorldFlagRow = {
  player_id: string;
  flag_key: string;
  flag_value: boolean;
  description: string;
  source_event_id: string | null;
  updated_at: Date;
};

type ChoiceEventCountRow = {
  count: string;
};

function createWorldEventId(): string {
  return `world_${crypto.randomUUID()}`;
}

async function insertSystemWorldEvent(
  db: Pool,
  event: WorldEventTriggeredEvent
): Promise<void> {
  await db.query(
    `
    INSERT INTO system_world_events (
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
      metadata
    )
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
    ON CONFLICT (event_id) DO NOTHING
    `,
    [
      event.eventId,
      event.eventType,
      event.occurredAt,
      event.playerId ?? null,
      event.worldEventType,
      event.source,
      event.severity,
      event.description,
      event.affectedFaction ?? null,
      event.affectedNpcId ?? null,
      event.flagKey ?? null,
      event.metadata ?? {}
    ]
  );
}

async function upsertWorldStateFlag(
  db: Pool,
  params: {
    playerId: string;
    flagKey: string;
    flagValue: boolean;
    description: string;
    sourceEventId: string;
  }
): Promise<void> {
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
      params.playerId,
      params.flagKey,
      params.flagValue,
      params.description,
      params.sourceEventId
    ]
  );
}

async function refreshRedisWorldState(
  db: Pool,
  redis: Redis,
  playerId: string
): Promise<void> {
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
}

async function addRedisLatestSystemEvent(
  redis: Redis,
  event: WorldEventTriggeredEvent
): Promise<void> {
  if (!event.playerId) {
    return;
  }

  const key = `player:${event.playerId}:latest_events`;

  await redis.lpush(
    key,
    JSON.stringify({
      eventId: event.eventId,
      eventType: event.eventType,
      occurredAt: event.occurredAt,
      actionType: event.worldEventType,
      targetFaction: event.affectedFaction ?? null,
      npcId: event.affectedNpcId ?? null,
      metadata: event.metadata ?? {},
      systemGenerated: true
    })
  );

  await redis.ltrim(key, 0, 24);
  await redis.expire(key, 3600);
}

async function publishRedisWorldEventUpdate(
  redis: Redis,
  event: WorldEventTriggeredEvent
): Promise<void> {
  if (!event.playerId) {
    return;
  }

  const channel = `channel:${event.playerId}:updates`;

  await redis.publish(
    channel,
    JSON.stringify({
      type: "WORLD_EVENT_TRIGGERED",
      playerId: event.playerId,
      eventId: event.eventId,
      worldEventType: event.worldEventType,
      severity: event.severity,
      description: event.description,
      affectedFaction: event.affectedFaction ?? null,
      affectedNpcId: event.affectedNpcId ?? null,
      flagKey: event.flagKey ?? null,
      occurredAt: event.occurredAt
    })
  );
}

async function hasFlag(
  db: Pool,
  playerId: string,
  flagKey: string
): Promise<boolean> {
  const result = await db.query<WorldFlagRow>(
    `
    SELECT *
    FROM world_state_flags
    WHERE player_id = $1
      AND flag_key = $2
      AND flag_value = true
    LIMIT 1
    `,
    [playerId, flagKey]
  );

  return (result.rowCount ?? 0) > 0;
}

async function wasWorldEventAlreadyTriggered(
  db: Pool,
  playerId: string,
  worldEventType: string,
  flagKey?: string
): Promise<boolean> {
  const result = await db.query(
    `
    SELECT event_id
    FROM system_world_events
    WHERE player_id = $1
      AND world_event_type = $2
      AND ($3::TEXT IS NULL OR flag_key = $3)
    LIMIT 1
    `,
    [playerId, worldEventType, flagKey ?? null]
  );

  return (result.rowCount ?? 0) > 0;
}

async function countRecentPlayerActions(
  db: Pool,
  playerId: string,
  actionType: string,
  targetFaction: string
): Promise<number> {
  const result = await db.query<ChoiceEventCountRow>(
    `
    SELECT COUNT(*)::TEXT AS count
    FROM choice_events
    WHERE player_id = $1
      AND action_type = $2
      AND LOWER(target_faction) = LOWER($3)
    `,
    [playerId, actionType, targetFaction]
  );

  return Number(result.rows[0]?.count ?? 0);
}

async function getActivePlayers(db: Pool): Promise<string[]> {
  const result = await db.query<{ id: string }>(
    `
    SELECT id
    FROM players
    ORDER BY id
    `
  );

  return result.rows.map((row) => row.id);
}

async function triggerClinicSuppliesLowIfNeeded(
  db: Pool,
  redis: Redis,
  playerId: string
): Promise<ScheduledWorldEventResult> {
  const clinicSupplied = await hasFlag(
    db,
    playerId,
    "survivors_clinic_supplied"
  );

  if (clinicSupplied) {
    return {
      triggered: false,
      reason: "Clinic is already supplied."
    };
  }

  const alreadyTriggered = await wasWorldEventAlreadyTriggered(
    db,
    playerId,
    "CLINIC_SUPPLIES_LOW",
    "survivors_clinic_supplies_low"
  );

  if (alreadyTriggered) {
    return {
      triggered: false,
      reason: "Clinic supplies low event was already triggered."
    };
  }

  const event: WorldEventTriggeredEvent = {
    eventId: createWorldEventId(),
    eventType: "world.event.triggered",
    occurredAt: new Date().toISOString(),
    playerId,
    worldEventType: "CLINIC_SUPPLIES_LOW",
    source: "scheduler",
    severity: "MEDIUM",
    description:
      "Dusthaven Clinic supplies are running low because medicine has not been delivered.",
    affectedFaction: "survivors",
    affectedNpcId: "elias",
    flagKey: "survivors_clinic_supplies_low",
    metadata: {
      reason: "The clinic has not received medicine.",
      location: "Dusthaven Clinic"
    }
  };

  const parsed = WorldEventTriggeredEventSchema.parse(event);

  await insertSystemWorldEvent(db, parsed);

  await upsertWorldStateFlag(db, {
    playerId,
    flagKey: "survivors_clinic_supplies_low",
    flagValue: true,
    description:
      "Dusthaven Clinic is running low on supplies because no medicine has been delivered.",
    sourceEventId: parsed.eventId
  });

  await refreshRedisWorldState(db, redis, playerId);
  await addRedisLatestSystemEvent(redis, parsed);
  await publishRedisWorldEventUpdate(redis, parsed);

  return {
    triggered: true,
    event: parsed,
    reason: "Clinic supplies low event triggered."
  };
}

async function triggerRaiderRetaliationIfNeeded(
  db: Pool,
  redis: Redis,
  playerId: string
): Promise<ScheduledWorldEventResult> {
  const attackCount = await countRecentPlayerActions(
    db,
    playerId,
    "ATTACK_FACTION",
    "Raiders"
  );

  if (attackCount < 2) {
    return {
      triggered: false,
      reason: "Not enough Raider attacks to trigger retaliation."
    };
  }

  const alreadyTriggered = await wasWorldEventAlreadyTriggered(
    db,
    playerId,
    "RAIDER_RETALIATION",
    "raider_retaliation_active"
  );

  if (alreadyTriggered) {
    return {
      triggered: false,
      reason: "Raider retaliation already active."
    };
  }

  const event: WorldEventTriggeredEvent = {
    eventId: createWorldEventId(),
    eventType: "world.event.triggered",
    occurredAt: new Date().toISOString(),
    playerId,
    worldEventType: "RAIDER_RETALIATION",
    source: "scheduler",
    severity: "HIGH",
    description:
      "Raiders have launched retaliation after repeated attacks from the player.",
    affectedFaction: "raiders",
    affectedNpcId: "knox",
    flagKey: "raider_retaliation_active",
    metadata: {
      reason: "Player attacked Raiders multiple times.",
      count: attackCount,
      location: "Raider Checkpoint"
    }
  };

  const parsed = WorldEventTriggeredEventSchema.parse(event);

  await insertSystemWorldEvent(db, parsed);

  await upsertWorldStateFlag(db, {
    playerId,
    flagKey: "raider_retaliation_active",
    flagValue: true,
    description:
      "Raiders are actively retaliating after repeated attacks from the player.",
    sourceEventId: parsed.eventId
  });

  await refreshRedisWorldState(db, redis, playerId);
  await addRedisLatestSystemEvent(redis, parsed);
  await publishRedisWorldEventUpdate(redis, parsed);

  return {
    triggered: true,
    event: parsed,
    reason: "Raider retaliation event triggered."
  };
}

async function triggerTraderMarketDisruptedIfNeeded(
  db: Pool,
  redis: Redis,
  playerId: string
): Promise<ScheduledWorldEventResult> {
  const marketUnstable = await hasFlag(db, playerId, "trader_market_unstable");

  if (!marketUnstable) {
    return {
      triggered: false,
      reason: "Trader market is not unstable."
    };
  }

  const alreadyTriggered = await wasWorldEventAlreadyTriggered(
    db,
    playerId,
    "TRADER_MARKET_DISRUPTED",
    "trader_market_disrupted"
  );

  if (alreadyTriggered) {
    return {
      triggered: false,
      reason: "Trader market disruption already triggered."
    };
  }

  const event: WorldEventTriggeredEvent = {
    eventId: createWorldEventId(),
    eventType: "world.event.triggered",
    occurredAt: new Date().toISOString(),
    playerId,
    worldEventType: "TRADER_MARKET_DISRUPTED",
    source: "scheduler",
    severity: "MEDIUM",
    description:
      "The trader market has become disrupted after instability around Mara's trade post.",
    affectedFaction: "traders",
    affectedNpcId: "mara",
    flagKey: "trader_market_disrupted",
    metadata: {
      reason: "Trader market was previously marked unstable.",
      location: "Mara's Trade Post"
    }
  };

  const parsed = WorldEventTriggeredEventSchema.parse(event);

  await insertSystemWorldEvent(db, parsed);

  await upsertWorldStateFlag(db, {
    playerId,
    flagKey: "trader_market_disrupted",
    flagValue: true,
    description:
      "The trader market is disrupted after escalating instability around Mara's trade post.",
    sourceEventId: parsed.eventId
  });

  await refreshRedisWorldState(db, redis, playerId);
  await addRedisLatestSystemEvent(redis, parsed);
  await publishRedisWorldEventUpdate(redis, parsed);

  return {
    triggered: true,
    event: parsed,
    reason: "Trader market disruption event triggered."
  };
}

export async function runScheduledWorldEventCheck(
  db: Pool,
  redis: Redis
): Promise<ScheduledWorldEventResult[]> {
  const playerIds = await getActivePlayers(db);
  const results: ScheduledWorldEventResult[] = [];

  for (const playerId of playerIds) {
    results.push(await triggerClinicSuppliesLowIfNeeded(db, redis, playerId));
    results.push(await triggerRaiderRetaliationIfNeeded(db, redis, playerId));
    results.push(await triggerTraderMarketDisruptedIfNeeded(db, redis, playerId));
  }

  return results;
}