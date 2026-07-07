import Fastify from "fastify";
import cors from "@fastify/cors";
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

const app = Fastify({
  logger: true
});

const db = new Pool({
  connectionString: DATABASE_URL
});

let producer: Producer;

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

app.get("/health", async () => {
  await db.query("SELECT 1");

  return {
    status: "ok",
    service: "remnant-api",
    kafkaBroker: KAFKA_BROKER,
    database: "connected"
  };
});

app.post("/actions", async (request, reply) => {
  const parsed = CreatePlayerActionRequestSchema.safeParse(request.body);

  if (!parsed.success) {
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

    return reply.status(200).send({
      status: "accepted",
      eventType: event.eventType,
      eventId: event.eventId
    });
  } catch (error) {
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

  return reply.status(200).send({
    player: playerResult.rows[0],
    reputation: reputationResult.rows,
    recentTimeline: timelineResult.rows
  });
});

async function start() {
  try {
    await db.query("SELECT 1");
    app.log.info("PostgreSQL connection verified");

    producer = await buildKafkaProducer();

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
  await db.end();
  await app.close();
  process.exit(0);
});

start();