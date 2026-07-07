import Fastify from "fastify";
import cors from "@fastify/cors";
import { Kafka, Producer } from "kafkajs";
import {
  CreatePlayerActionRequestSchema,
  PLAYER_ACTION_CREATED_TOPIC,
  PlayerActionCreatedEvent
} from "@remnant/shared";

const PORT = Number(process.env.PORT ?? 3000);
const HOST = process.env.HOST ?? "0.0.0.0";
const KAFKA_BROKER = process.env.KAFKA_BROKER ?? "localhost:9092";

const app = Fastify({
  logger: true
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
  return {
    status: "ok",
    service: "remnant-api",
    kafkaBroker: KAFKA_BROKER
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
      message: "The action was not accepted because the event could not be published."
    });
  }
});

async function start() {
  try {
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
  await app.close();
  process.exit(0);
});

start();