import { describe, expect, it } from "vitest";
import {
  CreatePlayerActionRequestSchema,
  PlayerActionCreatedEventSchema,
  PLAYER_ACTION_CREATED_TOPIC
} from "../playerActionCreated";

describe("player action shared schema", () => {
  it("uses the correct Kafka topic", () => {
    expect(PLAYER_ACTION_CREATED_TOPIC).toBe("player.action.created");
  });

  it("accepts a valid create player action request", () => {
    const result = CreatePlayerActionRequestSchema.safeParse({
      playerId: "player_001",
      actionType: "DONATE_RESOURCE",
      targetFaction: "Survivors",
      npcId: "elias",
      metadata: {
        resource: "medicine",
        amount: 2,
        location: "Dusthaven Clinic",
        notes: "Emergency supplies"
      }
    });

    expect(result.success).toBe(true);
  });

  it("rejects an invalid action type", () => {
    const result = CreatePlayerActionRequestSchema.safeParse({
      playerId: "player_001",
      actionType: "INVALID_ACTION",
      targetFaction: "Survivors"
    });

    expect(result.success).toBe(false);
  });

  it("accepts a valid full player action created event", () => {
    const result = PlayerActionCreatedEventSchema.safeParse({
      eventId: "event_001",
      eventType: "player.action.created",
      occurredAt: new Date().toISOString(),
      playerId: "player_001",
      actionType: "SPARE_ENEMY",
      targetFaction: "Raiders",
      npcId: "knox",
      metadata: {
        location: "Raider Checkpoint"
      }
    });

    expect(result.success).toBe(true);
  });

  it("rejects an event with the wrong event type", () => {
    const result = PlayerActionCreatedEventSchema.safeParse({
      eventId: "event_001",
      eventType: "wrong.event.type",
      occurredAt: new Date().toISOString(),
      playerId: "player_001",
      actionType: "SPARE_ENEMY",
      targetFaction: "Raiders",
      npcId: "knox"
    });

    expect(result.success).toBe(false);
  });
});