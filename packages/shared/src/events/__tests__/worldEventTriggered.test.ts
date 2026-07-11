import { describe, expect, it } from "vitest";
import {
  WORLD_EVENT_TRIGGERED_TOPIC,
  WorldEventTriggeredEventSchema
} from "../worldEventTriggered";

describe("world event triggered shared schema", () => {
  it("uses the correct Kafka topic", () => {
    expect(WORLD_EVENT_TRIGGERED_TOPIC).toBe("world.event.triggered");
  });

  it("accepts a valid scheduled world event", () => {
    const result = WorldEventTriggeredEventSchema.safeParse({
      eventId: "world_event_001",
      eventType: "world.event.triggered",
      occurredAt: new Date().toISOString(),
      playerId: "player_001",
      worldEventType: "CLINIC_SUPPLIES_LOW",
      source: "scheduler",
      severity: "MEDIUM",
      description: "Dusthaven Clinic supplies are running low.",
      affectedFaction: "survivors",
      affectedNpcId: "elias",
      flagKey: "survivors_clinic_supplies_low",
      metadata: {
        reason: "Clinic has not received enough medicine.",
        location: "Dusthaven Clinic"
      }
    });

    expect(result.success).toBe(true);
  });

  it("accepts a global world event without playerId", () => {
    const result = WorldEventTriggeredEventSchema.safeParse({
      eventId: "world_event_002",
      eventType: "world.event.triggered",
      occurredAt: new Date().toISOString(),
      worldEventType: "FACTION_TENSION_SHIFT",
      source: "scheduler",
      severity: "HIGH",
      description: "Faction tensions increased across the region.",
      affectedFaction: "raiders",
      metadata: {
        reason: "Repeated faction conflict."
      }
    });

    expect(result.success).toBe(true);
  });

  it("rejects an invalid world event type", () => {
    const result = WorldEventTriggeredEventSchema.safeParse({
      eventId: "world_event_003",
      eventType: "world.event.triggered",
      occurredAt: new Date().toISOString(),
      worldEventType: "INVALID_WORLD_EVENT",
      source: "scheduler",
      severity: "LOW",
      description: "Invalid event."
    });

    expect(result.success).toBe(false);
  });

  it("rejects an invalid eventType value", () => {
    const result = WorldEventTriggeredEventSchema.safeParse({
      eventId: "world_event_004",
      eventType: "player.action.created",
      occurredAt: new Date().toISOString(),
      worldEventType: "RAIDER_RETALIATION",
      source: "scheduler",
      severity: "HIGH",
      description: "Raiders retaliated."
    });

    expect(result.success).toBe(false);
  });

  it("applies default source and severity values", () => {
    const result = WorldEventTriggeredEventSchema.safeParse({
      eventId: "world_event_005",
      eventType: "world.event.triggered",
      occurredAt: new Date().toISOString(),
      worldEventType: "WORLD_STATE_RECOVERY",
      description: "The trader market has started to recover."
    });

    expect(result.success).toBe(true);

    if (result.success) {
      expect(result.data.source).toBe("scheduler");
      expect(result.data.severity).toBe("MEDIUM");
    }
  });
});