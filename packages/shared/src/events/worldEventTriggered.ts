import { z } from "zod";

export const WORLD_EVENT_TRIGGERED_TOPIC = "world.event.triggered";

export const WorldEventTypeSchema = z.enum([
  "CLINIC_SUPPLIES_LOW",
  "RAIDER_RETALIATION",
  "TRADER_MARKET_DISRUPTED",
  "FACTION_TENSION_SHIFT",
  "WORLD_STATE_RECOVERY"
]);

export const WorldEventTriggeredEventSchema = z.object({
  eventId: z.string().min(1),
  eventType: z.literal("world.event.triggered"),
  occurredAt: z.string().datetime(),
  playerId: z.string().min(1).optional(),
  worldEventType: WorldEventTypeSchema,
  source: z.enum(["scheduler", "system", "admin"]).default("scheduler"),
  severity: z.enum(["LOW", "MEDIUM", "HIGH"]).default("MEDIUM"),
  description: z.string().min(1),
  affectedFaction: z.string().min(1).optional(),
  affectedNpcId: z.string().min(1).optional(),
  flagKey: z.string().min(1).optional(),
  metadata: z
    .object({
      reason: z.string().optional(),
      previousValue: z.string().optional(),
      newValue: z.string().optional(),
      count: z.number().optional(),
      location: z.string().optional(),
      notes: z.string().optional()
    })
    .optional()
});

export type WorldEventType = z.infer<typeof WorldEventTypeSchema>;
export type WorldEventTriggeredEvent = z.infer<
  typeof WorldEventTriggeredEventSchema
>;