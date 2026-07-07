import { z } from "zod";

export const PLAYER_ACTION_CREATED_TOPIC = "player.action.created";

export const PlayerActionTypeSchema = z.enum([
  "HELP_FACTION",
  "ROB_NPC",
  "SPARE_ENEMY",
  "ATTACK_FACTION",
  "DONATE_RESOURCE"
]);

export const PlayerActionCreatedEventSchema = z.object({
  eventId: z.string().min(1),
  eventType: z.literal("player.action.created"),
  occurredAt: z.string().datetime(),

  playerId: z.string().min(1),
  actionType: PlayerActionTypeSchema,

  targetFaction: z.string().optional(),
  npcId: z.string().optional(),

  metadata: z
    .object({
      resource: z.string().optional(),
      amount: z.number().optional(),
      location: z.string().optional(),
      notes: z.string().optional()
    })
    .optional()
});

export const CreatePlayerActionRequestSchema =
  PlayerActionCreatedEventSchema.omit({
    eventId: true,
    eventType: true,
    occurredAt: true
  });

export type PlayerActionType = z.infer<typeof PlayerActionTypeSchema>;

export type PlayerActionCreatedEvent = z.infer<
  typeof PlayerActionCreatedEventSchema
>;

export type CreatePlayerActionRequest = z.infer<
  typeof CreatePlayerActionRequestSchema
>;