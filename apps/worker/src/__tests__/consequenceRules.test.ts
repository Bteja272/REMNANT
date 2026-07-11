import { describe, expect, it } from "vitest";
import type { PlayerActionCreatedEvent } from "@remnant/shared";
import {
  buildNpcMemory,
  buildTypeScriptConsequencePlan,
  buildWorldFlags,
  getBaseReputationDelta,
  normalizeFactionId
} from "../consequenceRules";

function makeEvent(
  overrides: Partial<PlayerActionCreatedEvent>
): PlayerActionCreatedEvent {
  return {
    eventId: "event_001",
    eventType: "player.action.created",
    occurredAt: new Date().toISOString(),
    playerId: "player_001",
    actionType: "DONATE_RESOURCE",
    targetFaction: "Survivors",
    npcId: "elias",
    metadata: {},
    ...overrides
  };
}

describe("worker consequence rules", () => {
  it("normalizes known faction names", () => {
    expect(normalizeFactionId("Survivors")).toBe("survivors");
    expect(normalizeFactionId("Raiders")).toBe("raiders");
    expect(normalizeFactionId("Traders")).toBe("traders");
    expect(normalizeFactionId("The Order")).toBe("order");
  });

  it("normalizes unknown faction names into ids", () => {
    expect(normalizeFactionId("Northern Guard")).toBe("northern_guard");
  });

  it("returns expected base reputation deltas", () => {
    expect(getBaseReputationDelta(makeEvent({ actionType: "HELP_FACTION" }))).toBe(15);
    expect(getBaseReputationDelta(makeEvent({ actionType: "DONATE_RESOURCE" }))).toBe(10);
    expect(getBaseReputationDelta(makeEvent({ actionType: "ROB_NPC" }))).toBe(-20);
    expect(getBaseReputationDelta(makeEvent({ actionType: "ATTACK_FACTION" }))).toBe(-25);
    expect(getBaseReputationDelta(makeEvent({ actionType: "SPARE_ENEMY" }))).toBe(8);
  });

  it("builds donated resource NPC memory", () => {
    const memory = buildNpcMemory(
      makeEvent({
        actionType: "DONATE_RESOURCE",
        npcId: "elias",
        metadata: {
          resource: "medicine",
          amount: 2
        }
      })
    );

    expect(memory).toEqual({
      memoryType: "DONATED_RESOURCE",
      description: "Player donated 2 medicine.",
      intensity: 7
    });
  });

  it("returns null NPC memory when no NPC is attached", () => {
    const memory = buildNpcMemory(
      makeEvent({
        actionType: "DONATE_RESOURCE",
        npcId: undefined
      })
    );

    expect(memory).toBeNull();
  });

  it("builds clinic supplied world flag for medicine donation to Survivors", () => {
    const flags = buildWorldFlags(
      makeEvent({
        actionType: "DONATE_RESOURCE",
        targetFaction: "Survivors",
        npcId: "elias",
        metadata: {
          resource: "medicine",
          amount: 2
        }
      })
    );

    expect(flags).toHaveLength(1);
    expect(flags[0].flagKey).toBe("survivors_clinic_supplied");
    expect(flags[0].flagValue).toBe(true);
  });

  it("builds trader market unstable flag when Mara is robbed", () => {
    const flags = buildWorldFlags(
      makeEvent({
        actionType: "ROB_NPC",
        targetFaction: "Traders",
        npcId: "mara"
      })
    );

    expect(flags).toHaveLength(1);
    expect(flags[0].flagKey).toBe("trader_market_unstable");
  });

  it("builds ambush disabled flag when Knox is spared", () => {
    const flags = buildWorldFlags(
      makeEvent({
        actionType: "SPARE_ENEMY",
        targetFaction: "Raiders",
        npcId: "knox"
      })
    );

    expect(flags).toHaveLength(1);
    expect(flags[0].flagKey).toBe("raider_checkpoint_ambush_disabled");
  });

  it("builds Raider hostile checkpoint flag when Raiders are attacked", () => {
    const flags = buildWorldFlags(
      makeEvent({
        actionType: "ATTACK_FACTION",
        targetFaction: "Raiders",
        npcId: "knox"
      })
    );

    expect(flags).toHaveLength(1);
    expect(flags[0].flagKey).toBe("raider_checkpoint_hostile");
  });

  it("builds full TypeScript consequence plan", () => {
    const plan = buildTypeScriptConsequencePlan(
      makeEvent({
        actionType: "DONATE_RESOURCE",
        targetFaction: "Survivors",
        npcId: "elias",
        metadata: {
          resource: "medicine",
          amount: 2
        }
      })
    );

    expect(plan.directReputationDelta).toBe(10);
    expect(plan.npcMemory?.memoryType).toBe("DONATED_RESOURCE");
    expect(plan.worldFlags[0].flagKey).toBe("survivors_clinic_supplied");
  });
});