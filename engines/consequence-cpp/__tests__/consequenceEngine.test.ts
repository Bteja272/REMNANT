import { spawnSync } from "node:child_process";
import { existsSync } from "node:fs";
import { describe, expect, it } from "vitest";

const ENGINE_PATH = "engines/consequence-cpp/build/remnant_consequence_engine";

function runEngine(input: unknown) {
  if (!existsSync(ENGINE_PATH)) {
    throw new Error(
      `C++ engine not found at ${ENGINE_PATH}. Run: npm run build:cpp`
    );
  }

  const result = spawnSync(ENGINE_PATH, [], {
    input: JSON.stringify(input),
    encoding: "utf-8"
  });

  if (result.error) {
    throw result.error;
  }

  if (result.status !== 0) {
    throw new Error(
      `C++ engine failed with status ${result.status}. stderr=${result.stderr}`
    );
  }

  return JSON.parse(result.stdout) as {
    engine: string;
    engineVersion: string;
    eventId: string;
    playerId: string;
    actionType: string;
    directReputationDelta: number;
    npcMemory: null | {
      memoryType: string;
      description: string;
      intensity: number;
    };
    worldFlags: Array<{
      flagKey: string;
      flagValue: boolean;
      description: string;
    }>;
  };
}

describe("C++ consequence engine", () => {
  it("returns donation consequences for medicine donated to Survivors", () => {
    const output = runEngine({
      eventId: "event_001",
      playerId: "player_001",
      actionType: "DONATE_RESOURCE",
      targetFaction: "Survivors",
      npcId: "elias",
      metadata: {
        resource: "medicine",
        amount: 2
      }
    });

    expect(output.engine).toBe("cpp");
    expect(output.actionType).toBe("DONATE_RESOURCE");
    expect(output.directReputationDelta).toBe(10);
    expect(output.npcMemory?.memoryType).toBe("DONATED_RESOURCE");
    expect(output.npcMemory?.intensity).toBe(7);
    expect(output.worldFlags).toHaveLength(1);
    expect(output.worldFlags[0].flagKey).toBe("survivors_clinic_supplied");
  });

  it("returns robbery consequences when Mara is robbed", () => {
    const output = runEngine({
      eventId: "event_002",
      playerId: "player_001",
      actionType: "ROB_NPC",
      targetFaction: "Traders",
      npcId: "mara",
      metadata: {}
    });

    expect(output.engine).toBe("cpp");
    expect(output.directReputationDelta).toBe(-20);
    expect(output.npcMemory?.memoryType).toBe("ROBBED_BY_PLAYER");
    expect(output.npcMemory?.intensity).toBe(9);
    expect(output.worldFlags).toHaveLength(1);
    expect(output.worldFlags[0].flagKey).toBe("trader_market_unstable");
  });

  it("returns spared enemy consequences when Knox is spared", () => {
    const output = runEngine({
      eventId: "event_003",
      playerId: "player_001",
      actionType: "SPARE_ENEMY",
      targetFaction: "Raiders",
      npcId: "knox",
      metadata: {}
    });

    expect(output.engine).toBe("cpp");
    expect(output.directReputationDelta).toBe(8);
    expect(output.npcMemory?.memoryType).toBe("SPARED_BY_PLAYER");
    expect(output.worldFlags).toHaveLength(1);
    expect(output.worldFlags[0].flagKey).toBe(
      "raider_checkpoint_ambush_disabled"
    );
  });

  it("returns hostile checkpoint flag when Raiders are attacked", () => {
    const output = runEngine({
      eventId: "event_004",
      playerId: "player_001",
      actionType: "ATTACK_FACTION",
      targetFaction: "Raiders",
      npcId: "knox",
      metadata: {}
    });

    expect(output.engine).toBe("cpp");
    expect(output.directReputationDelta).toBe(-25);
    expect(output.npcMemory?.memoryType).toBe("ATTACKED_FACTION");
    expect(output.worldFlags).toHaveLength(1);
    expect(output.worldFlags[0].flagKey).toBe("raider_checkpoint_hostile");
  });

  it("returns no NPC memory when no npcId is provided", () => {
    const output = runEngine({
      eventId: "event_005",
      playerId: "player_001",
      actionType: "HELP_FACTION",
      targetFaction: "Survivors",
      metadata: {}
    });

    expect(output.engine).toBe("cpp");
    expect(output.directReputationDelta).toBe(15);
    expect(output.npcMemory).toBeNull();
  });
});