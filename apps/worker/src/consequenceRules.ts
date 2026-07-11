import type { PlayerActionCreatedEvent } from "@remnant/shared";

export type WorkerNpcMemory = {
  memoryType: string;
  description: string;
  intensity: number;
};

export type WorkerWorldFlag = {
  flagKey: string;
  flagValue: boolean;
  description: string;
};

export type WorkerConsequencePlan = {
  directReputationDelta: number;
  npcMemory: WorkerNpcMemory | null;
  worldFlags: WorkerWorldFlag[];
};

export function normalizeFactionId(factionName?: string): string | null {
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

export function getBaseReputationDelta(
  event: Pick<PlayerActionCreatedEvent, "actionType">
): number {
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

export function buildNpcMemory(
  event: Pick<
    PlayerActionCreatedEvent,
    "actionType" | "targetFaction" | "npcId" | "metadata"
  >
): WorkerNpcMemory | null {
  if (!event.npcId) {
    return null;
  }

  switch (event.actionType) {
    case "HELP_FACTION":
      return {
        memoryType: "HELPED_ALLIED_FACTION",
        description: `Player helped ${
          event.targetFaction ?? "an allied faction"
        } near this NPC.`,
        intensity: 6
      };

    case "DONATE_RESOURCE":
      return {
        memoryType: "DONATED_RESOURCE",
        description: `Player donated ${
          event.metadata?.amount ?? "some"
        } ${event.metadata?.resource ?? "resources"}.`,
        intensity: 7
      };

    case "ROB_NPC":
      return {
        memoryType: "ROBBED_BY_PLAYER",
        description: "Player robbed this NPC during an encounter.",
        intensity: 9
      };

    case "SPARE_ENEMY":
      return {
        memoryType: "SPARED_BY_PLAYER",
        description:
          "Player spared this NPC instead of killing or capturing them.",
        intensity: 8
      };

    case "ATTACK_FACTION":
      return {
        memoryType: "ATTACKED_FACTION",
        description: `Player attacked ${
          event.targetFaction ?? "this NPC's faction"
        }.`,
        intensity: 8
      };

    default:
      return null;
  }
}

export function buildWorldFlags(
  event: Pick<
    PlayerActionCreatedEvent,
    "actionType" | "targetFaction" | "npcId" | "metadata"
  >
): WorkerWorldFlag[] {
  const targetFactionId = normalizeFactionId(event.targetFaction);
  const npcId = event.npcId?.toLowerCase();
  const resource = event.metadata?.resource?.toLowerCase();

  const flags: WorkerWorldFlag[] = [];

  if (
    event.actionType === "DONATE_RESOURCE" &&
    targetFactionId === "survivors" &&
    resource === "medicine"
  ) {
    flags.push({
      flagKey: "survivors_clinic_supplied",
      flagValue: true,
      description:
        "Dusthaven Clinic has enough medicine because the player donated supplies to the Survivors."
    });
  }

  if (event.actionType === "ROB_NPC" && npcId === "mara") {
    flags.push({
      flagKey: "trader_market_unstable",
      flagValue: true,
      description: "Mara's trade post is unstable after the player robbed her."
    });
  }

  if (event.actionType === "SPARE_ENEMY" && npcId === "knox") {
    flags.push({
      flagKey: "raider_checkpoint_ambush_disabled",
      flagValue: true,
      description:
        "Knox remembers being spared, reducing the chance of a Raider checkpoint ambush."
    });
  }

  if (event.actionType === "ATTACK_FACTION" && targetFactionId === "raiders") {
    flags.push({
      flagKey: "raider_checkpoint_hostile",
      flagValue: true,
      description:
        "The Raider checkpoint has become hostile after the player attacked Raiders."
    });
  }

  return flags;
}

export function buildTypeScriptConsequencePlan(
  event: PlayerActionCreatedEvent
): WorkerConsequencePlan {
  return {
    directReputationDelta: getBaseReputationDelta(event),
    npcMemory: buildNpcMemory(event),
    worldFlags: buildWorldFlags(event)
  };
}