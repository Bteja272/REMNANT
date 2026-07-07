export type ScenarioActionType =
  | "HELP_FACTION"
  | "ROB_NPC"
  | "SPARE_ENEMY"
  | "ATTACK_FACTION"
  | "DONATE_RESOURCE";

export type ScenarioAction = {
  actionType: ScenarioActionType;
  targetFaction: string;
  npcId: string;
  metadata: Record<string, string | number>;
};

export type ScenarioChoice = {
  id: string;
  label: string;
  description: string;
  action?: ScenarioAction;
  nextSceneId: string;
};

export type ScenarioNode = {
  id: string;
  chapter: string;
  title: string;
  text: string;
  choices: ScenarioChoice[];
};

export const START_SCENE_ID = "dusthaven_clinic";

export const scenarioNodes: Record<string, ScenarioNode> = {
  dusthaven_clinic: {
    id: "dusthaven_clinic",
    chapter: "Chapter 1 · Dusthaven",
    title: "The Clinic Is Running Out of Medicine",
    text:
      "Elias, the Survivor doctor, is trying to keep the Dusthaven Clinic open. Mara's trade post has medicine, but she is guarding it closely. Raider scout Knox has been seen near the checkpoint. The next choice will decide who remembers you as an ally, a threat, or something in between.",
    choices: [
      {
        id: "give_medicine_to_elias",
        label: "Give medicine to Elias",
        description:
          "Support the Survivors and help stabilize the Dusthaven Clinic.",
        action: {
          actionType: "DONATE_RESOURCE",
          targetFaction: "Survivors",
          npcId: "elias",
          metadata: {
            resource: "medicine",
            amount: 2,
            location: "dusthaven_clinic"
          }
        },
        nextSceneId: "clinic_stabilized"
      },
      {
        id: "rob_mara_supplies",
        label: "Rob Mara's supplies",
        description:
          "Take medicine and water from the trade post without negotiating.",
        action: {
          actionType: "ROB_NPC",
          targetFaction: "Traders",
          npcId: "mara",
          metadata: {
            resource: "water",
            amount: 1,
            location: "mara_trade_post"
          }
        },
        nextSceneId: "market_unstable"
      },
      {
        id: "spare_knox",
        label: "Spare Knox",
        description:
          "Let the Raider scout leave after catching him near the checkpoint.",
        action: {
          actionType: "SPARE_ENEMY",
          targetFaction: "Raiders",
          npcId: "knox",
          metadata: {
            location: "raider_checkpoint",
            notes: "Player spared Knox after an ambush."
          }
        },
        nextSceneId: "knox_remembers"
      },
      {
        id: "attack_checkpoint",
        label: "Attack the Raider checkpoint",
        description:
          "Strike the Raiders first before they can threaten Dusthaven.",
        action: {
          actionType: "ATTACK_FACTION",
          targetFaction: "Raiders",
          npcId: "knox",
          metadata: {
            location: "raider_checkpoint"
          }
        },
        nextSceneId: "checkpoint_burning"
      }
    ]
  },

  clinic_stabilized: {
    id: "clinic_stabilized",
    chapter: "Chapter 1 · Consequence",
    title: "The Clinic Survives the Night",
    text:
      "Elias stores the medicine behind a locked cabinet and tells the Survivors that you kept the clinic alive. The wounded now have a place to recover, but others in the region notice that you chose the Survivors first.",
    choices: [
      {
        id: "help_survivor_patrol",
        label: "Help a Survivor patrol",
        description:
          "Strengthen your bond with the Survivors by securing the clinic road.",
        action: {
          actionType: "HELP_FACTION",
          targetFaction: "Survivors",
          npcId: "elias",
          metadata: {
            location: "clinic_road",
            notes: "Player helped Survivors secure the clinic road."
          }
        },
        nextSceneId: "survivor_route_secured"
      },
      {
        id: "visit_market_after_helping",
        label: "Visit Mara after helping Elias",
        description:
          "Try to keep trade relations alive after choosing the Survivors.",
        action: {
          actionType: "HELP_FACTION",
          targetFaction: "Traders",
          npcId: "mara",
          metadata: {
            location: "mara_trade_post",
            notes: "Player offered help after supporting the Survivors."
          }
        },
        nextSceneId: "mara_watches_you"
      }
    ]
  },

  market_unstable: {
    id: "market_unstable",
    chapter: "Chapter 1 · Consequence",
    title: "The Trade Post Locks Down",
    text:
      "Mara shuts the market gates before sunrise. Traders whisper that outsiders cannot be trusted, and prices rise across the settlement. The clinic may survive, but the road to Mara's trade post has become colder.",
    choices: [
      {
        id: "try_to_repair_market",
        label: "Offer to repair the damage",
        description:
          "Attempt to rebuild some trust with the Traders after the robbery.",
        action: {
          actionType: "HELP_FACTION",
          targetFaction: "Traders",
          npcId: "mara",
          metadata: {
            location: "mara_trade_post",
            notes: "Player tried to repair trust after robbing Mara."
          }
        },
        nextSceneId: "mara_watches_you"
      },
      {
        id: "double_down_against_traders",
        label: "Threaten the traders again",
        description:
          "Force the market to cooperate, even if it creates long-term enemies.",
        action: {
          actionType: "ATTACK_FACTION",
          targetFaction: "Traders",
          npcId: "mara",
          metadata: {
            location: "mara_trade_post"
          }
        },
        nextSceneId: "market_breaks"
      }
    ]
  },

  knox_remembers: {
    id: "knox_remembers",
    chapter: "Chapter 1 · Consequence",
    title: "Knox Leaves a Warning Mark",
    text:
      "Knox disappears into the ash road, but by morning a Raider warning mark has been scratched off the checkpoint wall. Someone inside the Raiders now knows you showed mercy.",
    choices: [
      {
        id: "approach_raider_checkpoint",
        label: "Approach the Raider checkpoint",
        description:
          "Use Knox's memory of mercy to reduce the chance of an ambush.",
        action: {
          actionType: "HELP_FACTION",
          targetFaction: "Raiders",
          npcId: "knox",
          metadata: {
            location: "raider_checkpoint",
            notes: "Player approached the checkpoint after sparing Knox."
          }
        },
        nextSceneId: "checkpoint_quiet"
      },
      {
        id: "warn_survivors_about_raiders",
        label: "Warn the Survivors",
        description:
          "Use what you learned from Knox to help the Survivors prepare.",
        action: {
          actionType: "HELP_FACTION",
          targetFaction: "Survivors",
          npcId: "elias",
          metadata: {
            location: "dusthaven_clinic",
            notes: "Player warned Survivors about Raider movement."
          }
        },
        nextSceneId: "survivor_route_secured"
      }
    ]
  },

  checkpoint_burning: {
    id: "checkpoint_burning",
    chapter: "Chapter 1 · Consequence",
    title: "The Checkpoint Burns",
    text:
      "Smoke rises over the Raider checkpoint. Dusthaven is safer for now, but the Raiders will remember the attack. Knox survives long enough to carry your description back into Raider territory.",
    choices: [
      {
        id: "fortify_clinic_after_attack",
        label: "Fortify the clinic",
        description:
          "Prepare the Survivors for possible Raider retaliation.",
        action: {
          actionType: "HELP_FACTION",
          targetFaction: "Survivors",
          npcId: "elias",
          metadata: {
            location: "dusthaven_clinic",
            notes: "Player fortified the clinic after attacking Raiders."
          }
        },
        nextSceneId: "survivor_route_secured"
      },
      {
        id: "push_deeper_into_raider_land",
        label: "Push deeper into Raider land",
        description:
          "Escalate the conflict before the Raiders can regroup.",
        action: {
          actionType: "ATTACK_FACTION",
          targetFaction: "Raiders",
          npcId: "knox",
          metadata: {
            location: "outer_raider_camp"
          }
        },
        nextSceneId: "raiders_harden"
      }
    ]
  },

  survivor_route_secured: {
    id: "survivor_route_secured",
    chapter: "Chapter 1 · Branch",
    title: "The Clinic Road Is Secured",
    text:
      "Survivor patrols begin moving medicine, water, and wounded civilians across the clinic road. Elias trusts you more openly now, but other factions may see your loyalty as a political choice.",
    choices: [
      {
        id: "return_to_crossroads_from_survivors",
        label: "Return to the crossroads",
        description: "Choose another path through Dusthaven.",
        nextSceneId: "dusthaven_clinic"
      }
    ]
  },

  mara_watches_you: {
    id: "mara_watches_you",
    chapter: "Chapter 1 · Branch",
    title: "Mara Does Not Forget",
    text:
      "Mara listens, but she does not relax. Traders are practical, not sentimental. If you keep helping the market, she may reopen trade. If you cross her again, the post may close to you permanently.",
    choices: [
      {
        id: "return_to_crossroads_from_mara",
        label: "Return to the crossroads",
        description: "Choose another path through Dusthaven.",
        nextSceneId: "dusthaven_clinic"
      }
    ]
  },

  market_breaks: {
    id: "market_breaks",
    chapter: "Chapter 1 · Branch",
    title: "The Market Starts to Break",
    text:
      "The trade post becomes quieter. Some merchants pack up before sunset. Mara's people now treat you as a threat, and Dusthaven loses one of its few stable supply lines.",
    choices: [
      {
        id: "return_to_crossroads_from_broken_market",
        label: "Return to the crossroads",
        description: "Choose another path through Dusthaven.",
        nextSceneId: "dusthaven_clinic"
      }
    ]
  },

  checkpoint_quiet: {
    id: "checkpoint_quiet",
    chapter: "Chapter 1 · Branch",
    title: "The Checkpoint Goes Quiet",
    text:
      "The Raider checkpoint does not open fire. Knox's memory bought you a narrow path through hostile ground. Mercy did not make you safe, but it changed the rules of the encounter.",
    choices: [
      {
        id: "return_to_crossroads_from_checkpoint",
        label: "Return to the crossroads",
        description: "Choose another path through Dusthaven.",
        nextSceneId: "dusthaven_clinic"
      }
    ]
  },

  raiders_harden: {
    id: "raiders_harden",
    chapter: "Chapter 1 · Branch",
    title: "The Raiders Harden Their Line",
    text:
      "The second attack does not scare the Raiders away. It organizes them. Patrols become more aggressive, and the checkpoint becomes a warning to anyone traveling through the ash road.",
    choices: [
      {
        id: "return_to_crossroads_from_raiders",
        label: "Return to the crossroads",
        description: "Choose another path through Dusthaven.",
        nextSceneId: "dusthaven_clinic"
      }
    ]
  }
};