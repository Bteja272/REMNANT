type ReputationRow = {
  faction_id: string;
  faction_name: string;
  reputation_score: number;
  updated_at: string;
};

type WorldStateFlag = {
  flag_key: string;
  flag_value: boolean;
  description: string;
  source_event_id: string | null;
  updated_at: string;
};

type FactionWorldMapProps = {
  reputation: ReputationRow[];
  worldState: WorldStateFlag[];
};

type Territory = {
  factionId: string;
  name: string;
  x: number;
  y: number;
  width: number;
  height: number;
  labelX: number;
  labelY: number;
  description: string;
};

const territories: Territory[] = [
  {
    factionId: "survivors",
    name: "Survivors",
    x: 70,
    y: 80,
    width: 220,
    height: 160,
    labelX: 180,
    labelY: 155,
    description: "Dusthaven Clinic"
  },
  {
    factionId: "traders",
    name: "Traders",
    x: 370,
    y: 70,
    width: 240,
    height: 150,
    labelX: 490,
    labelY: 145,
    description: "Mara's Trade Post"
  },
  {
    factionId: "raiders",
    name: "Raiders",
    x: 110,
    y: 320,
    width: 260,
    height: 160,
    labelX: 240,
    labelY: 395,
    description: "Raider Checkpoint"
  },
  {
    factionId: "order",
    name: "The Order",
    x: 470,
    y: 315,
    width: 220,
    height: 170,
    labelX: 580,
    labelY: 395,
    description: "Fort Providence"
  }
];

function getReputationScore(
  reputation: ReputationRow[],
  factionId: string
): number {
  const row = reputation.find((item) => item.faction_id === factionId);
  return row?.reputation_score ?? 0;
}

function getStandingLabel(score: number): string {
  if (score >= 40) return "Trusted";
  if (score >= 15) return "Friendly";
  if (score <= -40) return "Hostile";
  if (score <= -15) return "Suspicious";
  return "Neutral";
}

function getTerritoryClass(score: number): string {
  if (score >= 40) return "territory territory-trusted";
  if (score >= 15) return "territory territory-friendly";
  if (score <= -40) return "territory territory-hostile";
  if (score <= -15) return "territory territory-suspicious";
  return "territory territory-neutral";
}

function hasFlag(worldState: WorldStateFlag[], flagKey: string): boolean {
  return worldState.some((flag) => flag.flag_key === flagKey && flag.flag_value);
}

function FactionWorldMap({ reputation, worldState }: FactionWorldMapProps) {
  const clinicSupplied = hasFlag(worldState, "survivors_clinic_supplied");
  const marketUnstable = hasFlag(worldState, "trader_market_unstable");
  const ambushDisabled = hasFlag(
    worldState,
    "raider_checkpoint_ambush_disabled"
  );
  const raiderHostile = hasFlag(worldState, "raider_checkpoint_hostile");

  return (
    <div className="map-shell">
      <div className="map-header">
        <div>
          <h2>Faction World Map</h2>
          <p>
            Reputation and world-state flags are projected onto the current
            playable region.
          </p>
        </div>
      </div>

      <svg
        className="world-map"
        viewBox="0 0 760 560"
        role="img"
        aria-label="REMNANT faction world map"
      >
        <defs>
          <filter id="territoryGlow" x="-20%" y="-20%" width="140%" height="140%">
            <feDropShadow dx="0" dy="0" stdDeviation="5" floodOpacity="0.45" />
          </filter>
        </defs>

        <rect x="0" y="0" width="760" height="560" rx="28" className="map-bg" />

        <path
          d="M320 20 C300 120 330 210 280 285 C230 360 260 470 225 540"
          className="map-road"
        />
        <path
          d="M40 285 C160 250 270 300 380 265 C500 225 620 245 730 215"
          className="map-road"
        />

        <circle cx="345" cy="278" r="9" className="map-crossroad" />
        <text x="360" y="284" className="map-small-label">
          Dead Route Crossroads
        </text>

        {territories.map((territory) => {
          const score = getReputationScore(reputation, territory.factionId);
          const standing = getStandingLabel(score);

          return (
            <g key={territory.factionId}>
              <rect
                x={territory.x}
                y={territory.y}
                width={territory.width}
                height={territory.height}
                rx="22"
                className={getTerritoryClass(score)}
                filter="url(#territoryGlow)"
              />

              <text
                x={territory.labelX}
                y={territory.labelY - 22}
                textAnchor="middle"
                className="map-faction-name"
              >
                {territory.name}
              </text>

              <text
                x={territory.labelX}
                y={territory.labelY + 2}
                textAnchor="middle"
                className="map-description"
              >
                {territory.description}
              </text>

              <text
                x={territory.labelX}
                y={territory.labelY + 28}
                textAnchor="middle"
                className="map-score"
              >
                {standing} · {score}
              </text>
            </g>
          );
        })}

        {clinicSupplied && (
          <g>
            <circle cx="250" cy="105" r="13" className="map-event-positive" />
            <text x="270" y="111" className="map-event-label">
              Clinic supplied
            </text>
          </g>
        )}

        {marketUnstable && (
          <g>
            <circle cx="585" cy="100" r="13" className="map-event-warning" />
            <text x="605" y="106" className="map-event-label">
              Market unstable
            </text>
          </g>
        )}

        {ambushDisabled && (
          <g>
            <circle cx="330" cy="345" r="13" className="map-event-positive" />
            <text x="350" y="351" className="map-event-label">
              Ambush disabled
            </text>
          </g>
        )}

        {raiderHostile && (
          <g>
            <circle cx="160" cy="455" r="13" className="map-event-danger" />
            <text x="180" y="461" className="map-event-label">
              Checkpoint hostile
            </text>
          </g>
        )}
      </svg>

      <div className="map-legend">
        <span>
          <i className="legend-dot legend-trusted" /> Trusted/Friendly
        </span>
        <span>
          <i className="legend-dot legend-neutral" /> Neutral
        </span>
        <span>
          <i className="legend-dot legend-hostile" /> Suspicious/Hostile
        </span>
        <span>
          <i className="legend-dot legend-event" /> World event
        </span>
      </div>
    </div>
  );
}

export default FactionWorldMap;