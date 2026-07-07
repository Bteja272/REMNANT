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
    x: 55,
    y: 55,
    width: 230,
    height: 120,
    labelX: 170,
    labelY: 112,
    description: "Dusthaven Clinic"
  },
  {
    factionId: "traders",
    name: "Traders",
    x: 450,
    y: 45,
    width: 230,
    height: 120,
    labelX: 565,
    labelY: 102,
    description: "Mara's Trade Post"
  },
  {
    factionId: "raiders",
    name: "Raiders",
    x: 95,
    y: 255,
    width: 250,
    height: 120,
    labelX: 220,
    labelY: 312,
    description: "Raider Checkpoint"
  },
  {
    factionId: "order",
    name: "The Order",
    x: 455,
    y: 250,
    width: 225,
    height: 125,
    labelX: 568,
    labelY: 307,
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
    <div className="map-shell compact-map-shell">
      <div className="map-header compact-map-header">
        <div>
          <h2>World Map</h2>
          <p>Territory status updates after each committed choice.</p>
        </div>
      </div>

      <svg
        className="world-map compact-world-map"
        viewBox="0 0 760 430"
        role="img"
        aria-label="REMNANT compact faction world map"
      >
        <rect x="0" y="0" width="760" height="430" rx="24" className="map-bg" />

        <path
          d="M325 10 C310 95 335 165 300 220 C260 285 250 335 245 420"
          className="map-road"
        />
        <path
          d="M40 215 C160 180 260 225 385 205 C510 185 620 178 725 150"
          className="map-road"
        />

        <circle cx="365" cy="205" r="8" className="map-crossroad" />
        <text x="382" y="210" className="map-small-label">
          Dead Route
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
                rx="18"
                className={getTerritoryClass(score)}
              />

              <text
                x={territory.labelX}
                y={territory.labelY - 16}
                textAnchor="middle"
                className="map-faction-name"
              >
                {territory.name}
              </text>

              <text
                x={territory.labelX}
                y={territory.labelY + 6}
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
                {standing}
              </text>
            </g>
          );
        })}

        {clinicSupplied && (
          <g>
            <circle cx="260" cy="72" r="9" className="map-event-positive" />
            <text x="274" y="77" className="map-event-label">
              Clinic supplied
            </text>
          </g>
        )}

        {marketUnstable && (
          <g>
            <circle cx="650" cy="62" r="9" className="map-event-warning" />
            <text x="664" y="67" className="map-event-label">
              Market unstable
            </text>
          </g>
        )}

        {ambushDisabled && (
          <g>
            <circle cx="318" cy="270" r="9" className="map-event-positive" />
            <text x="332" y="275" className="map-event-label">
              Ambush disabled
            </text>
          </g>
        )}

        {raiderHostile && (
          <g>
            <circle cx="130" cy="355" r="9" className="map-event-danger" />
            <text x="144" y="360" className="map-event-label">
              Checkpoint hostile
            </text>
          </g>
        )}
      </svg>
    </div>
  );
}

export default FactionWorldMap;