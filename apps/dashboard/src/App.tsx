import { useEffect, useMemo, useState } from "react";
import FactionWorldMap from "./components/FactionWorldMap";
import {
  START_SCENE_ID,
  ScenarioChoice,
  scenarioNodes
} from "./game/scenario";
import {
  Bar,
  BarChart,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis
} from "recharts";

const API_BASE_URL = "http://localhost:3000";
const WS_URL = "ws://localhost:3000/ws/players/player_001";
const PLAYER_ID = "player_001";

type ReputationRow = {
  faction_id: string;
  faction_name: string;
  reputation_score: number;
  updated_at: string;
};

type TimelineEvent = {
  event_id: string;
  occurred_at: string;
  player_id?: string;
  action_type: string;
  target_faction: string | null;
  npc_id: string | null;
  metadata: Record<string, unknown>;
};

type WorldStateFlag = {
  flag_key: string;
  flag_value: boolean;
  description: string;
  source_event_id: string | null;
  updated_at: string;
};

type PlayerStateResponse = {
  player: {
    id: string;
    display_name: string;
    created_at: string;
  };
  reputation: ReputationRow[];
  recentTimeline: TimelineEvent[];
  worldState: WorldStateFlag[];
};

type NpcBehaviorResponse = {
  npc: {
    id: string;
    name: string;
    faction_id: string;
    faction_name: string;
    description: string;
  };
  playerId: string;
  behavior: string;
  reason: string;
  memories: Array<{
    id: number;
    memory_type: string;
    description: string;
    intensity: number;
    created_at: string;
  }>;
};

type LiveMessage = {
  type: string;
  playerId?: string;
  eventId?: string;
  actionType?: string;
  targetFaction?: string | null;
  npcId?: string | null;
  reputationUpdated?: boolean;
  npcMemoryUpdated?: boolean;
  worldStateUpdated?: boolean;
  occurredAt?: string;
};

async function fetchJson<T>(url: string): Promise<T> {
  const response = await fetch(url);

  if (!response.ok) {
    throw new Error(`Request failed: ${response.status}`);
  }

  return response.json() as Promise<T>;
}

function formatDate(value: string): string {
  return new Date(value).toLocaleString();
}

function App() {
  const [currentSceneId, setCurrentSceneId] = useState(START_SCENE_ID);
  const [playerState, setPlayerState] = useState<PlayerStateResponse | null>(
    null
  );
  const [selectedNpc, setSelectedNpc] = useState("elias");
  const [npcBehavior, setNpcBehavior] = useState<NpcBehaviorResponse | null>(
    null
  );
  const [liveMessages, setLiveMessages] = useState<LiveMessage[]>([]);
  const [connectionStatus, setConnectionStatus] = useState("Disconnected");
  const [loadingChoiceId, setLoadingChoiceId] = useState<string | null>(null);
  const [error, setError] = useState("");

  const currentScene = scenarioNodes[currentSceneId];

  const reputationChartData = useMemo(() => {
    return (
      playerState?.reputation.map((row) => ({
        faction: row.faction_name,
        score: row.reputation_score
      })) ?? []
    );
  }, [playerState]);

  async function loadPlayerState() {
    const state = await fetchJson<PlayerStateResponse>(
      `${API_BASE_URL}/players/${PLAYER_ID}/state`
    );

    setPlayerState(state);
  }

  async function loadNpcBehavior(npcId: string) {
    const behavior = await fetchJson<NpcBehaviorResponse>(
      `${API_BASE_URL}/npcs/${npcId}/behavior?playerId=${PLAYER_ID}`
    );

    setNpcBehavior(behavior);
  }

  async function refreshView(npcId = selectedNpc) {
    await loadPlayerState();
    await loadNpcBehavior(npcId);
  }

  async function handleScenarioChoice(choice: ScenarioChoice) {
    setLoadingChoiceId(choice.id);
    setError("");

    try {
      if (choice.action) {
        const response = await fetch(`${API_BASE_URL}/actions`, {
          method: "POST",
          headers: {
            "Content-Type": "application/json"
          },
          body: JSON.stringify({
            playerId: PLAYER_ID,
            actionType: choice.action.actionType,
            targetFaction: choice.action.targetFaction,
            npcId: choice.action.npcId,
            metadata: choice.action.metadata
          })
        });

        if (!response.ok) {
          throw new Error(`Action failed: ${response.status}`);
        }

        setSelectedNpc(choice.action.npcId);
        await refreshView(choice.action.npcId);
      } else {
        await refreshView();
      }

      setCurrentSceneId(choice.nextSceneId);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unknown error");
    } finally {
      setLoadingChoiceId(null);
    }
  }

  function resetScenario() {
    setCurrentSceneId(START_SCENE_ID);
    setSelectedNpc("elias");
  }

  useEffect(() => {
    refreshView().catch((err) => {
      setError(err instanceof Error ? err.message : "Failed to load state");
    });
  }, []);

  useEffect(() => {
    loadNpcBehavior(selectedNpc).catch((err) => {
      setError(
        err instanceof Error ? err.message : "Failed to load NPC behavior"
      );
    });
  }, [selectedNpc]);

  useEffect(() => {
    const socket = new WebSocket(WS_URL);

    socket.onopen = () => {
      setConnectionStatus("Connected");
    };

    socket.onmessage = async (event) => {
      const message = JSON.parse(event.data) as LiveMessage;

      setLiveMessages((current) => [message, ...current].slice(0, 10));

      if (message.type === "PLAYER_STATE_UPDATED") {
        await refreshView(selectedNpc);
      }
    };

    socket.onerror = () => {
      setConnectionStatus("Error");
    };

    socket.onclose = () => {
      setConnectionStatus("Disconnected");
    };

    return () => {
      socket.close();
    };
  }, [selectedNpc]);

  return (
    <main className="app">
      <header className="hero">
        <div>
          <p className="eyebrow">REMNANT</p>
          <h1>Survival Choice Engine</h1>
          <p>
            Play through a branching survival scenario where choices update
            faction trust, NPC memory, world-state flags, and live map feedback.
          </p>
        </div>

        <div className="status-card">
          <span className="status-label">Live Connection</span>
          <strong>{connectionStatus}</strong>
        </div>
      </header>

      {error && <div className="error">{error}</div>}

      <section className="play-layout">
        <section className="panel scenario-panel">
          <div className="scenario-topline">
            <p className="eyebrow">{currentScene.chapter}</p>

            <button className="reset-button" onClick={resetScenario}>
              Reset scene
            </button>
          </div>

          <h2>{currentScene.title}</h2>

          <p className="scenario-text">{currentScene.text}</p>

          <div className="choice-list">
            {currentScene.choices.map((choice) => (
              <button
                key={choice.id}
                disabled={loadingChoiceId !== null}
                onClick={() => handleScenarioChoice(choice)}
                className="choice-button"
              >
                <strong>
                  {loadingChoiceId === choice.id
                    ? "Processing..."
                    : choice.label}
                </strong>
                <span>{choice.description}</span>
              </button>
            ))}
          </div>
        </section>

        <section className="panel compact-map-panel">
          <FactionWorldMap
            reputation={playerState?.reputation ?? []}
            worldState={playerState?.worldState ?? []}
          />
        </section>
      </section>

      <section className="grid secondary-grid">
        <section className="panel">
          <h2>NPC Reaction</h2>

          <select
            value={selectedNpc}
            onChange={(event) => setSelectedNpc(event.target.value)}
          >
            <option value="elias">Elias</option>
            <option value="mara">Mara</option>
            <option value="knox">Knox</option>
          </select>

          {npcBehavior ? (
            <div className="npc-card">
              <h3>{npcBehavior.npc.name}</h3>
              <p>{npcBehavior.npc.description}</p>
              <div className="behavior">{npcBehavior.behavior}</div>
              <p>{npcBehavior.reason}</p>
            </div>
          ) : (
            <p className="empty">No NPC behavior loaded.</p>
          )}
        </section>

        <section className="panel">
          <h2>World State Changes</h2>

          <div className="list">
            {playerState?.worldState.length ? (
              playerState.worldState.map((flag) => (
                <div key={flag.flag_key} className="list-item">
                  <strong>{flag.flag_key}</strong>
                  <span>{flag.description}</span>
                  <small>{formatDate(flag.updated_at)}</small>
                </div>
              ))
            ) : (
              <p className="empty">No world-state flags yet.</p>
            )}
          </div>
        </section>

        <section className="panel timeline-panel">
          <h2>Recent Timeline</h2>

          <div className="list compact-timeline">
            {playerState?.recentTimeline.length ? (
              playerState.recentTimeline.slice(0, 6).map((event) => (
                <div key={event.event_id} className="list-item">
                  <strong>{event.action_type}</strong>
                  <span>
                    Target: {event.target_faction ?? "N/A"} · NPC:{" "}
                    {event.npc_id ?? "N/A"}
                  </span>
                  <small>{formatDate(event.occurred_at)}</small>
                </div>
              ))
            ) : (
              <p className="empty">No timeline events yet.</p>
            )}
          </div>
        </section>

        <section className="panel debug-panel">
          <details>
            <summary>Developer Debug Panel</summary>

            <div className="debug-grid">
              <section>
                <h3>Reputation Graph</h3>
                <p className="debug-note">
                  Hidden by default so the player does not see exact numerical
                  consequences during normal play.
                </p>

                <div className="chart-wrap">
                  <ResponsiveContainer width="100%" height={260}>
                    <BarChart data={reputationChartData}>
                      <CartesianGrid strokeDasharray="3 3" />
                      <XAxis dataKey="faction" />
                      <YAxis />
                      <Tooltip />
                      <Bar dataKey="score" />
                    </BarChart>
                  </ResponsiveContainer>
                </div>
              </section>

              <section>
                <h3>Live Event Feed</h3>

                <div className="list">
                  {liveMessages.length ? (
                    liveMessages.map((message, index) => (
                      <div
                        key={`${message.eventId ?? message.type}-${index}`}
                        className="list-item"
                      >
                        <strong>{message.type}</strong>
                        <span>{message.actionType ?? "System message"}</span>
                        <small>
                          {message.occurredAt
                            ? formatDate(message.occurredAt)
                            : ""}
                        </small>
                      </div>
                    ))
                  ) : (
                    <p className="empty">Waiting for live updates...</p>
                  )}
                </div>
              </section>
            </div>
          </details>
        </section>
      </section>
    </main>
  );
}

export default App;