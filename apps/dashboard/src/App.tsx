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
const DEFAULT_PLAYER_ID = "player_001";

type Player = {
  id: string;
  display_name: string;
  created_at: string;
};

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
  player_id?: string;
  flag_key: string;
  flag_value: boolean;
  description: string;
  source_event_id: string | null;
  updated_at: string;
};

type WorldEvent = {
  event_id: string;
  event_type?: string;
  occurred_at: string;
  player_id?: string;
  world_event_type: string;
  source?: string;
  severity: "LOW" | "MEDIUM" | "HIGH";
  description: string;
  affected_faction: string | null;
  affected_npc_id: string | null;
  flag_key: string | null;
  metadata?: Record<string, unknown>;
};

type PlayerStateResponse = {
  player: Player;
  reputation: ReputationRow[];
  recentTimeline: TimelineEvent[];
  worldState: WorldStateFlag[];
  worldEvents: WorldEvent[];
};

type PlayersResponse = {
  players: Player[];
};

type SharedWorldStateResponse = {
  players: Player[];
  actionCounts: Array<{
    action_type: string;
    count: number;
  }>;
  factionActivity: Array<{
    faction: string;
    count: number;
  }>;
  latestEvents: TimelineEvent[];
  worldFlags: WorldStateFlag[];
  systemEvents: WorldEvent[];
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
  worldEventType?: string;
  severity?: string;
  description?: string;
  targetFaction?: string | null;
  npcId?: string | null;
  affectedFaction?: string | null;
  affectedNpcId?: string | null;
  flagKey?: string | null;
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

function formatLabel(value: string): string {
  return value.replace(/_/g, " ");
}

function App() {
  const [currentSceneId, setCurrentSceneId] = useState(START_SCENE_ID);
  const [selectedPlayerId, setSelectedPlayerId] = useState(DEFAULT_PLAYER_ID);
  const [players, setPlayers] = useState<Player[]>([]);
  const [newPlayerId, setNewPlayerId] = useState("player_002");
  const [newPlayerName, setNewPlayerName] = useState("Second Survivor");

  const [playerState, setPlayerState] = useState<PlayerStateResponse | null>(
    null
  );
  const [sharedWorldState, setSharedWorldState] =
    useState<SharedWorldStateResponse | null>(null);

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

  async function loadPlayers() {
    const response = await fetchJson<PlayersResponse>(`${API_BASE_URL}/players`);
    setPlayers(response.players);
  }

  async function loadSharedWorldState() {
    const state = await fetchJson<SharedWorldStateResponse>(
      `${API_BASE_URL}/world/shared-state`
    );

    setSharedWorldState(state);
  }

  async function loadPlayerState(playerId = selectedPlayerId) {
    const state = await fetchJson<PlayerStateResponse>(
      `${API_BASE_URL}/players/${playerId}/state`
    );

    setPlayerState({
      ...state,
      worldEvents: state.worldEvents ?? []
    });
  }

  async function loadNpcBehavior(npcId: string, playerId = selectedPlayerId) {
    const behavior = await fetchJson<NpcBehaviorResponse>(
      `${API_BASE_URL}/npcs/${npcId}/behavior?playerId=${playerId}`
    );

    setNpcBehavior(behavior);
  }

  async function refreshView(
    npcId = selectedNpc,
    playerId = selectedPlayerId
  ) {
    await Promise.all([
      loadPlayers(),
      loadPlayerState(playerId),
      loadNpcBehavior(npcId, playerId),
      loadSharedWorldState()
    ]);
  }

  async function createPlayer() {
    setError("");

    try {
      const response = await fetch(`${API_BASE_URL}/players`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json"
        },
        body: JSON.stringify({
          id: newPlayerId,
          displayName: newPlayerName
        })
      });

      if (!response.ok) {
        throw new Error(`Create player failed: ${response.status}`);
      }

      const data = (await response.json()) as { player: Player };

      setSelectedPlayerId(data.player.id);
      setCurrentSceneId(START_SCENE_ID);
      setSelectedNpc("elias");
      await refreshView("elias", data.player.id);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to create player");
    }
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
            playerId: selectedPlayerId,
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
        await refreshView(choice.action.npcId, selectedPlayerId);
      } else {
        await refreshView(selectedNpc, selectedPlayerId);
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
    refreshView("elias", selectedPlayerId).catch((err) => {
      setError(err instanceof Error ? err.message : "Failed to load state");
    });
  }, [selectedPlayerId]);

  useEffect(() => {
    loadNpcBehavior(selectedNpc, selectedPlayerId).catch((err) => {
      setError(
        err instanceof Error ? err.message : "Failed to load NPC behavior"
      );
    });
  }, [selectedNpc, selectedPlayerId]);

  useEffect(() => {
    const socket = new WebSocket(
      `ws://localhost:3000/ws/players/${selectedPlayerId}`
    );

    socket.onopen = () => {
      setConnectionStatus(`Connected: ${selectedPlayerId}`);
    };

    socket.onmessage = async (event) => {
      const message = JSON.parse(event.data) as LiveMessage;

      setLiveMessages((current) => [message, ...current].slice(0, 10));

      if (
        message.type === "PLAYER_STATE_UPDATED" ||
        message.type === "WORLD_EVENT_TRIGGERED"
      ) {
        await refreshView(selectedNpc, selectedPlayerId);
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
  }, [selectedPlayerId, selectedNpc]);

  return (
    <main className="app">
      <header className="hero">
        <div>
          <p className="eyebrow">REMNANT</p>
          <h1>Survival Choice Engine</h1>
          <p>
            Play through a branching survival scenario where choices update
            faction trust, NPC memory, world-state flags, scheduled events,
            multiplayer state, and live map feedback.
          </p>
        </div>

        <div className="status-card">
          <span className="status-label">Live Connection</span>
          <strong>{connectionStatus}</strong>
        </div>
      </header>

      {error && <div className="error">{error}</div>}

      <section className="panel player-switcher-panel">
        <div className="panel-header">
          <div>
            <p className="eyebrow">Multiplayer</p>
            <h2>Player Switching</h2>
          </div>
        </div>

        <div className="player-switcher-grid">
          <label>
            Active Player
            <select
              value={selectedPlayerId}
              onChange={(event) => {
                setSelectedPlayerId(event.target.value);
                setCurrentSceneId(START_SCENE_ID);
                setSelectedNpc("elias");
                setLiveMessages([]);
              }}
            >
              {players.length ? (
                players.map((player) => (
                  <option key={player.id} value={player.id}>
                    {player.display_name} ({player.id})
                  </option>
                ))
              ) : (
                <option value={DEFAULT_PLAYER_ID}>player_001</option>
              )}
            </select>
          </label>

          <label>
            New Player ID
            <input
              value={newPlayerId}
              onChange={(event) => setNewPlayerId(event.target.value)}
              placeholder="player_002"
            />
          </label>

          <label>
            Display Name
            <input
              value={newPlayerName}
              onChange={(event) => setNewPlayerName(event.target.value)}
              placeholder="Second Survivor"
            />
          </label>

          <button className="create-player-button" onClick={createPlayer}>
            Create / Switch Player
          </button>
        </div>
      </section>

      <section className="play-layout">
        <section className="panel scenario-panel">
          <div className="scenario-topline">
            <p className="eyebrow">
              {currentScene.chapter} · {selectedPlayerId}
            </p>

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

        <section className="panel world-events-panel">
          <div className="panel-header">
            <div>
              <p className="eyebrow">System Events</p>
              <h2>Scheduled World Events</h2>
            </div>
          </div>

          {playerState?.worldEvents.length ? (
            <div className="world-event-list">
              {playerState.worldEvents.map((event) => (
                <article
                  key={event.event_id}
                  className={`world-event-card severity-${event.severity.toLowerCase()}`}
                >
                  <div className="world-event-topline">
                    <span>{formatLabel(event.world_event_type)}</span>
                    <strong>{event.severity}</strong>
                  </div>

                  <p>{event.description}</p>

                  <div className="world-event-meta">
                    <span>{formatDate(event.occurred_at)}</span>

                    {event.affected_faction && (
                      <span>Faction: {event.affected_faction}</span>
                    )}

                    {event.affected_npc_id && (
                      <span>NPC: {event.affected_npc_id}</span>
                    )}
                  </div>
                </article>
              ))}
            </div>
          ) : (
            <p className="empty">No scheduled world events have triggered yet.</p>
          )}
        </section>

        <section className="panel timeline-panel">
          <h2>Recent Timeline</h2>

          <div className="list compact-timeline">
            {playerState?.recentTimeline.length ? (
              playerState.recentTimeline.slice(0, 6).map((event) => (
                <div key={event.event_id} className="list-item">
                  <strong>{event.action_type}</strong>
                  <span>
                    Player: {selectedPlayerId} · Target:{" "}
                    {event.target_faction ?? "N/A"} · NPC:{" "}
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

        <section className="panel shared-world-panel">
          <div className="panel-header">
            <div>
              <p className="eyebrow">Shared State</p>
              <h2>Global World Activity</h2>
            </div>
          </div>

          <div className="shared-world-grid">
            <div>
              <h3>Players</h3>
              <div className="list">
                {sharedWorldState?.players.length ? (
                  sharedWorldState.players.map((player) => (
                    <div key={player.id} className="list-item">
                      <strong>{player.display_name}</strong>
                      <span>{player.id}</span>
                    </div>
                  ))
                ) : (
                  <p className="empty">No players loaded.</p>
                )}
              </div>
            </div>

            <div>
              <h3>Action Counts</h3>
              <div className="list">
                {sharedWorldState?.actionCounts.length ? (
                  sharedWorldState.actionCounts.map((item) => (
                    <div key={item.action_type} className="list-item">
                      <strong>{item.action_type}</strong>
                      <span>{item.count} actions</span>
                    </div>
                  ))
                ) : (
                  <p className="empty">No shared action counts yet.</p>
                )}
              </div>
            </div>

            <div>
              <h3>Faction Activity</h3>
              <div className="list">
                {sharedWorldState?.factionActivity.length ? (
                  sharedWorldState.factionActivity.map((item) => (
                    <div key={item.faction} className="list-item">
                      <strong>{item.faction}</strong>
                      <span>{item.count} interactions</span>
                    </div>
                  ))
                ) : (
                  <p className="empty">No faction activity yet.</p>
                )}
              </div>
            </div>

            <div>
              <h3>Latest Shared Events</h3>
              <div className="list">
                {sharedWorldState?.latestEvents.length ? (
                  sharedWorldState.latestEvents.slice(0, 6).map((event) => (
                    <div key={event.event_id} className="list-item">
                      <strong>{event.action_type}</strong>
                      <span>
                        {event.player_id} → {event.target_faction ?? "N/A"}
                      </span>
                      <small>{formatDate(event.occurred_at)}</small>
                    </div>
                  ))
                ) : (
                  <p className="empty">No shared events yet.</p>
                )}
              </div>
            </div>
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
                        <span>
                          {message.actionType ??
                            message.worldEventType ??
                            "System message"}
                        </span>
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