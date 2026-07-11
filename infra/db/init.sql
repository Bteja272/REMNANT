CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS players (
    id TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS factions (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS faction_relationships (
    source_faction_id TEXT NOT NULL REFERENCES factions(id) ON DELETE CASCADE,
    affected_faction_id TEXT NOT NULL REFERENCES factions(id) ON DELETE CASCADE,
    relationship_type TEXT NOT NULL,
    influence_multiplier NUMERIC(5, 2) NOT NULL DEFAULT 0,
    PRIMARY KEY (source_faction_id, affected_faction_id)
);

CREATE TABLE IF NOT EXISTS player_faction_reputation (
    player_id TEXT NOT NULL REFERENCES players(id) ON DELETE CASCADE,
    faction_id TEXT NOT NULL REFERENCES factions(id) ON DELETE CASCADE,
    reputation_score INTEGER NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (player_id, faction_id)
);

CREATE TABLE IF NOT EXISTS faction_relationships (
    source_faction_id TEXT NOT NULL REFERENCES factions(id) ON DELETE CASCADE,
    affected_faction_id TEXT NOT NULL REFERENCES factions(id) ON DELETE CASCADE,
    relationship_type TEXT NOT NULL,
    influence_multiplier NUMERIC(5, 2) NOT NULL DEFAULT 0,
    PRIMARY KEY (source_faction_id, affected_faction_id)
);

CREATE TABLE IF NOT EXISTS npcs (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    faction_id TEXT REFERENCES factions(id) ON DELETE SET NULL,
    description TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS npc_memory (
    id BIGSERIAL PRIMARY KEY,
    npc_id TEXT NOT NULL,
    player_id TEXT NOT NULL REFERENCES players(id) ON DELETE CASCADE,
    memory_type TEXT NOT NULL,
    description TEXT NOT NULL,
    intensity INTEGER NOT NULL DEFAULT 5,
    related_event_id TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);


CREATE INDEX IF NOT EXISTS idx_npc_memory_player_npc
ON npc_memory (player_id, npc_id);

CREATE INDEX IF NOT EXISTS idx_npc_memory_created_at
ON npc_memory (created_at DESC);


CREATE TABLE IF NOT EXISTS world_state_flags (
    id BIGSERIAL PRIMARY KEY,
    player_id TEXT NOT NULL REFERENCES players(id) ON DELETE CASCADE,
    flag_key TEXT NOT NULL,
    flag_value BOOLEAN NOT NULL DEFAULT TRUE,
    description TEXT NOT NULL,
    source_event_id TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (player_id, flag_key)
);

CREATE INDEX IF NOT EXISTS idx_world_state_flags_player
ON world_state_flags (player_id);

CREATE INDEX IF NOT EXISTS idx_world_state_flags_updated_at
ON world_state_flags (updated_at DESC);


CREATE TABLE IF NOT EXISTS choice_events (
    event_id TEXT NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    player_id TEXT NOT NULL,
    action_type TEXT NOT NULL,
    target_faction TEXT,
    npc_id TEXT,
    metadata JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (event_id, occurred_at)
);

SELECT create_hypertable('choice_events', 'occurred_at', if_not_exists => TRUE);

INSERT INTO players (id, display_name)
VALUES
    ('player_001', 'Demo Player')
ON CONFLICT (id) DO NOTHING;

INSERT INTO factions (id, name, description)
VALUES
    ('survivors', 'Survivors', 'A civilian faction focused on rebuilding safe communities.'),
    ('raiders', 'Raiders', 'A hostile faction that survives through violence and theft.'),
    ('traders', 'Traders', 'A trade-focused faction controlling scarce resources.'),
    ('order', 'The Order', 'A militarized faction obsessed with discipline and control.')
ON CONFLICT (id) DO NOTHING;

INSERT INTO player_faction_reputation (player_id, faction_id, reputation_score)
VALUES
    ('player_001', 'survivors', 0),
    ('player_001', 'raiders', 0),
    ('player_001', 'traders', 0),
    ('player_001', 'order', 0)
ON CONFLICT (player_id, faction_id) DO NOTHING;

INSERT INTO faction_relationships (
    source_faction_id,
    affected_faction_id,
    relationship_type,
    influence_multiplier
)
VALUES
    ('survivors', 'raiders', 'hostile', -0.70),
    ('survivors', 'traders', 'friendly', 0.20),
    ('survivors', 'order', 'neutral', 0.00),

    ('raiders', 'survivors', 'hostile', -0.70),
    ('raiders', 'traders', 'hostile', -0.50),
    ('raiders', 'order', 'hostile', -0.40),

    ('traders', 'survivors', 'friendly', 0.20),
    ('traders', 'raiders', 'hostile', -0.50),
    ('traders', 'order', 'neutral', 0.00),

    ('order', 'survivors', 'neutral', 0.00),
    ('order', 'raiders', 'hostile', -0.40),
    ('order', 'traders', 'neutral', 0.00)
ON CONFLICT (source_faction_id, affected_faction_id) DO NOTHING;

INSERT INTO faction_relationships (
    source_faction_id,
    affected_faction_id,
    relationship_type,
    influence_multiplier
)
VALUES
    ('survivors', 'raiders', 'hostile', -0.70),
    ('survivors', 'traders', 'friendly', 0.20),
    ('survivors', 'order', 'neutral', 0.00),

    ('raiders', 'survivors', 'hostile', -0.70),
    ('raiders', 'traders', 'hostile', -0.50),
    ('raiders', 'order', 'hostile', -0.40),

    ('traders', 'survivors', 'friendly', 0.20),
    ('traders', 'raiders', 'hostile', -0.50),
    ('traders', 'order', 'neutral', 0.00),

    ('order', 'survivors', 'neutral', 0.00),
    ('order', 'raiders', 'hostile', -0.40),
    ('order', 'traders', 'neutral', 0.00)
ON CONFLICT (source_faction_id, affected_faction_id) DO NOTHING;

INSERT INTO npcs (id, name, faction_id, description)
VALUES
    ('elias', 'Elias', 'survivors', 'A Survivor doctor trying to keep Dusthaven Clinic alive.'),
    ('mara', 'Mara', 'traders', 'A cautious Trader who controls access to scarce supplies.'),
    ('knox', 'Knox', 'raiders', 'A Raider scout who remembers mercy and betrayal.')
ON CONFLICT (id) DO NOTHING;



CREATE TABLE IF NOT EXISTS system_world_events (
  event_id TEXT PRIMARY KEY,
  event_type TEXT NOT NULL DEFAULT 'world.event.triggered',
  occurred_at TIMESTAMPTZ NOT NULL,
  player_id TEXT REFERENCES players(id) ON DELETE CASCADE,
  world_event_type TEXT NOT NULL,
  source TEXT NOT NULL DEFAULT 'scheduler',
  severity TEXT NOT NULL DEFAULT 'MEDIUM',
  description TEXT NOT NULL,
  affected_faction TEXT REFERENCES factions(id) ON DELETE SET NULL,
  affected_npc_id TEXT REFERENCES npcs(id) ON DELETE SET NULL,
  flag_key TEXT,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_system_world_events_player_time
  ON system_world_events (player_id, occurred_at DESC);

CREATE INDEX IF NOT EXISTS idx_system_world_events_type_time
  ON system_world_events (world_event_type, occurred_at DESC);

CREATE INDEX IF NOT EXISTS idx_system_world_events_severity_time
  ON system_world_events (severity, occurred_at DESC);