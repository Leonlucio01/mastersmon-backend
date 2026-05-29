ALTER TABLE game.trade_offers
ADD COLUMN IF NOT EXISTS owner_user_id UUID REFERENCES game.users(id) ON DELETE CASCADE,
ADD COLUMN IF NOT EXISTS offered_player_monster_id UUID REFERENCES game.player_monsters(id) ON DELETE CASCADE,
ADD COLUMN IF NOT EXISTS requested_type TEXT NOT NULL DEFAULT 'any',
ADD COLUMN IF NOT EXISTS requested_type_slug TEXT NULL,
ADD COLUMN IF NOT EXISTS requested_rarity TEXT NULL,
ADD COLUMN IF NOT EXISTS requested_min_level INTEGER NULL,
ADD COLUMN IF NOT EXISTS requested_notes TEXT NULL,
ADD COLUMN IF NOT EXISTS accepted_by_user_id UUID NULL REFERENCES game.users(id) ON DELETE SET NULL,
ADD COLUMN IF NOT EXISTS accepted_player_monster_id UUID NULL REFERENCES game.player_monsters(id) ON DELETE SET NULL,
ADD COLUMN IF NOT EXISTS accepted_at TIMESTAMPTZ NULL,
ADD COLUMN IF NOT EXISTS cancelled_at TIMESTAMPTZ NULL,
ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now();

UPDATE game.trade_offers
SET
  owner_user_id = COALESCE(owner_user_id, user_id),
  offered_player_monster_id = COALESCE(offered_player_monster_id, offered_monster_id),
  requested_type = COALESCE(requested_type, CASE WHEN requested_species_id IS NOT NULL THEN 'species' ELSE 'any' END),
  updated_at = COALESCE(updated_at, created_at, now());

ALTER TABLE game.trade_offers
ALTER COLUMN owner_user_id SET NOT NULL,
ALTER COLUMN offered_player_monster_id SET NOT NULL;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'trade_offers_requested_type_check'
      AND conrelid = 'game.trade_offers'::regclass
  ) THEN
    ALTER TABLE game.trade_offers
    ADD CONSTRAINT trade_offers_requested_type_check
    CHECK (requested_type IN ('any', 'species', 'type', 'rarity', 'specific'));
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'trade_offers_no_self_accept_check'
      AND conrelid = 'game.trade_offers'::regclass
  ) THEN
    ALTER TABLE game.trade_offers
    ADD CONSTRAINT trade_offers_no_self_accept_check
    CHECK (accepted_by_user_id IS NULL OR accepted_by_user_id <> owner_user_id);
  END IF;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS idx_trade_offers_open_offered_monster
ON game.trade_offers (offered_player_monster_id)
WHERE status = 'open';

CREATE INDEX IF NOT EXISTS idx_trade_offers_status_created
ON game.trade_offers (status, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_trade_offers_owner_status
ON game.trade_offers (owner_user_id, status, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_trade_offers_accepted_user
ON game.trade_offers (accepted_by_user_id, accepted_at DESC);

CREATE TABLE IF NOT EXISTS game.trade_history (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  trade_offer_id UUID NOT NULL REFERENCES game.trade_offers(id) ON DELETE CASCADE,
  owner_user_id UUID NOT NULL REFERENCES game.users(id) ON DELETE CASCADE,
  accepted_by_user_id UUID NOT NULL REFERENCES game.users(id) ON DELETE CASCADE,
  offered_player_monster_id UUID NOT NULL REFERENCES game.player_monsters(id) ON DELETE CASCADE,
  accepted_player_monster_id UUID NOT NULL REFERENCES game.player_monsters(id) ON DELETE CASCADE,
  offered_snapshot JSONB,
  accepted_snapshot JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_trade_history_owner_created
ON game.trade_history (owner_user_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_trade_history_accepted_created
ON game.trade_history (accepted_by_user_id, created_at DESC);

INSERT INTO game.quests (
  slug,
  name,
  description,
  quest_type,
  target_type,
  target_value,
  reward_gold,
  reward_item_id,
  reward_item_quantity,
  sort_order
)
VALUES
  ('complete_1_trade', 'Completa 1 trade', 'Completa un intercambio real con otro entrenador.', 'main', 'trade_complete', 1, 1000, NULL, 0, 101),
  ('complete_3_trades', 'Completa 3 trades', 'Completa tres intercambios reales con otros entrenadores.', 'main', 'trade_complete', 3, 2000, (SELECT id FROM game.items WHERE slug = 'rare-candy' LIMIT 1), 1, 102),
  ('list_1_trade', 'Publica una oferta', 'Publica una criatura en el Trade Center.', 'main', 'trade_list', 1, 0, (SELECT id FROM game.items WHERE slug = 'poke-ball' LIMIT 1), 5, 100)
ON CONFLICT (slug) DO UPDATE
SET
  name = EXCLUDED.name,
  description = EXCLUDED.description,
  quest_type = EXCLUDED.quest_type,
  target_type = EXCLUDED.target_type,
  target_value = EXCLUDED.target_value,
  reward_gold = EXCLUDED.reward_gold,
  reward_item_id = EXCLUDED.reward_item_id,
  reward_item_quantity = EXCLUDED.reward_item_quantity,
  sort_order = EXCLUDED.sort_order,
  is_active = true,
  updated_at = now();
