ALTER TABLE game.market_listings
DROP CONSTRAINT IF EXISTS market_listings_status_check;

ALTER TABLE game.market_listings
ADD CONSTRAINT market_listings_status_check
CHECK (status IN ('active', 'open', 'sold', 'cancelled', 'expired'));

ALTER TABLE game.market_listings
ALTER COLUMN status SET DEFAULT 'open';

ALTER TABLE game.market_listings
ADD COLUMN IF NOT EXISTS seller_user_id UUID REFERENCES game.users(id) ON DELETE CASCADE,
ADD COLUMN IF NOT EXISTS player_monster_id UUID REFERENCES game.player_monsters(id) ON DELETE CASCADE,
ADD COLUMN IF NOT EXISTS item_slug TEXT NULL,
ADD COLUMN IF NOT EXISTS quantity INTEGER NOT NULL DEFAULT 1,
ADD COLUMN IF NOT EXISTS buyer_user_id UUID NULL REFERENCES game.users(id) ON DELETE SET NULL,
ADD COLUMN IF NOT EXISTS cancelled_at TIMESTAMPTZ NULL,
ADD COLUMN IF NOT EXISTS expires_at TIMESTAMPTZ NULL,
ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now();

UPDATE game.market_listings ml
SET
  seller_user_id = COALESCE(seller_user_id, seller_id),
  player_monster_id = COALESCE(player_monster_id, monster_id),
  quantity = COALESCE(quantity, item_quantity, 1),
  item_slug = COALESCE(item_slug, i.slug),
  status = CASE WHEN status = 'active' THEN 'open' ELSE status END,
  updated_at = COALESCE(ml.updated_at, ml.created_at, now())
FROM game.items i
WHERE ml.item_id = i.id;

UPDATE game.market_listings
SET
  seller_user_id = COALESCE(seller_user_id, seller_id),
  player_monster_id = COALESCE(player_monster_id, monster_id),
  quantity = COALESCE(quantity, item_quantity, 1),
  status = CASE WHEN status = 'active' THEN 'open' ELSE status END,
  updated_at = COALESCE(updated_at, created_at, now())
WHERE item_id IS NULL;

ALTER TABLE game.market_listings
ALTER COLUMN seller_user_id SET NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_market_listings_open_monster
ON game.market_listings (player_monster_id)
WHERE status IN ('open', 'active') AND player_monster_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_market_listings_status_created
ON game.market_listings (status, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_market_listings_seller_status
ON game.market_listings (seller_user_id, status, created_at DESC);

ALTER TABLE game.auction_listings
DROP CONSTRAINT IF EXISTS auction_listings_status_check;

ALTER TABLE game.auction_listings
ADD CONSTRAINT auction_listings_status_check
CHECK (status IN ('active', 'open', 'sold', 'cancelled', 'expired'));

ALTER TABLE game.auction_listings
ALTER COLUMN status SET DEFAULT 'open',
ALTER COLUMN currency SET DEFAULT 'gold';

ALTER TABLE game.auction_listings
ADD COLUMN IF NOT EXISTS seller_user_id UUID REFERENCES game.users(id) ON DELETE CASCADE,
ADD COLUMN IF NOT EXISTS auction_type TEXT NOT NULL DEFAULT 'monster',
ADD COLUMN IF NOT EXISTS player_monster_id UUID REFERENCES game.player_monsters(id) ON DELETE CASCADE,
ADD COLUMN IF NOT EXISTS item_slug TEXT NULL,
ADD COLUMN IF NOT EXISTS quantity INTEGER NOT NULL DEFAULT 1,
ADD COLUMN IF NOT EXISTS starting_price_gold BIGINT NOT NULL DEFAULT 0,
ADD COLUMN IF NOT EXISTS current_price_gold BIGINT NOT NULL DEFAULT 0,
ADD COLUMN IF NOT EXISTS buyout_price_gold BIGINT NULL,
ADD COLUMN IF NOT EXISTS highest_bidder_user_id UUID NULL REFERENCES game.users(id) ON DELETE SET NULL,
ADD COLUMN IF NOT EXISTS sold_at TIMESTAMPTZ NULL,
ADD COLUMN IF NOT EXISTS cancelled_at TIMESTAMPTZ NULL,
ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now();

UPDATE game.auction_listings al
SET
  seller_user_id = COALESCE(seller_user_id, seller_id),
  auction_type = CASE WHEN item_id IS NOT NULL THEN 'item' ELSE 'monster' END,
  player_monster_id = COALESCE(player_monster_id, monster_id),
  quantity = COALESCE(quantity, item_quantity, 1),
  item_slug = COALESCE(item_slug, i.slug),
  starting_price_gold = COALESCE(NULLIF(starting_price_gold, 0), starting_price, 0),
  current_price_gold = GREATEST(COALESCE(current_price_gold, 0), COALESCE(starting_price, 0)),
  buyout_price_gold = COALESCE(buyout_price_gold, buyout_price),
  status = CASE WHEN status = 'active' THEN 'open' ELSE status END,
  currency = 'gold',
  updated_at = COALESCE(al.updated_at, al.created_at, now())
FROM game.items i
WHERE al.item_id = i.id;

UPDATE game.auction_listings
SET
  seller_user_id = COALESCE(seller_user_id, seller_id),
  auction_type = CASE WHEN item_id IS NOT NULL THEN 'item' ELSE 'monster' END,
  player_monster_id = COALESCE(player_monster_id, monster_id),
  quantity = COALESCE(quantity, item_quantity, 1),
  starting_price_gold = COALESCE(NULLIF(starting_price_gold, 0), starting_price, 0),
  current_price_gold = GREATEST(COALESCE(current_price_gold, 0), COALESCE(starting_price, 0)),
  buyout_price_gold = COALESCE(buyout_price_gold, buyout_price),
  status = CASE WHEN status = 'active' THEN 'open' ELSE status END,
  currency = 'gold',
  updated_at = COALESCE(updated_at, created_at, now())
WHERE item_id IS NULL;

ALTER TABLE game.auction_listings
ALTER COLUMN seller_user_id SET NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_auction_listings_open_monster
ON game.auction_listings (player_monster_id)
WHERE status IN ('open', 'active') AND player_monster_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_auction_listings_status_ends
ON game.auction_listings (status, ends_at, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_auction_listings_seller_status
ON game.auction_listings (seller_user_id, status, created_at DESC);

ALTER TABLE game.auction_bids
ADD COLUMN IF NOT EXISTS bidder_user_id UUID REFERENCES game.users(id) ON DELETE CASCADE,
ADD COLUMN IF NOT EXISTS bid_gold BIGINT NOT NULL DEFAULT 0;

UPDATE game.auction_bids
SET
  bidder_user_id = COALESCE(bidder_user_id, bidder_id),
  bid_gold = COALESCE(NULLIF(bid_gold, 0), amount, 0);

ALTER TABLE game.auction_bids
ALTER COLUMN bidder_user_id SET NOT NULL;

CREATE INDEX IF NOT EXISTS idx_auction_bids_auction_amount
ON game.auction_bids (auction_id, bid_gold DESC, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_auction_bids_bidder_created
ON game.auction_bids (bidder_user_id, created_at DESC);

CREATE TABLE IF NOT EXISTS game.market_history (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  event_type TEXT NOT NULL,
  listing_id UUID NULL REFERENCES game.market_listings(id) ON DELETE SET NULL,
  auction_id UUID NULL REFERENCES game.auction_listings(id) ON DELETE SET NULL,
  seller_user_id UUID NULL REFERENCES game.users(id) ON DELETE SET NULL,
  buyer_user_id UUID NULL REFERENCES game.users(id) ON DELETE SET NULL,
  bidder_user_id UUID NULL REFERENCES game.users(id) ON DELETE SET NULL,
  listing_type TEXT NULL,
  item_id UUID NULL REFERENCES game.items(id) ON DELETE SET NULL,
  item_slug TEXT NULL,
  player_monster_id UUID NULL REFERENCES game.player_monsters(id) ON DELETE SET NULL,
  quantity INTEGER NOT NULL DEFAULT 1,
  price_gold BIGINT NOT NULL DEFAULT 0,
  price_diamonds BIGINT NOT NULL DEFAULT 0,
  snapshot JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_market_history_users_created
ON game.market_history (seller_user_id, buyer_user_id, bidder_user_id, created_at DESC);

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
  ('buy_1_market', 'Compra 1 cosa en mercado', 'Compra una criatura o item en el Mercado global.', 'main', 'market_buy', 1, 1000, NULL, 0, 111),
  ('sell_1_market', 'Vende 1 cosa en mercado', 'Completa una venta en el Mercado global o Subastas.', 'main', 'market_sell', 1, 1000, NULL, 0, 112),
  ('bid_1_auction', 'Haz 1 puja', 'Realiza una puja en una subasta.', 'main', 'auction_bid', 1, 0, (SELECT id FROM game.items WHERE slug = 'poke-ball' LIMIT 1), 5, 113),
  ('win_1_auction', 'Gana 1 subasta', 'Gana una subasta o usa compra directa en subasta.', 'main', 'auction_win', 1, 0, (SELECT id FROM game.items WHERE slug = 'rare-candy' LIMIT 1), 1, 114)
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
