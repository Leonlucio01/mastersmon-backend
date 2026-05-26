ALTER TABLE game.skills
ADD COLUMN IF NOT EXISTS energy_cost INTEGER NOT NULL DEFAULT 0;

ALTER TABLE game.skills
ADD COLUMN IF NOT EXISTS cooldown_turns INTEGER NOT NULL DEFAULT 0;

UPDATE game.skills
SET energy_cost = v.energy_cost,
    cooldown_turns = v.cooldown_turns,
    updated_at = now()
FROM (
  VALUES
    ('tackle', 0, 0),
    ('quick-attack', 10, 1),
    ('ember', 15, 1),
    ('water-gun', 15, 1),
    ('vine-whip', 15, 1),
    ('thunder-shock', 15, 1),
    ('rock-throw', 20, 1),
    ('poison-sting', 15, 1),
    ('bug-bite', 15, 1),
    ('gust', 15, 1),
    ('confusion', 20, 1),
    ('mud-slap', 15, 1),
    ('ice-shard', 20, 1),
    ('dragon-breath', 30, 2),
    ('bite', 25, 2),
    ('lick', 15, 1),
    ('karate-chop', 20, 1)
) AS v(slug, energy_cost, cooldown_turns)
WHERE game.skills.slug = v.slug;
