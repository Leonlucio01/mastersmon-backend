ALTER TABLE game.skills
ADD COLUMN IF NOT EXISTS effect_type TEXT;

ALTER TABLE game.skills
ADD COLUMN IF NOT EXISTS effect_chance INTEGER NOT NULL DEFAULT 0;

ALTER TABLE game.skills
ADD COLUMN IF NOT EXISTS effect_value INTEGER NOT NULL DEFAULT 0;

UPDATE game.skills
SET effect_type = v.effect_type,
    effect_chance = v.effect_chance,
    effect_value = v.effect_value,
    updated_at = now()
FROM (
  VALUES
    ('poison-sting', 'poison', 30, 0),
    ('ember', 'burn', 20, 0),
    ('thunder-shock', 'paralysis', 20, 0),
    ('mud-slap', 'accuracy_down', 30, 10)
) AS v(slug, effect_type, effect_chance, effect_value)
WHERE game.skills.slug = v.slug;

UPDATE game.skills
SET effect_type = 'attack_down',
    effect_chance = 100,
    effect_value = 10,
    updated_at = now()
WHERE slug = 'growl';

UPDATE game.skills
SET effect_type = 'defense_down',
    effect_chance = 100,
    effect_value = 10,
    updated_at = now()
WHERE slug = 'tail-whip';
