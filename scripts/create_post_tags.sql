-- Создание и обновление материализованного представления post_tags

-- Проверяем и создаем функцию для извлечения тегов
CREATE OR REPLACE FUNCTION extract_tags(tags_text TEXT)
RETURNS TABLE(tag TEXT) AS $$
BEGIN
    IF tags_text IS NULL THEN
        RETURN;
END IF;

RETURN QUERY
SELECT
    unnest(
            string_to_array(
                    regexp_replace(
                            regexp_replace(tags_text, '[<>]', ' ', 'g'),
                            '\s+', ' ', 'g'
                    ),
                    ' '
            )
    ) AS tag
    WHERE length(tag) > 0;  -- Используем правильный алиас tag вместо unnest
END;
$$ LANGUAGE plpgsql;

-- Удаляем представление, если оно существует
DROP MATERIALIZED VIEW IF EXISTS post_tags;

-- Создаем представление с разобранными тегами для каждого поста
CREATE MATERIALIZED VIEW post_tags AS
SELECT
    p.id AS post_id,
    t.tag
FROM
    posts p,
    LATERAL extract_tags(p.tags) t
WHERE
    p.tags IS NOT NULL;

-- Индекс на представление для быстрого поиска постов по тегу
CREATE INDEX IF NOT EXISTS idx_post_tags_tag ON post_tags(tag);
CREATE INDEX IF NOT EXISTS idx_post_tags_post_id ON post_tags(post_id);