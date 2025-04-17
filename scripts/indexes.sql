-- Индексы для оптимизации запросов

BEGIN;

CREATE INDEX IF NOT EXISTS idx_users_reputation ON users(reputation);
CREATE INDEX IF NOT EXISTS idx_users_creation_date ON users(creation_date);

CREATE INDEX IF NOT EXISTS idx_posts_post_type_id ON posts(post_type_id);
CREATE INDEX IF NOT EXISTS idx_posts_owner_user_id ON posts(owner_user_id);
CREATE INDEX IF NOT EXISTS idx_posts_accepted_answer_id ON posts(accepted_answer_id);
CREATE INDEX IF NOT EXISTS idx_posts_creation_date ON posts(creation_date);
CREATE INDEX IF NOT EXISTS idx_posts_parent_id ON posts(parent_id);
CREATE INDEX IF NOT EXISTS idx_posts_score ON posts(score);

CREATE INDEX IF NOT EXISTS idx_posts_tags ON posts USING GIN (to_tsvector('english', tags));

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
    WHERE length(tag) > 0;
END;
$$ LANGUAGE plpgsql;

CREATE MATERIALIZED VIEW IF NOT EXISTS post_tags AS
SELECT
    p.id AS post_id,
    t.tag
FROM
    posts p,
    LATERAL extract_tags(p.tags) t
WHERE
    p.tags IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_post_tags_tag ON post_tags(tag);
CREATE INDEX IF NOT EXISTS idx_post_tags_post_id ON post_tags(post_id);

CREATE INDEX IF NOT EXISTS idx_votes_post_id ON votes(post_id);
CREATE INDEX IF NOT EXISTS idx_votes_vote_type_id ON votes(vote_type_id);

CREATE INDEX IF NOT EXISTS idx_comments_post_id ON comments(post_id);
CREATE INDEX IF NOT EXISTS idx_comments_user_id ON comments(user_id);

CREATE INDEX IF NOT EXISTS idx_badges_user_id ON badges(user_id);
CREATE INDEX IF NOT EXISTS idx_badges_name ON badges(name);

COMMIT;