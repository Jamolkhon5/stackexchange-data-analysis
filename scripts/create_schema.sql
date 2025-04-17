-- Схема базы данных для StackExchange Data Dump

BEGIN;

DROP TABLE IF EXISTS votes CASCADE;
DROP TABLE IF EXISTS tags CASCADE;
DROP TABLE IF EXISTS post_links CASCADE;
DROP TABLE IF EXISTS post_history CASCADE;
DROP TABLE IF EXISTS comments CASCADE;
DROP TABLE IF EXISTS posts CASCADE;
DROP TABLE IF EXISTS badges CASCADE;
DROP TABLE IF EXISTS users CASCADE;
DROP MATERIALIZED VIEW IF EXISTS post_tags CASCADE;
DROP FUNCTION IF EXISTS extract_tags CASCADE;

COMMIT;

BEGIN;

CREATE TABLE IF NOT EXISTS users (
                                     id INTEGER PRIMARY KEY,
                                     reputation INTEGER NOT NULL,
                                     display_name TEXT NOT NULL,
                                     about_me TEXT,
                                     website_url TEXT,
                                     location TEXT,
                                     creation_date TIMESTAMP NOT NULL,
                                     last_access_date TIMESTAMP,
                                     views INTEGER DEFAULT 0,
                                     up_votes INTEGER DEFAULT 0,
                                     down_votes INTEGER DEFAULT 0,
                                     account_id INTEGER
);

CREATE TABLE IF NOT EXISTS badges (
                                      id INTEGER PRIMARY KEY,
                                      user_id INTEGER NOT NULL,
                                      name TEXT NOT NULL,
                                      date TIMESTAMP NOT NULL,
                                      class INTEGER,
                                      tag_based BOOLEAN
);

CREATE TABLE IF NOT EXISTS posts (
                                     id INTEGER PRIMARY KEY,
                                     post_type_id INTEGER NOT NULL,
                                     accepted_answer_id INTEGER,
                                     creation_date TIMESTAMP NOT NULL,
                                     score INTEGER DEFAULT 0,
                                     view_count INTEGER,
                                     body TEXT,
                                     owner_user_id INTEGER,
                                     last_editor_user_id INTEGER,
                                     last_edit_date TIMESTAMP,
                                     last_activity_date TIMESTAMP,
                                     title TEXT,
                                     tags TEXT,
                                     answer_count INTEGER DEFAULT 0,
                                     comment_count INTEGER DEFAULT 0,
                                     favorite_count INTEGER DEFAULT 0,
                                     closed_date TIMESTAMP,
                                     parent_id INTEGER,
                                     community_owned_date TIMESTAMP
);


CREATE TABLE IF NOT EXISTS comments (
                                        id INTEGER PRIMARY KEY,
                                        post_id INTEGER NOT NULL,
                                        user_id INTEGER,
                                        score INTEGER DEFAULT 0,
                                        text TEXT NOT NULL,
                                        creation_date TIMESTAMP NOT NULL
);


CREATE TABLE IF NOT EXISTS post_history (
                                            id INTEGER PRIMARY KEY,
                                            post_id INTEGER NOT NULL,
                                            user_id INTEGER,
                                            post_history_type_id INTEGER NOT NULL,
                                            revision_guid TEXT,
                                            creation_date TIMESTAMP NOT NULL,
                                            text TEXT,
                                            comment TEXT
);


CREATE TABLE IF NOT EXISTS post_links (
                                          id INTEGER PRIMARY KEY,
                                          creation_date TIMESTAMP NOT NULL,
                                          post_id INTEGER NOT NULL,
                                          related_post_id INTEGER NOT NULL,
                                          link_type_id INTEGER NOT NULL
);


CREATE TABLE IF NOT EXISTS tags (
                                    id INTEGER PRIMARY KEY,
                                    tag_name TEXT NOT NULL,
                                    count INTEGER DEFAULT 0,
                                    excerpt_post_id INTEGER,
                                    wiki_post_id INTEGER
);

CREATE TABLE IF NOT EXISTS votes (
                                     id INTEGER PRIMARY KEY,
                                     post_id INTEGER NOT NULL,
                                     vote_type_id INTEGER NOT NULL,
                                     user_id INTEGER,
                                     creation_date TIMESTAMP NOT NULL,
                                     bounty_amount INTEGER
);

-- Создаем функцию для извлечения тегов из строки tags формата '<tag1><tag2><tag3>'
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

COMMIT;