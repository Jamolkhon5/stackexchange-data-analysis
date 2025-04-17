-- Скрипт для добавления ограничений внешнего ключа после импорта данных
BEGIN;

-- Badges → Users
ALTER TABLE badges ADD CONSTRAINT fk_badges_user_id
    FOREIGN KEY (user_id) REFERENCES users(id);

-- Posts → Users (owner)
ALTER TABLE posts ADD CONSTRAINT fk_posts_owner_user_id
    FOREIGN KEY (owner_user_id) REFERENCES users(id);

-- Posts → Users (editor)
ALTER TABLE posts ADD CONSTRAINT fk_posts_last_editor_user_id
    FOREIGN KEY (last_editor_user_id) REFERENCES users(id);

-- Posts → Posts (циклические ссылки)
ALTER TABLE posts ADD CONSTRAINT fk_posts_accepted_answer_id
    FOREIGN KEY (accepted_answer_id) REFERENCES posts(id);

ALTER TABLE posts ADD CONSTRAINT fk_posts_parent_id
    FOREIGN KEY (parent_id) REFERENCES posts(id);

-- Comments → Posts
ALTER TABLE comments ADD CONSTRAINT fk_comments_post_id
    FOREIGN KEY (post_id) REFERENCES posts(id);

-- Comments → Users
ALTER TABLE comments ADD CONSTRAINT fk_comments_user_id
    FOREIGN KEY (user_id) REFERENCES users(id);

-- PostHistory → Posts
ALTER TABLE post_history ADD CONSTRAINT fk_post_history_post_id
    FOREIGN KEY (post_id) REFERENCES posts(id);

-- PostHistory → Users
ALTER TABLE post_history ADD CONSTRAINT fk_post_history_user_id
    FOREIGN KEY (user_id) REFERENCES users(id);

-- PostLinks → Posts
ALTER TABLE post_links ADD CONSTRAINT fk_post_links_post_id
    FOREIGN KEY (post_id) REFERENCES posts(id);

ALTER TABLE post_links ADD CONSTRAINT fk_post_links_related_post_id
    FOREIGN KEY (related_post_id) REFERENCES posts(id);

-- Votes → Posts
ALTER TABLE votes ADD CONSTRAINT fk_votes_post_id
    FOREIGN KEY (post_id) REFERENCES posts(id);

-- Votes → Users
ALTER TABLE votes ADD CONSTRAINT fk_votes_user_id
    FOREIGN KEY (user_id) REFERENCES users(id);

COMMIT;