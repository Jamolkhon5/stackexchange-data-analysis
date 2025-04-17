-- Q2 - "Успешные шутники"
-- Найти ответы с самыми низкими оценками, которые были приняты как лучший ответ

EXPLAIN ANALYZE
SELECT
    a.id AS answer_id,
    q.id AS question_id,
    q.title AS question_title,
    a.score AS answer_score,
    a.creation_date AS answer_date,
    u.id AS user_id,
    u.display_name AS user_name,
    u.reputation AS user_reputation
FROM
    posts q
        JOIN
    posts a ON q.accepted_answer_id = a.id
        JOIN
    users u ON a.owner_user_id = u.id
WHERE
    q.post_type_id = 1
  AND a.post_type_id = 2
  AND a.score <= 5
  AND q.accepted_answer_id IS NOT NULL
ORDER BY
    a.score ASC,
    q.creation_date DESC
    LIMIT 20;