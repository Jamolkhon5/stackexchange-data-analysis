-- Q1 - "Репутационные пары"
-- Запрос анализирует какие теги задаются одновременно, как быстро на них отвечают,
-- и как это связано с репутацией пользователей

EXPLAIN ANALYZE
WITH question_answer_pairs AS (
    SELECT
        q.id AS question_id,
        q.creation_date AS question_date,
        a.id AS answer_id,
        a.creation_date AS answer_date,
        a.owner_user_id AS answerer_id,
        q.tags AS question_tags,
        EXTRACT(EPOCH FROM (a.creation_date - q.creation_date)) / 60.0 AS response_time_minutes
    FROM
        posts q
    JOIN
        posts a ON a.parent_id = q.id
    WHERE
        q.post_type_id = 1
        AND a.post_type_id = 2
        AND q.tags IS NOT NULL
),
tag_pairs AS (
    SELECT
        qap.question_id,
        t1.tag AS tag1,
        t2.tag AS tag2,
        qap.response_time_minutes,
        qap.answerer_id
    FROM
        question_answer_pairs qap
    CROSS JOIN LATERAL
        extract_tags(qap.question_tags) t1
    CROSS JOIN LATERAL
        extract_tags(qap.question_tags) t2
    WHERE
        t1.tag < t2.tag
)
SELECT
    tp.tag1,
    tp.tag2,
    COUNT(*) AS pair_count,
    AVG(tp.response_time_minutes) AS avg_response_time_minutes,
    AVG(u.reputation) AS avg_answerer_reputation,
    STDDEV(tp.response_time_minutes) AS stddev_response_time,
    CORR(tp.response_time_minutes, u.reputation) AS correlation_time_reputation
FROM
    tag_pairs tp
        JOIN
    users u ON tp.answerer_id = u.id
GROUP BY
    tp.tag1, tp.tag2
HAVING
    COUNT(*) >= 2
ORDER BY
    pair_count DESC,
    avg_response_time_minutes ASC
    LIMIT 20;