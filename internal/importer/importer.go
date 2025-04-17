package importer

import (
	"database/sql"
	"encoding/xml"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"go.uber.org/zap"
	"stackexchange-data-analysis/internal/config"
)

type Importer struct {
	db          *sqlx.DB
	dataDir     string
	logger      *zap.Logger
	concurrency int
	dbConfig    *config.DatabaseConfig
}

func NewImporter(db *sqlx.DB, cfg *config.Config, logger *zap.Logger) *Importer {
	return &Importer{
		db:          db,
		dataDir:     cfg.DataDir,
		logger:      logger,
		concurrency: cfg.Concurrency,
		dbConfig:    &cfg.Database,
	}
}

func (i *Importer) ImportAll() error {
	var err error

	mainArchive := filepath.Join(i.dataDir, "dba.stackexchange.com.7z")
	mainExtractDir := filepath.Join(i.dataDir, "dba.stackexchange.com")
	metaArchive := filepath.Join(i.dataDir, "dba.meta.stackexchange.com.7z")
	metaExtractDir := filepath.Join(i.dataDir, "dba.meta.stackexchange.com")

	if err = extract7zArchive(mainArchive, mainExtractDir, i.logger); err != nil {
		return err
	}

	if err = extract7zArchive(metaArchive, metaExtractDir, i.logger); err != nil {
		return err
	}

	if err = i.importSite(mainExtractDir); err != nil {
		return err
	}

	if err = i.importSite(metaExtractDir); err != nil {
		return err
	}

	if err = i.refreshMaterializedViews(); err != nil {
		return err
	}

	return nil
}

func (i *Importer) refreshMaterializedViews() error {
	i.logger.Info("обновление материализованных представлений")

	var exists bool
	err := i.db.Get(&exists, `
        SELECT EXISTS (
            SELECT 1 
            FROM pg_matviews 
            WHERE matviewname = 'post_tags'
        )
    `)

	if err != nil {
		return fmt.Errorf("ошибка проверки существования представления: %w", err)
	}

	if exists {
		_, err = i.db.Exec("REFRESH MATERIALIZED VIEW post_tags")
		if err != nil {
			return fmt.Errorf("ошибка обновления материализованного представления: %w", err)
		}
		i.logger.Info("материализованное представление post_tags успешно обновлено")
	} else {
		i.logger.Warn("представление post_tags не найдено, обновление пропущено")
	}

	return nil
}

func (i *Importer) importSite(siteDir string) error {
	i.logger.Info("начало импорта данных сайта", zap.String("dir", siteDir))

	importOrder := []struct {
		entityType string
		importFunc func(string) error
	}{
		{"Users", i.importUsers},
		{"Posts", i.importPosts},
		{"Comments", i.importComments},
		{"Badges", i.importBadges},
		{"PostHistory", i.importPostHistory},
		{"PostLinks", i.importPostLinks},
		{"Tags", i.importTags},
		{"Votes", i.importVotes},
	}

	for _, item := range importOrder {
		xmlFile, err := findXmlFile(siteDir, item.entityType)
		if err != nil {
			i.logger.Warn("файл не найден, пропускаем",
				zap.String("entity", item.entityType),
				zap.Error(err))
			continue
		}

		if err := item.importFunc(xmlFile); err != nil {
			return fmt.Errorf("ошибка импорта %s: %w", item.entityType, err)
		}
	}

	i.logger.Info("импорт данных сайта завершен", zap.String("dir", siteDir))
	return nil
}

func (i *Importer) importUsers(xmlFile string) error {
	i.logger.Info("импорт пользователей", zap.String("file", xmlFile))

	insertStmt, err := i.db.Prepare(`
		INSERT INTO users (
			id, reputation, display_name, about_me, website_url, location,
			creation_date, last_access_date, views, up_votes, down_votes, account_id
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
		ON CONFLICT (id) DO UPDATE SET
			reputation = EXCLUDED.reputation,
			display_name = EXCLUDED.display_name
	`)
	if err != nil {
		return fmt.Errorf("ошибка подготовки запроса: %w", err)
	}
	defer insertStmt.Close()

	rowProcessor := func(start *xml.StartElement) error {
		attrs := startElementToMap(start)

		id, _ := strconv.Atoi(attrs["Id"])
		reputation, _ := strconv.Atoi(attrs["Reputation"])
		views, _ := strconv.Atoi(attrs["Views"])
		upVotes, _ := strconv.Atoi(attrs["UpVotes"])
		downVotes, _ := strconv.Atoi(attrs["DownVotes"])
		accountId, _ := strconv.Atoi(attrs["AccountId"])

		creationDate, _ := parseTime(attrs["CreationDate"])
		lastAccessDate, _ := parseTime(attrs["LastAccessDate"])

		_, err := insertStmt.Exec(
			id, reputation, attrs["DisplayName"], attrs["AboutMe"],
			attrs["WebsiteUrl"], attrs["Location"], creationDate, lastAccessDate,
			views, upVotes, downVotes, accountId,
		)
		return err
	}

	return parseXmlFile(xmlFile, rowProcessor, i.logger)
}

func (i *Importer) importPosts(xmlFile string) error {
	i.logger.Info("импорт постов", zap.String("file", xmlFile))

	_, err := i.db.Exec(`
        ALTER TABLE posts DROP CONSTRAINT IF EXISTS fk_posts_accepted_answer_id;
        ALTER TABLE posts DROP CONSTRAINT IF EXISTS fk_posts_parent_id;
        ALTER TABLE posts DROP CONSTRAINT IF EXISTS fk_posts_owner_user_id;
        ALTER TABLE posts DROP CONSTRAINT IF EXISTS fk_posts_last_editor_user_id;
    `)
	if err != nil {
		return fmt.Errorf("ошибка отключения ограничений внешнего ключа: %w", err)
	}

	insertStmt, err := i.db.Prepare(`
        INSERT INTO posts (
            id, post_type_id, accepted_answer_id, creation_date, score, view_count,
            body, owner_user_id, last_editor_user_id, last_edit_date, last_activity_date,
            title, tags, answer_count, comment_count, favorite_count, closed_date,
            parent_id, community_owned_date
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)
        ON CONFLICT (id) DO UPDATE SET
            score = EXCLUDED.score,
            view_count = EXCLUDED.view_count,
            answer_count = EXCLUDED.answer_count
    `)
	if err != nil {
		return fmt.Errorf("ошибка подготовки запроса: %w", err)
	}
	defer insertStmt.Close()

	rowProcessor := func(start *xml.StartElement) error {
		attrs := startElementToMap(start)

		id, _ := strconv.Atoi(attrs["Id"])
		postTypeId, _ := strconv.Atoi(attrs["PostTypeId"])

		var acceptedAnswerId sql.NullInt64
		if val, ok := attrs["AcceptedAnswerId"]; ok && val != "" {
			id, _ := strconv.ParseInt(val, 10, 64)
			acceptedAnswerId = sql.NullInt64{Valid: true, Int64: id}
		}

		score, _ := strconv.Atoi(attrs["Score"])

		var viewCount sql.NullInt64
		if val, ok := attrs["ViewCount"]; ok && val != "" {
			id, _ := strconv.ParseInt(val, 10, 64)
			viewCount = sql.NullInt64{Valid: true, Int64: id}
		}

		var ownerUserId sql.NullInt64
		if val, ok := attrs["OwnerUserId"]; ok && val != "" {
			id, _ := strconv.ParseInt(val, 10, 64)
			ownerUserId = sql.NullInt64{Valid: true, Int64: id}
		}

		var lastEditorUserId sql.NullInt64
		if val, ok := attrs["LastEditorUserId"]; ok && val != "" {
			id, _ := strconv.ParseInt(val, 10, 64)
			lastEditorUserId = sql.NullInt64{Valid: true, Int64: id}
		}

		answerCount, _ := strconv.Atoi(attrs["AnswerCount"])
		commentCount, _ := strconv.Atoi(attrs["CommentCount"])
		favoriteCount, _ := strconv.Atoi(attrs["FavoriteCount"])

		var parentId sql.NullInt64
		if val, ok := attrs["ParentId"]; ok && val != "" {
			id, _ := strconv.ParseInt(val, 10, 64)
			parentId = sql.NullInt64{Valid: true, Int64: id}
		}

		creationDate, _ := parseTime(attrs["CreationDate"])
		lastEditDate, _ := parseTimeNullable(attrs["LastEditDate"])
		lastActivityDate, _ := parseTimeNullable(attrs["LastActivityDate"])
		closedDate, _ := parseTimeNullable(attrs["ClosedDate"])
		communityOwnedDate, _ := parseTimeNullable(attrs["CommunityOwnedDate"])

		_, err := insertStmt.Exec(
			id, postTypeId, acceptedAnswerId, creationDate, score, viewCount,
			attrs["Body"], ownerUserId, lastEditorUserId, lastEditDate, lastActivityDate,
			attrs["Title"], attrs["Tags"], answerCount, commentCount, favoriteCount,
			closedDate, parentId, communityOwnedDate,
		)
		return err
	}

	err = parseXmlFile(xmlFile, rowProcessor, i.logger)
	if err != nil {
		return err
	}

	i.logger.Info("очистка несогласованных данных в posts")
	_, err = i.db.Exec(`
        -- Очистка несуществующих accepted_answer_id
        UPDATE posts 
        SET accepted_answer_id = NULL 
        WHERE accepted_answer_id IS NOT NULL 
        AND NOT EXISTS (SELECT 1 FROM posts p2 WHERE p2.id = posts.accepted_answer_id);
        
        -- Очистка несуществующих parent_id
        UPDATE posts 
        SET parent_id = NULL 
        WHERE parent_id IS NOT NULL 
        AND NOT EXISTS (SELECT 1 FROM posts p2 WHERE p2.id = posts.parent_id);
        
        -- Очистка несуществующих owner_user_id
        UPDATE posts 
        SET owner_user_id = NULL 
        WHERE owner_user_id IS NOT NULL 
        AND NOT EXISTS (SELECT 1 FROM users u WHERE u.id = posts.owner_user_id);
        
        -- Очистка несуществующих last_editor_user_id
        UPDATE posts 
        SET last_editor_user_id = NULL 
        WHERE last_editor_user_id IS NOT NULL 
        AND NOT EXISTS (SELECT 1 FROM users u WHERE u.id = posts.last_editor_user_id);
    `)
	if err != nil {
		return fmt.Errorf("ошибка очистки несогласованных данных: %w", err)
	}

	// НЕ добавляем ограничения внешнего ключа автоматически
	// Они будут добавлены позже через отдельный скрипт add_constraints.sql
	i.logger.Info("импорт постов завершен успешно")
	return nil
}

func (i *Importer) importComments(xmlFile string) error {
	i.logger.Info("импорт комментариев", zap.String("file", xmlFile))

	insertStmt, err := i.db.Prepare(`
		INSERT INTO comments (id, post_id, user_id, score, text, creation_date)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (id) DO NOTHING
	`)
	if err != nil {
		return fmt.Errorf("ошибка подготовки запроса: %w", err)
	}
	defer insertStmt.Close()

	rowProcessor := func(start *xml.StartElement) error {
		attrs := startElementToMap(start)

		id, _ := strconv.Atoi(attrs["Id"])
		postId, _ := strconv.Atoi(attrs["PostId"])

		var userId sql.NullInt64
		if val, ok := attrs["UserId"]; ok && val != "" {
			id, _ := strconv.ParseInt(val, 10, 64)
			userId = sql.NullInt64{Valid: true, Int64: id}
		}

		score, _ := strconv.Atoi(attrs["Score"])
		creationDate, _ := parseTime(attrs["CreationDate"])

		_, err := insertStmt.Exec(
			id, postId, userId, score, attrs["Text"], creationDate,
		)
		return err
	}

	return parseXmlFile(xmlFile, rowProcessor, i.logger)
}

func (i *Importer) importBadges(xmlFile string) error {
	i.logger.Info("импорт знаков отличия", zap.String("file", xmlFile))

	insertStmt, err := i.db.Prepare(`
		INSERT INTO badges (id, user_id, name, date, class, tag_based)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (id) DO NOTHING
	`)
	if err != nil {
		return fmt.Errorf("ошибка подготовки запроса: %w", err)
	}
	defer insertStmt.Close()

	rowProcessor := func(start *xml.StartElement) error {
		attrs := startElementToMap(start)

		id, _ := strconv.Atoi(attrs["Id"])
		userId, _ := strconv.Atoi(attrs["UserId"])
		class, _ := strconv.Atoi(attrs["Class"])

		var tagBased bool
		if val, ok := attrs["TagBased"]; ok {
			tagBased = strings.ToLower(val) == "true"
		}

		date, _ := parseTime(attrs["Date"])

		_, err := insertStmt.Exec(
			id, userId, attrs["Name"], date, class, tagBased,
		)
		return err
	}

	return parseXmlFile(xmlFile, rowProcessor, i.logger)
}

func (i *Importer) importPostHistory(xmlFile string) error {
	i.logger.Info("импорт истории постов", zap.String("file", xmlFile))

	insertStmt, err := i.db.Prepare(`
		INSERT INTO post_history (
			id, post_id, user_id, post_history_type_id, revision_guid,
			creation_date, text, comment
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (id) DO NOTHING
	`)
	if err != nil {
		return fmt.Errorf("ошибка подготовки запроса: %w", err)
	}
	defer insertStmt.Close()

	rowProcessor := func(start *xml.StartElement) error {
		attrs := startElementToMap(start)

		id, _ := strconv.Atoi(attrs["Id"])
		postId, _ := strconv.Atoi(attrs["PostId"])

		var userId sql.NullInt64
		if val, ok := attrs["UserId"]; ok && val != "" {
			id, _ := strconv.ParseInt(val, 10, 64)
			userId = sql.NullInt64{Valid: true, Int64: id}
		}

		postHistoryTypeId, _ := strconv.Atoi(attrs["PostHistoryTypeId"])
		creationDate, _ := parseTime(attrs["CreationDate"])

		_, err := insertStmt.Exec(
			id, postId, userId, postHistoryTypeId, attrs["RevisionGUID"],
			creationDate, attrs["Text"], attrs["Comment"],
		)
		return err
	}

	return parseXmlFile(xmlFile, rowProcessor, i.logger)
}

func (i *Importer) importPostLinks(xmlFile string) error {
	i.logger.Info("импорт связей между постами", zap.String("file", xmlFile))

	insertStmt, err := i.db.Prepare(`
		INSERT INTO post_links (id, creation_date, post_id, related_post_id, link_type_id)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (id) DO NOTHING
	`)
	if err != nil {
		return fmt.Errorf("ошибка подготовки запроса: %w", err)
	}
	defer insertStmt.Close()

	rowProcessor := func(start *xml.StartElement) error {
		attrs := startElementToMap(start)

		id, _ := strconv.Atoi(attrs["Id"])
		postId, _ := strconv.Atoi(attrs["PostId"])
		relatedPostId, _ := strconv.Atoi(attrs["RelatedPostId"])
		linkTypeId, _ := strconv.Atoi(attrs["LinkTypeId"])
		creationDate, _ := parseTime(attrs["CreationDate"])

		_, err := insertStmt.Exec(
			id, creationDate, postId, relatedPostId, linkTypeId,
		)
		return err
	}

	return parseXmlFile(xmlFile, rowProcessor, i.logger)
}

func (i *Importer) importTags(xmlFile string) error {
	i.logger.Info("импорт тегов", zap.String("file", xmlFile))

	insertStmt, err := i.db.Prepare(`
		INSERT INTO tags (id, tag_name, count, excerpt_post_id, wiki_post_id)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (id) DO NOTHING
	`)
	if err != nil {
		return fmt.Errorf("ошибка подготовки запроса: %w", err)
	}
	defer insertStmt.Close()

	rowProcessor := func(start *xml.StartElement) error {
		attrs := startElementToMap(start)

		id, _ := strconv.Atoi(attrs["Id"])
		count, _ := strconv.Atoi(attrs["Count"])

		var excerptPostId sql.NullInt64
		if val, ok := attrs["ExcerptPostId"]; ok && val != "" {
			id, _ := strconv.ParseInt(val, 10, 64)
			excerptPostId = sql.NullInt64{Valid: true, Int64: id}
		}

		var wikiPostId sql.NullInt64
		if val, ok := attrs["WikiPostId"]; ok && val != "" {
			id, _ := strconv.ParseInt(val, 10, 64)
			wikiPostId = sql.NullInt64{Valid: true, Int64: id}
		}

		_, err := insertStmt.Exec(
			id, attrs["TagName"], count, excerptPostId, wikiPostId,
		)
		return err
	}

	return parseXmlFile(xmlFile, rowProcessor, i.logger)
}

func parseTime(timeStr string) (time.Time, error) {
	if timeStr == "" {
		return time.Time{}, fmt.Errorf("пустая строка времени")
	}
	return time.Parse("2006-01-02T15:04:05.000", timeStr)
}

func parseTimeNullable(timeStr string) (sql.NullTime, error) {
	if timeStr == "" {
		return sql.NullTime{Valid: false}, nil
	}

	t, err := time.Parse("2006-01-02T15:04:05.000", timeStr)
	if err != nil {
		return sql.NullTime{Valid: false}, err
	}

	return sql.NullTime{Valid: true, Time: t}, nil
}

func (i *Importer) importVotes(xmlFile string) error {
	i.logger.Info("импорт голосов", zap.String("file", xmlFile))

	insertStmt, err := i.db.Prepare(`
		INSERT INTO votes (id, post_id, vote_type_id, user_id, creation_date, bounty_amount)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (id) DO NOTHING
	`)
	if err != nil {
		return fmt.Errorf("ошибка подготовки запроса: %w", err)
	}
	defer insertStmt.Close()

	rowProcessor := func(start *xml.StartElement) error {
		attrs := startElementToMap(start)

		id, _ := strconv.Atoi(attrs["Id"])
		postId, _ := strconv.Atoi(attrs["PostId"])
		voteTypeId, _ := strconv.Atoi(attrs["VoteTypeId"])

		var userId sql.NullInt64
		if val, ok := attrs["UserId"]; ok && val != "" {
			id, _ := strconv.ParseInt(val, 10, 64)
			userId = sql.NullInt64{Valid: true, Int64: id}
		}

		var bountyAmount sql.NullInt64
		if val, ok := attrs["BountyAmount"]; ok && val != "" {
			amount, _ := strconv.ParseInt(val, 10, 64)
			bountyAmount = sql.NullInt64{Valid: true, Int64: amount}
		}

		creationDate, _ := parseTime(attrs["CreationDate"])

		_, err := insertStmt.Exec(
			id, postId, voteTypeId, userId, creationDate, bountyAmount,
		)
		return err
	}

	return parseXmlFile(xmlFile, rowProcessor, i.logger)
}
