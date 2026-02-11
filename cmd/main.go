package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/joho/godotenv"
	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"

	"github.com/quckapp/thread-service/internal/api"
	"github.com/quckapp/thread-service/internal/repository"
	"github.com/quckapp/thread-service/internal/service"
)

func main() {
	log := logrus.New()
	log.SetFormatter(&logrus.JSONFormatter{})
	_ = godotenv.Load()

	db, err := sqlx.Connect("mysql", getEnv("DATABASE_URL", "root:password@tcp(localhost:3306)/quckapp_threads?parseTime=true"))
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	// Run migrations
	runMigrations(db, log)

	// Initialize Kafka writer
	kafkaWriter := &kafka.Writer{
		Addr:         kafka.TCP(getEnv("KAFKA_BROKERS", "localhost:9092")),
		Topic:        getEnv("KAFKA_TOPIC", "thread-events"),
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 10 * time.Millisecond,
	}
	defer kafkaWriter.Close()

	// Initialize repositories
	threadRepo := repository.NewThreadRepository(db)
	replyRepo := repository.NewReplyRepository(db)
	participantRepo := repository.NewParticipantRepository(db)
	labelRepo := repository.NewLabelRepository(db)
	bookmarkRepo := repository.NewBookmarkRepository(db)
	reactionRepo := repository.NewReactionRepository(db)
	pollRepo := repository.NewPollRepository(db)
	draftRepo := repository.NewDraftRepository(db)
	mentionRepo := repository.NewMentionRepository(db)
	assignmentRepo := repository.NewAssignmentRepository(db)
	activityRepo := repository.NewActivityRepository(db)
	attachmentRepo := repository.NewAttachmentRepository(db)
	templateRepo := repository.NewTemplateRepository(db)
	followUpRepo := repository.NewFollowUpRepository(db)
	summaryRepo := repository.NewSummaryRepository(db)
	linkRepo := repository.NewLinkRepository(db)

	// Initialize service
	svc := service.NewThreadService(
		threadRepo, replyRepo, participantRepo,
		labelRepo, bookmarkRepo, reactionRepo,
		pollRepo, draftRepo, mentionRepo,
		assignmentRepo, activityRepo, attachmentRepo,
		templateRepo, followUpRepo, summaryRepo,
		linkRepo, kafkaWriter, log,
	)

	// Initialize handler
	handler := api.NewHandler(svc)

	// Setup router
	r := api.SetupRouter(db, handler, log)
	api.RegisterExtendedRoutes(r, db)

	srv := &http.Server{Addr: ":" + getEnv("PORT", "5009"), Handler: r}
	go func() {
		log.Infof("Thread service starting on port %s", getEnv("PORT", "5009"))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server error: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Info("Shutting down thread service...")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Errorf("Server shutdown error: %v", err)
	}
	log.Info("Thread service stopped")
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func runMigrations(db *sqlx.DB, log *logrus.Logger) {
	tables := []string{
		`CREATE TABLE IF NOT EXISTS threads (
			id VARCHAR(36) PRIMARY KEY,
			channel_id VARCHAR(36) NOT NULL,
			parent_message_id VARCHAR(36) NOT NULL,
			created_by VARCHAR(36) NOT NULL,
			title VARCHAR(500) DEFAULT '',
			reply_count INT DEFAULT 0,
			participant_count INT DEFAULT 0,
			last_reply_at DATETIME NULL,
			last_reply_by VARCHAR(36) NULL,
			is_resolved BOOLEAN DEFAULT FALSE,
			is_pinned BOOLEAN DEFAULT FALSE,
			is_locked BOOLEAN DEFAULT FALSE,
			is_archived BOOLEAN DEFAULT FALSE,
			priority VARCHAR(20) DEFAULT '',
			created_at DATETIME NOT NULL,
			updated_at DATETIME NOT NULL,
			INDEX idx_channel (channel_id),
			INDEX idx_message (parent_message_id),
			INDEX idx_created_by (created_by),
			INDEX idx_resolved (is_resolved)
		)`,
		`CREATE TABLE IF NOT EXISTS thread_replies (
			id VARCHAR(36) PRIMARY KEY,
			thread_id VARCHAR(36) NOT NULL,
			message_id VARCHAR(36) NOT NULL,
			user_id VARCHAR(36) NOT NULL,
			content TEXT DEFAULT '',
			created_at DATETIME NOT NULL,
			updated_at DATETIME NOT NULL,
			INDEX idx_thread (thread_id),
			INDEX idx_user (user_id)
		)`,
		`CREATE TABLE IF NOT EXISTS thread_participants (
			id VARCHAR(36) PRIMARY KEY,
			thread_id VARCHAR(36) NOT NULL,
			user_id VARCHAR(36) NOT NULL,
			last_read_at DATETIME NULL,
			is_subscribed BOOLEAN DEFAULT TRUE,
			joined_at DATETIME NOT NULL,
			INDEX idx_thread_user (thread_id, user_id),
			INDEX idx_user (user_id)
		)`,
		`CREATE TABLE IF NOT EXISTS thread_labels (
			id VARCHAR(36) PRIMARY KEY,
			name VARCHAR(100) NOT NULL,
			color VARCHAR(20) NOT NULL,
			channel_id VARCHAR(36) NOT NULL,
			created_by VARCHAR(36) NOT NULL,
			created_at DATETIME NOT NULL,
			INDEX idx_channel (channel_id)
		)`,
		`CREATE TABLE IF NOT EXISTS thread_label_assignments (
			id VARCHAR(36) PRIMARY KEY,
			thread_id VARCHAR(36) NOT NULL,
			label_id VARCHAR(36) NOT NULL,
			assigned_by VARCHAR(36) NOT NULL,
			assigned_at DATETIME NOT NULL,
			INDEX idx_thread (thread_id),
			INDEX idx_label (label_id),
			UNIQUE INDEX idx_thread_label (thread_id, label_id)
		)`,
		`CREATE TABLE IF NOT EXISTS thread_bookmarks (
			id VARCHAR(36) PRIMARY KEY,
			thread_id VARCHAR(36) NOT NULL,
			user_id VARCHAR(36) NOT NULL,
			note TEXT DEFAULT '',
			created_at DATETIME NOT NULL,
			UNIQUE INDEX idx_thread_user (thread_id, user_id),
			INDEX idx_user (user_id)
		)`,
		`CREATE TABLE IF NOT EXISTS thread_reactions (
			id VARCHAR(36) PRIMARY KEY,
			thread_id VARCHAR(36) NOT NULL,
			reply_id VARCHAR(36) DEFAULT '',
			user_id VARCHAR(36) NOT NULL,
			emoji VARCHAR(50) NOT NULL,
			created_at DATETIME NOT NULL,
			INDEX idx_thread (thread_id),
			INDEX idx_reply (reply_id)
		)`,
		`CREATE TABLE IF NOT EXISTS thread_polls (
			id VARCHAR(36) PRIMARY KEY,
			thread_id VARCHAR(36) NOT NULL,
			question VARCHAR(500) NOT NULL,
			created_by VARCHAR(36) NOT NULL,
			multi_vote BOOLEAN DEFAULT FALSE,
			anonymous BOOLEAN DEFAULT FALSE,
			expires_at DATETIME NULL,
			is_closed BOOLEAN DEFAULT FALSE,
			created_at DATETIME NOT NULL,
			INDEX idx_thread (thread_id)
		)`,
		`CREATE TABLE IF NOT EXISTS poll_options (
			id VARCHAR(36) PRIMARY KEY,
			poll_id VARCHAR(36) NOT NULL,
			text VARCHAR(500) NOT NULL,
			position INT DEFAULT 0,
			INDEX idx_poll (poll_id)
		)`,
		`CREATE TABLE IF NOT EXISTS poll_votes (
			id VARCHAR(36) PRIMARY KEY,
			poll_id VARCHAR(36) NOT NULL,
			option_id VARCHAR(36) NOT NULL,
			user_id VARCHAR(36) NOT NULL,
			voted_at DATETIME NOT NULL,
			INDEX idx_poll (poll_id),
			INDEX idx_option (option_id),
			INDEX idx_user (user_id)
		)`,
		`CREATE TABLE IF NOT EXISTS thread_drafts (
			id VARCHAR(36) PRIMARY KEY,
			thread_id VARCHAR(36) NOT NULL,
			user_id VARCHAR(36) NOT NULL,
			content TEXT NOT NULL,
			updated_at DATETIME NOT NULL,
			UNIQUE INDEX idx_thread_user (thread_id, user_id)
		)`,
		`CREATE TABLE IF NOT EXISTS thread_mentions (
			id VARCHAR(36) PRIMARY KEY,
			thread_id VARCHAR(36) NOT NULL,
			reply_id VARCHAR(36) DEFAULT '',
			mentioned_user VARCHAR(36) NOT NULL,
			mentioned_by VARCHAR(36) NOT NULL,
			is_read BOOLEAN DEFAULT FALSE,
			created_at DATETIME NOT NULL,
			INDEX idx_mentioned (mentioned_user),
			INDEX idx_unread (mentioned_user, is_read)
		)`,
		`CREATE TABLE IF NOT EXISTS thread_assignments (
			id VARCHAR(36) PRIMARY KEY,
			thread_id VARCHAR(36) NOT NULL,
			assignee_id VARCHAR(36) NOT NULL,
			assigned_by VARCHAR(36) NOT NULL,
			status VARCHAR(20) DEFAULT 'open',
			due_date DATETIME NULL,
			created_at DATETIME NOT NULL,
			updated_at DATETIME NOT NULL,
			INDEX idx_thread (thread_id),
			INDEX idx_assignee (assignee_id)
		)`,
		`CREATE TABLE IF NOT EXISTS thread_activity_log (
			id VARCHAR(36) PRIMARY KEY,
			thread_id VARCHAR(36) NOT NULL,
			user_id VARCHAR(36) NOT NULL,
			action VARCHAR(100) NOT NULL,
			details TEXT DEFAULT '',
			created_at DATETIME NOT NULL,
			INDEX idx_thread (thread_id)
		)`,
		`CREATE TABLE IF NOT EXISTS thread_attachments (
			id VARCHAR(36) PRIMARY KEY,
			thread_id VARCHAR(36) NOT NULL,
			reply_id VARCHAR(36) DEFAULT '',
			user_id VARCHAR(36) NOT NULL,
			file_name VARCHAR(500) NOT NULL,
			file_size BIGINT DEFAULT 0,
			mime_type VARCHAR(100) DEFAULT '',
			url VARCHAR(2000) NOT NULL,
			created_at DATETIME NOT NULL,
			INDEX idx_thread (thread_id)
		)`,
		`CREATE TABLE IF NOT EXISTS thread_templates (
			id VARCHAR(36) PRIMARY KEY,
			channel_id VARCHAR(36) NOT NULL,
			name VARCHAR(200) NOT NULL,
			description TEXT DEFAULT '',
			content TEXT DEFAULT '',
			labels TEXT DEFAULT '',
			priority VARCHAR(20) DEFAULT '',
			created_by VARCHAR(36) NOT NULL,
			created_at DATETIME NOT NULL,
			updated_at DATETIME NOT NULL,
			INDEX idx_channel (channel_id)
		)`,
		`CREATE TABLE IF NOT EXISTS thread_follow_ups (
			id VARCHAR(36) PRIMARY KEY,
			thread_id VARCHAR(36) NOT NULL,
			user_id VARCHAR(36) NOT NULL,
			remind_at DATETIME NOT NULL,
			note TEXT DEFAULT '',
			is_triggered BOOLEAN DEFAULT FALSE,
			created_at DATETIME NOT NULL,
			INDEX idx_user (user_id),
			INDEX idx_remind (remind_at, is_triggered)
		)`,
		`CREATE TABLE IF NOT EXISTS thread_summaries (
			id VARCHAR(36) PRIMARY KEY,
			thread_id VARCHAR(36) NOT NULL,
			summary TEXT NOT NULL,
			created_by VARCHAR(36) NOT NULL,
			created_at DATETIME NOT NULL,
			INDEX idx_thread (thread_id)
		)`,
		`CREATE TABLE IF NOT EXISTS thread_links (
			id VARCHAR(36) PRIMARY KEY,
			source_thread VARCHAR(36) NOT NULL,
			target_thread VARCHAR(36) NOT NULL,
			link_type VARCHAR(50) DEFAULT 'related',
			created_by VARCHAR(36) NOT NULL,
			created_at DATETIME NOT NULL,
			INDEX idx_source (source_thread),
			INDEX idx_target (target_thread)
		)`,
		`CREATE TABLE IF NOT EXISTS thread_permissions (
			id VARCHAR(36) PRIMARY KEY,
			thread_id VARCHAR(36) NOT NULL,
			user_id VARCHAR(36) NOT NULL,
			can_reply BOOLEAN DEFAULT TRUE,
			can_edit BOOLEAN DEFAULT FALSE,
			can_delete BOOLEAN DEFAULT FALSE,
			can_pin BOOLEAN DEFAULT FALSE,
			can_lock BOOLEAN DEFAULT FALSE,
			can_invite BOOLEAN DEFAULT FALSE,
			granted_by VARCHAR(36) NOT NULL,
			created_at DATETIME NOT NULL,
			updated_at DATETIME NOT NULL,
			UNIQUE KEY uq_thread_user (thread_id, user_id),
			INDEX idx_thread (thread_id)
		)`,
		`CREATE TABLE IF NOT EXISTS thread_watchers (
			id VARCHAR(36) PRIMARY KEY,
			thread_id VARCHAR(36) NOT NULL,
			user_id VARCHAR(36) NOT NULL,
			notify_on VARCHAR(20) DEFAULT 'all',
			created_at DATETIME NOT NULL,
			UNIQUE KEY uq_thread_user (thread_id, user_id),
			INDEX idx_user (user_id)
		)`,
		`CREATE TABLE IF NOT EXISTS read_receipts (
			id VARCHAR(36) PRIMARY KEY,
			thread_id VARCHAR(36) NOT NULL,
			user_id VARCHAR(36) NOT NULL,
			reply_id VARCHAR(36) DEFAULT '',
			read_at DATETIME NOT NULL,
			UNIQUE KEY uq_thread_user (thread_id, user_id),
			INDEX idx_thread (thread_id)
		)`,
		`CREATE TABLE IF NOT EXISTS scheduled_messages (
			id VARCHAR(36) PRIMARY KEY,
			thread_id VARCHAR(36) NOT NULL,
			user_id VARCHAR(36) NOT NULL,
			content TEXT NOT NULL,
			scheduled_at DATETIME NOT NULL,
			sent_at DATETIME,
			status VARCHAR(20) DEFAULT 'pending',
			created_at DATETIME NOT NULL,
			INDEX idx_thread (thread_id),
			INDEX idx_user (user_id),
			INDEX idx_scheduled (scheduled_at, status)
		)`,
		`CREATE TABLE IF NOT EXISTS saved_replies (
			id VARCHAR(36) PRIMARY KEY,
			user_id VARCHAR(36) NOT NULL,
			title VARCHAR(255) NOT NULL,
			content TEXT NOT NULL,
			shortcut VARCHAR(50) DEFAULT '',
			use_count INT DEFAULT 0,
			created_at DATETIME NOT NULL,
			updated_at DATETIME NOT NULL,
			INDEX idx_user (user_id)
		)`,
		`CREATE TABLE IF NOT EXISTS thread_tags (
			id VARCHAR(36) PRIMARY KEY,
			thread_id VARCHAR(36) NOT NULL,
			tag VARCHAR(100) NOT NULL,
			added_by VARCHAR(36) NOT NULL,
			created_at DATETIME NOT NULL,
			UNIQUE KEY uq_thread_tag (thread_id, tag),
			INDEX idx_tag (tag)
		)`,
		`CREATE TABLE IF NOT EXISTS thread_favorites (
			id VARCHAR(36) PRIMARY KEY,
			thread_id VARCHAR(36) NOT NULL,
			user_id VARCHAR(36) NOT NULL,
			created_at DATETIME NOT NULL,
			UNIQUE KEY uq_thread_user (thread_id, user_id),
			INDEX idx_user (user_id)
		)`,
		`CREATE TABLE IF NOT EXISTS thread_pins (
			id VARCHAR(36) PRIMARY KEY,
			thread_id VARCHAR(36) NOT NULL,
			channel_id VARCHAR(36) NOT NULL,
			pinned_by VARCHAR(36) NOT NULL,
			pinned_at DATETIME NOT NULL,
			UNIQUE KEY uq_thread (thread_id),
			INDEX idx_channel (channel_id)
		)`,
		`CREATE TABLE IF NOT EXISTS notification_preferences (
			id VARCHAR(36) PRIMARY KEY,
			thread_id VARCHAR(36) NOT NULL,
			user_id VARCHAR(36) NOT NULL,
			muted BOOLEAN DEFAULT FALSE,
			mute_until DATETIME,
			desktop BOOLEAN DEFAULT TRUE,
			mobile BOOLEAN DEFAULT TRUE,
			email BOOLEAN DEFAULT FALSE,
			created_at DATETIME NOT NULL,
			UNIQUE KEY uq_thread_user (thread_id, user_id),
			INDEX idx_user (user_id)
		)`,
	}

	for _, stmt := range tables {
		if _, err := db.Exec(stmt); err != nil {
			log.Warnf("Migration warning: %v", err)
		}
	}
	log.Info("Database migrations completed")
}
