package main

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	_ "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/joho/godotenv"
	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
)

// Thread represents a conversation thread
type Thread struct {
	ID               string     `json:"id" db:"id"`
	ChannelID        string     `json:"channel_id" db:"channel_id"`
	ParentMessageID  string     `json:"parent_message_id" db:"parent_message_id"`
	CreatedBy        string     `json:"created_by" db:"created_by"`
	ReplyCount       int        `json:"reply_count" db:"reply_count"`
	ParticipantCount int        `json:"participant_count" db:"participant_count"`
	LastReplyAt      *time.Time `json:"last_reply_at" db:"last_reply_at"`
	LastReplyBy      *string    `json:"last_reply_by" db:"last_reply_by"`
	IsResolved       bool       `json:"is_resolved" db:"is_resolved"`
	CreatedAt        time.Time  `json:"created_at" db:"created_at"`
	UpdatedAt        time.Time  `json:"updated_at" db:"updated_at"`
}

// ThreadReply represents a reply in a thread
type ThreadReply struct {
	ID        string    `json:"id" db:"id"`
	ThreadID  string    `json:"thread_id" db:"thread_id"`
	MessageID string    `json:"message_id" db:"message_id"`
	UserID    string    `json:"user_id" db:"user_id"`
	CreatedAt time.Time `json:"created_at" db:"created_at"`
}

// ThreadParticipant represents a user participating in a thread
type ThreadParticipant struct {
	ID           string     `json:"id" db:"id"`
	ThreadID     string     `json:"thread_id" db:"thread_id"`
	UserID       string     `json:"user_id" db:"user_id"`
	LastReadAt   *time.Time `json:"last_read_at" db:"last_read_at"`
	IsSubscribed bool       `json:"is_subscribed" db:"is_subscribed"`
	JoinedAt     time.Time  `json:"joined_at" db:"joined_at"`
}

// ThreadEvent for Kafka publishing
type ThreadEvent struct {
	Type      string      `json:"type"`
	ThreadID  string      `json:"thread_id"`
	ChannelID string      `json:"channel_id"`
	UserID    string      `json:"user_id"`
	Data      interface{} `json:"data"`
	Timestamp time.Time   `json:"timestamp"`
}

var (
	db          *sqlx.DB
	log         *logrus.Logger
	kafkaWriter *kafka.Writer
)

func main() {
	log = logrus.New()
	log.SetFormatter(&logrus.JSONFormatter{})
	_ = godotenv.Load()

	var err error
	db, err = sqlx.Connect("mysql", getEnv("DATABASE_URL", "root:password@tcp(localhost:3306)/quckchat_threads?parseTime=true"))
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	// Configure connection pool
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	// Initialize Kafka writer
	kafkaWriter = &kafka.Writer{
		Addr:         kafka.TCP(getEnv("KAFKA_BROKERS", "localhost:9092")),
		Topic:        getEnv("KAFKA_TOPIC", "thread-events"),
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 10 * time.Millisecond,
	}
	defer kafkaWriter.Close()

	r := gin.New()
	r.Use(gin.Recovery())
	r.Use(requestLogger())

	// Health checks
	r.GET("/health", healthCheck)
	r.GET("/ready", readinessCheck)

	api := r.Group("/api/v1")
	{
		// Thread management
		threads := api.Group("/threads")
		{
			threads.POST("", createThread)
			threads.GET("", listThreads)
			threads.GET("/:id", getThread)
			threads.PUT("/:id", updateThread)
			threads.DELETE("/:id", deleteThread)
			threads.GET("/message/:messageId", getThreadByMessage)
			threads.GET("/channel/:channelId", getChannelThreads)

			// Replies
			threads.GET("/:id/replies", getThreadReplies)
			threads.POST("/:id/replies", addReply)
			threads.DELETE("/:id/replies/:replyId", deleteReply)

			// Participants
			threads.GET("/:id/participants", getParticipants)
			threads.POST("/:id/participants", addParticipant)
			threads.DELETE("/:id/participants/:userId", removeParticipant)
			threads.PUT("/:id/participants/:userId/subscribe", updateSubscription)
			threads.POST("/:id/participants/:userId/read", markAsRead)
		}

		// User's threads
		api.GET("/users/:userId/threads", getUserThreads)

		// Stats
		api.GET("/stats", getStats)
	}

	srv := &http.Server{Addr: ":" + getEnv("PORT", "3005"), Handler: r}
	go func() {
		log.Infof("Thread service starting on port %s", getEnv("PORT", "3005"))
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

func requestLogger() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()
		log.WithFields(logrus.Fields{
			"method":   c.Request.Method,
			"path":     c.Request.URL.Path,
			"status":   c.Writer.Status(),
			"duration": time.Since(start).Milliseconds(),
		}).Info("request")
	}
}

// Health checks
func healthCheck(c *gin.Context) {
	c.JSON(200, gin.H{
		"status":  "healthy",
		"service": "thread-service",
		"time":    time.Now().UTC(),
	})
}

func readinessCheck(c *gin.Context) {
	if err := db.Ping(); err != nil {
		c.JSON(503, gin.H{"status": "not ready", "error": "database unavailable"})
		return
	}
	c.JSON(200, gin.H{"status": "ready"})
}

// Thread handlers
func createThread(c *gin.Context) {
	var req struct {
		ChannelID       string `json:"channel_id" binding:"required"`
		ParentMessageID string `json:"parent_message_id" binding:"required"`
		CreatedBy       string `json:"created_by" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}

	// Check if thread already exists for this message
	var existingID string
	err := db.Get(&existingID, `SELECT id FROM threads WHERE parent_message_id = ?`, req.ParentMessageID)
	if err == nil {
		c.JSON(409, gin.H{"error": "thread already exists for this message", "thread_id": existingID})
		return
	}

	threadID := uuid.New().String()
	now := time.Now()

	tx, err := db.Beginx()
	if err != nil {
		c.JSON(500, gin.H{"error": "failed to start transaction"})
		return
	}
	defer tx.Rollback()

	// Create thread
	_, err = tx.Exec(`INSERT INTO threads (id, channel_id, parent_message_id, created_by, reply_count, participant_count, is_resolved, created_at, updated_at)
		VALUES (?, ?, ?, ?, 0, 1, FALSE, ?, ?)`,
		threadID, req.ChannelID, req.ParentMessageID, req.CreatedBy, now, now)
	if err != nil {
		log.Errorf("Failed to create thread: %v", err)
		c.JSON(500, gin.H{"error": "failed to create thread"})
		return
	}

	// Add creator as first participant
	participantID := uuid.New().String()
	_, err = tx.Exec(`INSERT INTO thread_participants (id, thread_id, user_id, is_subscribed, joined_at)
		VALUES (?, ?, ?, TRUE, ?)`,
		participantID, threadID, req.CreatedBy, now)
	if err != nil {
		log.Errorf("Failed to add creator as participant: %v", err)
		c.JSON(500, gin.H{"error": "failed to create thread"})
		return
	}

	if err := tx.Commit(); err != nil {
		c.JSON(500, gin.H{"error": "failed to commit transaction"})
		return
	}

	// Publish event
	publishEvent(ThreadEvent{
		Type:      "thread.created",
		ThreadID:  threadID,
		ChannelID: req.ChannelID,
		UserID:    req.CreatedBy,
		Data: map[string]interface{}{
			"parent_message_id": req.ParentMessageID,
		},
		Timestamp: now,
	})

	log.WithFields(logrus.Fields{"thread_id": threadID, "channel_id": req.ChannelID}).Info("Thread created")

	thread := Thread{
		ID:               threadID,
		ChannelID:        req.ChannelID,
		ParentMessageID:  req.ParentMessageID,
		CreatedBy:        req.CreatedBy,
		ReplyCount:       0,
		ParticipantCount: 1,
		IsResolved:       false,
		CreatedAt:        now,
		UpdatedAt:        now,
	}
	c.JSON(201, thread)
}

func getThread(c *gin.Context) {
	id := c.Param("id")

	var thread Thread
	err := db.Get(&thread, `SELECT * FROM threads WHERE id = ?`, id)
	if err != nil {
		c.JSON(404, gin.H{"error": "thread not found"})
		return
	}
	c.JSON(200, thread)
}

func getThreadByMessage(c *gin.Context) {
	messageID := c.Param("messageId")

	var thread Thread
	err := db.Get(&thread, `SELECT * FROM threads WHERE parent_message_id = ?`, messageID)
	if err != nil {
		c.JSON(404, gin.H{"error": "thread not found"})
		return
	}
	c.JSON(200, thread)
}

func listThreads(c *gin.Context) {
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "50"))
	offset, _ := strconv.Atoi(c.DefaultQuery("offset", "0"))

	if limit > 100 {
		limit = 100
	}

	var threads []Thread
	err := db.Select(&threads, `SELECT * FROM threads ORDER BY updated_at DESC LIMIT ? OFFSET ?`, limit, offset)
	if err != nil {
		c.JSON(500, gin.H{"error": "failed to fetch threads"})
		return
	}

	var total int
	db.Get(&total, `SELECT COUNT(*) FROM threads`)

	c.JSON(200, gin.H{
		"threads": threads,
		"total":   total,
		"limit":   limit,
		"offset":  offset,
	})
}

func getChannelThreads(c *gin.Context) {
	channelID := c.Param("channelId")
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "50"))
	offset, _ := strconv.Atoi(c.DefaultQuery("offset", "0"))
	includeResolved := c.DefaultQuery("include_resolved", "true") == "true"

	if limit > 100 {
		limit = 100
	}

	query := `SELECT * FROM threads WHERE channel_id = ?`
	countQuery := `SELECT COUNT(*) FROM threads WHERE channel_id = ?`
	args := []interface{}{channelID}

	if !includeResolved {
		query += ` AND is_resolved = FALSE`
		countQuery += ` AND is_resolved = FALSE`
	}

	query += ` ORDER BY COALESCE(last_reply_at, created_at) DESC LIMIT ? OFFSET ?`
	args = append(args, limit, offset)

	var threads []Thread
	err := db.Select(&threads, query, args...)
	if err != nil {
		c.JSON(500, gin.H{"error": "failed to fetch threads"})
		return
	}

	var total int
	if includeResolved {
		db.Get(&total, countQuery, channelID)
	} else {
		db.Get(&total, countQuery, channelID)
	}

	c.JSON(200, gin.H{
		"threads": threads,
		"total":   total,
		"limit":   limit,
		"offset":  offset,
	})
}

func getUserThreads(c *gin.Context) {
	userID := c.Param("userId")
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "50"))
	offset, _ := strconv.Atoi(c.DefaultQuery("offset", "0"))
	subscribedOnly := c.DefaultQuery("subscribed_only", "false") == "true"

	if limit > 100 {
		limit = 100
	}

	query := `SELECT t.* FROM threads t
		INNER JOIN thread_participants tp ON t.id = tp.thread_id
		WHERE tp.user_id = ?`
	args := []interface{}{userID}

	if subscribedOnly {
		query += ` AND tp.is_subscribed = TRUE`
	}

	query += ` ORDER BY COALESCE(t.last_reply_at, t.created_at) DESC LIMIT ? OFFSET ?`
	args = append(args, limit, offset)

	var threads []Thread
	err := db.Select(&threads, query, args...)
	if err != nil {
		c.JSON(500, gin.H{"error": "failed to fetch threads"})
		return
	}

	c.JSON(200, gin.H{
		"threads": threads,
		"limit":   limit,
		"offset":  offset,
	})
}

func updateThread(c *gin.Context) {
	id := c.Param("id")

	var thread Thread
	if err := db.Get(&thread, `SELECT * FROM threads WHERE id = ?`, id); err != nil {
		c.JSON(404, gin.H{"error": "thread not found"})
		return
	}

	var req struct {
		IsResolved *bool `json:"is_resolved"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}

	if req.IsResolved != nil {
		_, err := db.Exec(`UPDATE threads SET is_resolved = ?, updated_at = NOW() WHERE id = ?`, *req.IsResolved, id)
		if err != nil {
			c.JSON(500, gin.H{"error": "failed to update thread"})
			return
		}

		eventType := "thread.resolved"
		if !*req.IsResolved {
			eventType = "thread.reopened"
		}

		publishEvent(ThreadEvent{
			Type:      eventType,
			ThreadID:  id,
			ChannelID: thread.ChannelID,
			Data: map[string]interface{}{
				"is_resolved": *req.IsResolved,
			},
			Timestamp: time.Now(),
		})
	}

	// Fetch updated thread
	db.Get(&thread, `SELECT * FROM threads WHERE id = ?`, id)
	c.JSON(200, thread)
}

func deleteThread(c *gin.Context) {
	id := c.Param("id")

	var thread Thread
	if err := db.Get(&thread, `SELECT * FROM threads WHERE id = ?`, id); err != nil {
		c.JSON(404, gin.H{"error": "thread not found"})
		return
	}

	result, err := db.Exec(`DELETE FROM threads WHERE id = ?`, id)
	if err != nil {
		c.JSON(500, gin.H{"error": "failed to delete thread"})
		return
	}

	rows, _ := result.RowsAffected()
	if rows == 0 {
		c.JSON(404, gin.H{"error": "thread not found"})
		return
	}

	publishEvent(ThreadEvent{
		Type:      "thread.deleted",
		ThreadID:  id,
		ChannelID: thread.ChannelID,
		Timestamp: time.Now(),
	})

	log.WithField("thread_id", id).Info("Thread deleted")
	c.JSON(200, gin.H{"message": "thread deleted"})
}

// Reply handlers
func getThreadReplies(c *gin.Context) {
	threadID := c.Param("id")
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "50"))
	offset, _ := strconv.Atoi(c.DefaultQuery("offset", "0"))
	order := c.DefaultQuery("order", "asc")

	if limit > 100 {
		limit = 100
	}

	orderDir := "ASC"
	if order == "desc" {
		orderDir = "DESC"
	}

	var replies []ThreadReply
	query := `SELECT * FROM thread_replies WHERE thread_id = ? ORDER BY created_at ` + orderDir + ` LIMIT ? OFFSET ?`
	err := db.Select(&replies, query, threadID, limit, offset)
	if err != nil {
		c.JSON(500, gin.H{"error": "failed to fetch replies"})
		return
	}

	var total int
	db.Get(&total, `SELECT COUNT(*) FROM thread_replies WHERE thread_id = ?`, threadID)

	c.JSON(200, gin.H{
		"replies": replies,
		"total":   total,
		"limit":   limit,
		"offset":  offset,
	})
}

func addReply(c *gin.Context) {
	threadID := c.Param("id")

	var thread Thread
	if err := db.Get(&thread, `SELECT * FROM threads WHERE id = ?`, threadID); err != nil {
		c.JSON(404, gin.H{"error": "thread not found"})
		return
	}

	var req struct {
		MessageID string `json:"message_id" binding:"required"`
		UserID    string `json:"user_id" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}

	replyID := uuid.New().String()
	now := time.Now()

	tx, err := db.Beginx()
	if err != nil {
		c.JSON(500, gin.H{"error": "failed to start transaction"})
		return
	}
	defer tx.Rollback()

	// Insert reply
	_, err = tx.Exec(`INSERT INTO thread_replies (id, thread_id, message_id, user_id, created_at)
		VALUES (?, ?, ?, ?, ?)`,
		replyID, threadID, req.MessageID, req.UserID, now)
	if err != nil {
		log.Errorf("Failed to add reply: %v", err)
		c.JSON(500, gin.H{"error": "failed to add reply"})
		return
	}

	// Update thread metadata
	_, err = tx.Exec(`UPDATE threads SET reply_count = reply_count + 1, last_reply_at = ?, last_reply_by = ?, updated_at = ?
		WHERE id = ?`, now, req.UserID, now, threadID)
	if err != nil {
		log.Errorf("Failed to update thread: %v", err)
		c.JSON(500, gin.H{"error": "failed to update thread"})
		return
	}

	// Add user as participant if not already
	var existingParticipant int
	tx.Get(&existingParticipant, `SELECT COUNT(*) FROM thread_participants WHERE thread_id = ? AND user_id = ?`,
		threadID, req.UserID)

	if existingParticipant == 0 {
		participantID := uuid.New().String()
		_, err = tx.Exec(`INSERT INTO thread_participants (id, thread_id, user_id, is_subscribed, joined_at)
			VALUES (?, ?, ?, TRUE, ?)`,
			participantID, threadID, req.UserID, now)
		if err != nil {
			log.Errorf("Failed to add participant: %v", err)
		} else {
			// Update participant count
			tx.Exec(`UPDATE threads SET participant_count = participant_count + 1 WHERE id = ?`, threadID)
		}
	}

	// Update last read for the replying user
	tx.Exec(`UPDATE thread_participants SET last_read_at = ? WHERE thread_id = ? AND user_id = ?`,
		now, threadID, req.UserID)

	if err := tx.Commit(); err != nil {
		c.JSON(500, gin.H{"error": "failed to commit transaction"})
		return
	}

	// Publish event
	publishEvent(ThreadEvent{
		Type:      "thread.reply.added",
		ThreadID:  threadID,
		ChannelID: thread.ChannelID,
		UserID:    req.UserID,
		Data: map[string]interface{}{
			"reply_id":   replyID,
			"message_id": req.MessageID,
		},
		Timestamp: now,
	})

	log.WithFields(logrus.Fields{"thread_id": threadID, "reply_id": replyID}).Info("Reply added")

	reply := ThreadReply{
		ID:        replyID,
		ThreadID:  threadID,
		MessageID: req.MessageID,
		UserID:    req.UserID,
		CreatedAt: now,
	}
	c.JSON(201, reply)
}

func deleteReply(c *gin.Context) {
	threadID := c.Param("id")
	replyID := c.Param("replyId")

	var reply ThreadReply
	if err := db.Get(&reply, `SELECT * FROM thread_replies WHERE id = ? AND thread_id = ?`, replyID, threadID); err != nil {
		c.JSON(404, gin.H{"error": "reply not found"})
		return
	}

	tx, err := db.Beginx()
	if err != nil {
		c.JSON(500, gin.H{"error": "failed to start transaction"})
		return
	}
	defer tx.Rollback()

	// Delete reply
	_, err = tx.Exec(`DELETE FROM thread_replies WHERE id = ?`, replyID)
	if err != nil {
		c.JSON(500, gin.H{"error": "failed to delete reply"})
		return
	}

	// Update thread reply count
	tx.Exec(`UPDATE threads SET reply_count = reply_count - 1, updated_at = NOW() WHERE id = ?`, threadID)

	// Update last_reply_at and last_reply_by if this was the last reply
	var lastReply ThreadReply
	err = tx.Get(&lastReply, `SELECT * FROM thread_replies WHERE thread_id = ? ORDER BY created_at DESC LIMIT 1`, threadID)
	if err != nil {
		// No more replies
		tx.Exec(`UPDATE threads SET last_reply_at = NULL, last_reply_by = NULL WHERE id = ?`, threadID)
	} else {
		tx.Exec(`UPDATE threads SET last_reply_at = ?, last_reply_by = ? WHERE id = ?`,
			lastReply.CreatedAt, lastReply.UserID, threadID)
	}

	if err := tx.Commit(); err != nil {
		c.JSON(500, gin.H{"error": "failed to commit transaction"})
		return
	}

	log.WithFields(logrus.Fields{"thread_id": threadID, "reply_id": replyID}).Info("Reply deleted")
	c.JSON(200, gin.H{"message": "reply deleted"})
}

// Participant handlers
func getParticipants(c *gin.Context) {
	threadID := c.Param("id")

	var participants []ThreadParticipant
	err := db.Select(&participants, `SELECT * FROM thread_participants WHERE thread_id = ? ORDER BY joined_at ASC`, threadID)
	if err != nil {
		c.JSON(500, gin.H{"error": "failed to fetch participants"})
		return
	}

	c.JSON(200, gin.H{"participants": participants})
}

func addParticipant(c *gin.Context) {
	threadID := c.Param("id")

	var thread Thread
	if err := db.Get(&thread, `SELECT * FROM threads WHERE id = ?`, threadID); err != nil {
		c.JSON(404, gin.H{"error": "thread not found"})
		return
	}

	var req struct {
		UserID string `json:"user_id" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}

	// Check if already participant
	var existing int
	db.Get(&existing, `SELECT COUNT(*) FROM thread_participants WHERE thread_id = ? AND user_id = ?`, threadID, req.UserID)
	if existing > 0 {
		c.JSON(409, gin.H{"error": "user is already a participant"})
		return
	}

	participantID := uuid.New().String()
	now := time.Now()

	tx, err := db.Beginx()
	if err != nil {
		c.JSON(500, gin.H{"error": "failed to start transaction"})
		return
	}
	defer tx.Rollback()

	_, err = tx.Exec(`INSERT INTO thread_participants (id, thread_id, user_id, is_subscribed, joined_at)
		VALUES (?, ?, ?, TRUE, ?)`,
		participantID, threadID, req.UserID, now)
	if err != nil {
		c.JSON(500, gin.H{"error": "failed to add participant"})
		return
	}

	tx.Exec(`UPDATE threads SET participant_count = participant_count + 1, updated_at = NOW() WHERE id = ?`, threadID)

	if err := tx.Commit(); err != nil {
		c.JSON(500, gin.H{"error": "failed to commit transaction"})
		return
	}

	publishEvent(ThreadEvent{
		Type:      "thread.participant.added",
		ThreadID:  threadID,
		ChannelID: thread.ChannelID,
		UserID:    req.UserID,
		Timestamp: now,
	})

	participant := ThreadParticipant{
		ID:           participantID,
		ThreadID:     threadID,
		UserID:       req.UserID,
		IsSubscribed: true,
		JoinedAt:     now,
	}
	c.JSON(201, participant)
}

func removeParticipant(c *gin.Context) {
	threadID := c.Param("id")
	userID := c.Param("userId")

	var thread Thread
	if err := db.Get(&thread, `SELECT * FROM threads WHERE id = ?`, threadID); err != nil {
		c.JSON(404, gin.H{"error": "thread not found"})
		return
	}

	// Can't remove the creator
	if thread.CreatedBy == userID {
		c.JSON(400, gin.H{"error": "cannot remove thread creator"})
		return
	}

	tx, err := db.Beginx()
	if err != nil {
		c.JSON(500, gin.H{"error": "failed to start transaction"})
		return
	}
	defer tx.Rollback()

	result, err := tx.Exec(`DELETE FROM thread_participants WHERE thread_id = ? AND user_id = ?`, threadID, userID)
	if err != nil {
		c.JSON(500, gin.H{"error": "failed to remove participant"})
		return
	}

	rows, _ := result.RowsAffected()
	if rows == 0 {
		c.JSON(404, gin.H{"error": "participant not found"})
		return
	}

	tx.Exec(`UPDATE threads SET participant_count = participant_count - 1, updated_at = NOW() WHERE id = ?`, threadID)

	if err := tx.Commit(); err != nil {
		c.JSON(500, gin.H{"error": "failed to commit transaction"})
		return
	}

	publishEvent(ThreadEvent{
		Type:      "thread.participant.removed",
		ThreadID:  threadID,
		ChannelID: thread.ChannelID,
		UserID:    userID,
		Timestamp: time.Now(),
	})

	c.JSON(200, gin.H{"message": "participant removed"})
}

func updateSubscription(c *gin.Context) {
	threadID := c.Param("id")
	userID := c.Param("userId")

	var req struct {
		IsSubscribed bool `json:"is_subscribed"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}

	result, err := db.Exec(`UPDATE thread_participants SET is_subscribed = ? WHERE thread_id = ? AND user_id = ?`,
		req.IsSubscribed, threadID, userID)
	if err != nil {
		c.JSON(500, gin.H{"error": "failed to update subscription"})
		return
	}

	rows, _ := result.RowsAffected()
	if rows == 0 {
		c.JSON(404, gin.H{"error": "participant not found"})
		return
	}

	c.JSON(200, gin.H{"message": "subscription updated", "is_subscribed": req.IsSubscribed})
}

func markAsRead(c *gin.Context) {
	threadID := c.Param("id")
	userID := c.Param("userId")

	now := time.Now()
	result, err := db.Exec(`UPDATE thread_participants SET last_read_at = ? WHERE thread_id = ? AND user_id = ?`,
		now, threadID, userID)
	if err != nil {
		c.JSON(500, gin.H{"error": "failed to mark as read"})
		return
	}

	rows, _ := result.RowsAffected()
	if rows == 0 {
		c.JSON(404, gin.H{"error": "participant not found"})
		return
	}

	c.JSON(200, gin.H{"message": "marked as read", "last_read_at": now})
}

// Stats
func getStats(c *gin.Context) {
	var stats struct {
		TotalThreads    int `db:"total_threads"`
		ActiveThreads   int `db:"active_threads"`
		ResolvedThreads int `db:"resolved_threads"`
		TotalReplies    int `db:"total_replies"`
		TotalParticipants int `db:"total_participants"`
	}

	db.Get(&stats.TotalThreads, `SELECT COUNT(*) FROM threads`)
	db.Get(&stats.ActiveThreads, `SELECT COUNT(*) FROM threads WHERE is_resolved = FALSE`)
	db.Get(&stats.ResolvedThreads, `SELECT COUNT(*) FROM threads WHERE is_resolved = TRUE`)
	db.Get(&stats.TotalReplies, `SELECT COUNT(*) FROM thread_replies`)
	db.Get(&stats.TotalParticipants, `SELECT COUNT(*) FROM thread_participants`)

	c.JSON(200, gin.H{
		"threads": gin.H{
			"total":    stats.TotalThreads,
			"active":   stats.ActiveThreads,
			"resolved": stats.ResolvedThreads,
		},
		"replies":      stats.TotalReplies,
		"participants": stats.TotalParticipants,
	})
}

// Kafka event publishing
func publishEvent(event ThreadEvent) {
	data, err := json.Marshal(event)
	if err != nil {
		log.Errorf("Failed to marshal event: %v", err)
		return
	}

	go func() {
		err := kafkaWriter.WriteMessages(context.Background(), kafka.Message{
			Key:   []byte(event.ThreadID),
			Value: data,
			Headers: []kafka.Header{
				{Key: "event_type", Value: []byte(event.Type)},
				{Key: "channel_id", Value: []byte(event.ChannelID)},
			},
		})
		if err != nil {
			log.Errorf("Failed to publish event %s: %v", event.Type, err)
		} else {
			log.WithFields(logrus.Fields{
				"event_type": event.Type,
				"thread_id":  event.ThreadID,
			}).Debug("Event published")
		}
	}()
}
