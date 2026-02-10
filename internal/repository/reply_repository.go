package repository

import (
	"database/sql"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/quckapp/thread-service/internal/models"
)

type ReplyRepository struct {
	db *sqlx.DB
}

func NewReplyRepository(db *sqlx.DB) *ReplyRepository {
	return &ReplyRepository{db: db}
}

func (r *ReplyRepository) Create(reply *models.ThreadReply) error {
	reply.ID = uuid.New().String()
	reply.CreatedAt = time.Now()
	reply.UpdatedAt = time.Now()
	_, err := r.db.Exec(`INSERT INTO thread_replies (id, thread_id, message_id, user_id, content, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		reply.ID, reply.ThreadID, reply.MessageID, reply.UserID, reply.Content, reply.CreatedAt, reply.UpdatedAt)
	return err
}

func (r *ReplyRepository) GetByID(id string) (*models.ThreadReply, error) {
	var reply models.ThreadReply
	err := r.db.Get(&reply, `SELECT * FROM thread_replies WHERE id = ?`, id)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return &reply, err
}

func (r *ReplyRepository) ListByThread(threadID string, order string, limit, offset int) ([]models.ThreadReply, int, error) {
	orderDir := "ASC"
	if order == "desc" {
		orderDir = "DESC"
	}
	var replies []models.ThreadReply
	err := r.db.Select(&replies, `SELECT * FROM thread_replies WHERE thread_id = ? ORDER BY created_at `+orderDir+` LIMIT ? OFFSET ?`, threadID, limit, offset)
	if err != nil {
		return nil, 0, err
	}
	var total int
	r.db.Get(&total, `SELECT COUNT(*) FROM thread_replies WHERE thread_id = ?`, threadID)
	return replies, total, nil
}

func (r *ReplyRepository) Delete(id string) error {
	_, err := r.db.Exec(`DELETE FROM thread_replies WHERE id = ?`, id)
	return err
}

func (r *ReplyRepository) GetLastReply(threadID string) (*models.ThreadReply, error) {
	var reply models.ThreadReply
	err := r.db.Get(&reply, `SELECT * FROM thread_replies WHERE thread_id = ? ORDER BY created_at DESC LIMIT 1`, threadID)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return &reply, err
}

func (r *ReplyRepository) CountByThread(threadID string) (int, error) {
	var count int
	err := r.db.Get(&count, `SELECT COUNT(*) FROM thread_replies WHERE thread_id = ?`, threadID)
	return count, err
}

func (r *ReplyRepository) Update(id, content string) error {
	_, err := r.db.Exec(`UPDATE thread_replies SET content = ?, updated_at = NOW() WHERE id = ?`, content, id)
	return err
}
