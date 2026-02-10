package repository

import (
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/quckapp/thread-service/internal/models"
)

type ReactionRepository struct {
	db *sqlx.DB
}

func NewReactionRepository(db *sqlx.DB) *ReactionRepository {
	return &ReactionRepository{db: db}
}

func (r *ReactionRepository) Create(reaction *models.ThreadReaction) error {
	reaction.ID = uuid.New().String()
	reaction.CreatedAt = time.Now()
	_, err := r.db.Exec(`INSERT INTO thread_reactions (id, thread_id, reply_id, user_id, emoji, created_at) VALUES (?, ?, ?, ?, ?, ?)`,
		reaction.ID, reaction.ThreadID, reaction.ReplyID, reaction.UserID, reaction.Emoji, reaction.CreatedAt)
	return err
}

func (r *ReactionRepository) Delete(threadID, replyID, userID, emoji string) error {
	_, err := r.db.Exec(`DELETE FROM thread_reactions WHERE thread_id = ? AND reply_id = ? AND user_id = ? AND emoji = ?`,
		threadID, replyID, userID, emoji)
	return err
}

func (r *ReactionRepository) ListByThread(threadID string) ([]models.ThreadReaction, error) {
	var reactions []models.ThreadReaction
	err := r.db.Select(&reactions, `SELECT * FROM thread_reactions WHERE thread_id = ? ORDER BY created_at ASC`, threadID)
	return reactions, err
}

func (r *ReactionRepository) ListByReply(replyID string) ([]models.ThreadReaction, error) {
	var reactions []models.ThreadReaction
	err := r.db.Select(&reactions, `SELECT * FROM thread_reactions WHERE reply_id = ? ORDER BY created_at ASC`, replyID)
	return reactions, err
}

func (r *ReactionRepository) GetSummary(threadID string) ([]models.ReactionSummary, error) {
	var summary []models.ReactionSummary
	err := r.db.Select(&summary, `SELECT emoji, COUNT(*) as count FROM thread_reactions WHERE thread_id = ? GROUP BY emoji ORDER BY count DESC`, threadID)
	return summary, err
}

func (r *ReactionRepository) Exists(threadID, replyID, userID, emoji string) (bool, error) {
	var count int
	err := r.db.Get(&count, `SELECT COUNT(*) FROM thread_reactions WHERE thread_id = ? AND reply_id = ? AND user_id = ? AND emoji = ?`,
		threadID, replyID, userID, emoji)
	return count > 0, err
}
