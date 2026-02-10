package repository

import (
	"database/sql"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/quckapp/thread-service/internal/models"
)

type ParticipantRepository struct {
	db *sqlx.DB
}

func NewParticipantRepository(db *sqlx.DB) *ParticipantRepository {
	return &ParticipantRepository{db: db}
}

func (r *ParticipantRepository) Create(p *models.ThreadParticipant) error {
	p.ID = uuid.New().String()
	p.JoinedAt = time.Now()
	_, err := r.db.Exec(`INSERT INTO thread_participants (id, thread_id, user_id, is_subscribed, joined_at) VALUES (?, ?, ?, ?, ?)`,
		p.ID, p.ThreadID, p.UserID, p.IsSubscribed, p.JoinedAt)
	return err
}

func (r *ParticipantRepository) GetByThreadAndUser(threadID, userID string) (*models.ThreadParticipant, error) {
	var p models.ThreadParticipant
	err := r.db.Get(&p, `SELECT * FROM thread_participants WHERE thread_id = ? AND user_id = ?`, threadID, userID)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return &p, err
}

func (r *ParticipantRepository) ListByThread(threadID string) ([]models.ThreadParticipant, error) {
	var ps []models.ThreadParticipant
	err := r.db.Select(&ps, `SELECT * FROM thread_participants WHERE thread_id = ? ORDER BY joined_at ASC`, threadID)
	return ps, err
}

func (r *ParticipantRepository) Delete(threadID, userID string) error {
	_, err := r.db.Exec(`DELETE FROM thread_participants WHERE thread_id = ? AND user_id = ?`, threadID, userID)
	return err
}

func (r *ParticipantRepository) UpdateSubscription(threadID, userID string, isSubscribed bool) error {
	_, err := r.db.Exec(`UPDATE thread_participants SET is_subscribed = ? WHERE thread_id = ? AND user_id = ?`, isSubscribed, threadID, userID)
	return err
}

func (r *ParticipantRepository) MarkRead(threadID, userID string) error {
	now := time.Now()
	_, err := r.db.Exec(`UPDATE thread_participants SET last_read_at = ? WHERE thread_id = ? AND user_id = ?`, now, threadID, userID)
	return err
}

func (r *ParticipantRepository) Exists(threadID, userID string) (bool, error) {
	var count int
	err := r.db.Get(&count, `SELECT COUNT(*) FROM thread_participants WHERE thread_id = ? AND user_id = ?`, threadID, userID)
	return count > 0, err
}

func (r *ParticipantRepository) CountByThread(threadID string) (int, error) {
	var count int
	err := r.db.Get(&count, `SELECT COUNT(*) FROM thread_participants WHERE thread_id = ?`, threadID)
	return count, err
}

func (r *ParticipantRepository) GetSubscribers(threadID string) ([]models.ThreadParticipant, error) {
	var ps []models.ThreadParticipant
	err := r.db.Select(&ps, `SELECT * FROM thread_participants WHERE thread_id = ? AND is_subscribed = TRUE`, threadID)
	return ps, err
}
