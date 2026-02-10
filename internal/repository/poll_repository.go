package repository

import (
	"database/sql"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/quckapp/thread-service/internal/models"
)

type PollRepository struct {
	db *sqlx.DB
}

func NewPollRepository(db *sqlx.DB) *PollRepository {
	return &PollRepository{db: db}
}

func (r *PollRepository) Create(poll *models.ThreadPoll) error {
	poll.ID = uuid.New().String()
	poll.CreatedAt = time.Now()
	_, err := r.db.Exec(`INSERT INTO thread_polls (id, thread_id, question, created_by, multi_vote, anonymous, expires_at, is_closed, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, FALSE, ?)`,
		poll.ID, poll.ThreadID, poll.Question, poll.CreatedBy, poll.MultiVote, poll.Anonymous, poll.ExpiresAt, poll.CreatedAt)
	return err
}

func (r *PollRepository) GetByID(id string) (*models.ThreadPoll, error) {
	var poll models.ThreadPoll
	err := r.db.Get(&poll, `SELECT * FROM thread_polls WHERE id = ?`, id)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return &poll, err
}

func (r *PollRepository) GetByThread(threadID string) (*models.ThreadPoll, error) {
	var poll models.ThreadPoll
	err := r.db.Get(&poll, `SELECT * FROM thread_polls WHERE thread_id = ? ORDER BY created_at DESC LIMIT 1`, threadID)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return &poll, err
}

func (r *PollRepository) Close(id string) error {
	_, err := r.db.Exec(`UPDATE thread_polls SET is_closed = TRUE WHERE id = ?`, id)
	return err
}

func (r *PollRepository) Delete(id string) error {
	_, err := r.db.Exec(`DELETE FROM thread_polls WHERE id = ?`, id)
	return err
}

func (r *PollRepository) CreateOption(opt *models.PollOption) error {
	opt.ID = uuid.New().String()
	_, err := r.db.Exec(`INSERT INTO poll_options (id, poll_id, text, position) VALUES (?, ?, ?, ?)`,
		opt.ID, opt.PollID, opt.Text, opt.Position)
	return err
}

func (r *PollRepository) ListOptions(pollID string) ([]models.PollOption, error) {
	var opts []models.PollOption
	err := r.db.Select(&opts, `SELECT * FROM poll_options WHERE poll_id = ? ORDER BY position ASC`, pollID)
	return opts, err
}

func (r *PollRepository) CastVote(pollID, optionID, userID string) error {
	id := uuid.New().String()
	_, err := r.db.Exec(`INSERT INTO poll_votes (id, poll_id, option_id, user_id, voted_at) VALUES (?, ?, ?, ?, ?)`,
		id, pollID, optionID, userID, time.Now())
	return err
}

func (r *PollRepository) RemoveVote(pollID, optionID, userID string) error {
	_, err := r.db.Exec(`DELETE FROM poll_votes WHERE poll_id = ? AND option_id = ? AND user_id = ?`, pollID, optionID, userID)
	return err
}

func (r *PollRepository) HasVoted(pollID, userID string) (bool, error) {
	var count int
	err := r.db.Get(&count, `SELECT COUNT(*) FROM poll_votes WHERE poll_id = ? AND user_id = ?`, pollID, userID)
	return count > 0, err
}

func (r *PollRepository) HasVotedOption(pollID, optionID, userID string) (bool, error) {
	var count int
	err := r.db.Get(&count, `SELECT COUNT(*) FROM poll_votes WHERE poll_id = ? AND option_id = ? AND user_id = ?`, pollID, optionID, userID)
	return count > 0, err
}

func (r *PollRepository) GetResults(pollID string) ([]models.PollResult, error) {
	var results []models.PollResult
	err := r.db.Select(&results, `SELECT po.id as option_id, po.text, COUNT(pv.id) as votes FROM poll_options po LEFT JOIN poll_votes pv ON po.id = pv.option_id WHERE po.poll_id = ? GROUP BY po.id, po.text, po.position ORDER BY po.position ASC`, pollID)
	return results, err
}
