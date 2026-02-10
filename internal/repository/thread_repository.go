package repository

import (
	"database/sql"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/quckapp/thread-service/internal/models"
)

type ThreadRepository struct {
	db *sqlx.DB
}

func NewThreadRepository(db *sqlx.DB) *ThreadRepository {
	return &ThreadRepository{db: db}
}

func (r *ThreadRepository) Create(thread *models.Thread) error {
	thread.ID = uuid.New().String()
	thread.CreatedAt = time.Now()
	thread.UpdatedAt = time.Now()
	_, err := r.db.Exec(`INSERT INTO threads (id, channel_id, parent_message_id, created_by, title, reply_count, participant_count, is_resolved, is_pinned, is_locked, is_archived, priority, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, 0, 1, FALSE, FALSE, FALSE, FALSE, ?, ?, ?)`,
		thread.ID, thread.ChannelID, thread.ParentMessageID, thread.CreatedBy, thread.Title, thread.Priority, thread.CreatedAt, thread.UpdatedAt)
	return err
}

func (r *ThreadRepository) GetByID(id string) (*models.Thread, error) {
	var thread models.Thread
	err := r.db.Get(&thread, `SELECT * FROM threads WHERE id = ?`, id)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return &thread, err
}

func (r *ThreadRepository) GetByMessageID(messageID string) (*models.Thread, error) {
	var thread models.Thread
	err := r.db.Get(&thread, `SELECT * FROM threads WHERE parent_message_id = ?`, messageID)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return &thread, err
}

func (r *ThreadRepository) List(limit, offset int) ([]models.Thread, int, error) {
	var threads []models.Thread
	err := r.db.Select(&threads, `SELECT * FROM threads ORDER BY updated_at DESC LIMIT ? OFFSET ?`, limit, offset)
	if err != nil {
		return nil, 0, err
	}
	var total int
	r.db.Get(&total, `SELECT COUNT(*) FROM threads`)
	return threads, total, nil
}

func (r *ThreadRepository) ListByChannel(channelID string, includeResolved bool, limit, offset int) ([]models.Thread, int, error) {
	query := `SELECT * FROM threads WHERE channel_id = ?`
	countQuery := `SELECT COUNT(*) FROM threads WHERE channel_id = ?`
	args := []interface{}{channelID}
	if !includeResolved {
		query += ` AND is_resolved = FALSE`
		countQuery += ` AND is_resolved = FALSE`
	}
	query += ` ORDER BY COALESCE(last_reply_at, created_at) DESC LIMIT ? OFFSET ?`

	var threads []models.Thread
	err := r.db.Select(&threads, query, append(args, limit, offset)...)
	if err != nil {
		return nil, 0, err
	}
	var total int
	r.db.Get(&total, countQuery, channelID)
	return threads, total, nil
}

func (r *ThreadRepository) ListByUser(userID string, subscribedOnly bool, limit, offset int) ([]models.Thread, error) {
	query := `SELECT t.* FROM threads t INNER JOIN thread_participants tp ON t.id = tp.thread_id WHERE tp.user_id = ?`
	args := []interface{}{userID}
	if subscribedOnly {
		query += ` AND tp.is_subscribed = TRUE`
	}
	query += ` ORDER BY COALESCE(t.last_reply_at, t.created_at) DESC LIMIT ? OFFSET ?`
	args = append(args, limit, offset)

	var threads []models.Thread
	err := r.db.Select(&threads, query, args...)
	return threads, err
}

func (r *ThreadRepository) Update(id string, updates map[string]interface{}) error {
	updates["updated_at"] = time.Now()
	setClauses := ""
	args := []interface{}{}
	for k, v := range updates {
		if setClauses != "" {
			setClauses += ", "
		}
		setClauses += k + " = ?"
		args = append(args, v)
	}
	args = append(args, id)
	_, err := r.db.Exec(`UPDATE threads SET `+setClauses+` WHERE id = ?`, args...)
	return err
}

func (r *ThreadRepository) Delete(id string) error {
	_, err := r.db.Exec(`DELETE FROM threads WHERE id = ?`, id)
	return err
}

func (r *ThreadRepository) IncrementReplyCount(threadID string, replyAt time.Time, replyBy string) error {
	_, err := r.db.Exec(`UPDATE threads SET reply_count = reply_count + 1, last_reply_at = ?, last_reply_by = ?, updated_at = ? WHERE id = ?`,
		replyAt, replyBy, time.Now(), threadID)
	return err
}

func (r *ThreadRepository) DecrementReplyCount(threadID string) error {
	_, err := r.db.Exec(`UPDATE threads SET reply_count = reply_count - 1, updated_at = NOW() WHERE id = ?`, threadID)
	return err
}

func (r *ThreadRepository) IncrementParticipantCount(threadID string) error {
	_, err := r.db.Exec(`UPDATE threads SET participant_count = participant_count + 1, updated_at = NOW() WHERE id = ?`, threadID)
	return err
}

func (r *ThreadRepository) DecrementParticipantCount(threadID string) error {
	_, err := r.db.Exec(`UPDATE threads SET participant_count = participant_count - 1, updated_at = NOW() WHERE id = ?`, threadID)
	return err
}

func (r *ThreadRepository) SetLastReply(threadID string, replyAt *time.Time, replyBy *string) error {
	_, err := r.db.Exec(`UPDATE threads SET last_reply_at = ?, last_reply_by = ? WHERE id = ?`, replyAt, replyBy, threadID)
	return err
}

func (r *ThreadRepository) Search(req *models.SearchThreadsRequest) ([]models.Thread, int, error) {
	query := `SELECT t.* FROM threads t`
	countQuery := `SELECT COUNT(*) FROM threads t`
	where := " WHERE 1=1"
	args := []interface{}{}

	if req.LabelID != "" {
		query += ` INNER JOIN thread_label_assignments tla ON t.id = tla.thread_id`
		countQuery += ` INNER JOIN thread_label_assignments tla ON t.id = tla.thread_id`
		where += ` AND tla.label_id = ?`
		args = append(args, req.LabelID)
	}
	if req.ChannelID != "" {
		where += ` AND t.channel_id = ?`
		args = append(args, req.ChannelID)
	}
	if req.CreatedBy != "" {
		where += ` AND t.created_by = ?`
		args = append(args, req.CreatedBy)
	}
	if req.IsResolved != nil {
		where += ` AND t.is_resolved = ?`
		args = append(args, *req.IsResolved)
	}
	if req.Priority != "" {
		where += ` AND t.priority = ?`
		args = append(args, req.Priority)
	}
	if req.Query != "" {
		where += ` AND (t.title LIKE ? OR t.parent_message_id LIKE ?)`
		like := "%" + req.Query + "%"
		args = append(args, like, like)
	}

	var total int
	_ = r.db.Get(&total, countQuery+where, args...)

	orderBy := " ORDER BY t.updated_at DESC"
	if req.SortBy == "created" {
		orderBy = " ORDER BY t.created_at DESC"
	} else if req.SortBy == "replies" {
		orderBy = " ORDER BY t.reply_count DESC"
	}

	limit := req.Limit
	if limit <= 0 || limit > 100 {
		limit = 50
	}

	query += where + orderBy + ` LIMIT ? OFFSET ?`
	args = append(args, limit, req.Offset)

	var threads []models.Thread
	err := r.db.Select(&threads, query, args...)
	return threads, total, err
}

func (r *ThreadRepository) ListPinned(channelID string) ([]models.Thread, error) {
	var threads []models.Thread
	err := r.db.Select(&threads, `SELECT * FROM threads WHERE channel_id = ? AND is_pinned = TRUE ORDER BY updated_at DESC`, channelID)
	return threads, err
}

func (r *ThreadRepository) BulkArchive(threadIDs []string) error {
	query, args, err := sqlx.In(`UPDATE threads SET is_archived = TRUE, updated_at = NOW() WHERE id IN (?)`, threadIDs)
	if err != nil {
		return err
	}
	_, err = r.db.Exec(r.db.Rebind(query), args...)
	return err
}

func (r *ThreadRepository) BulkUnarchive(threadIDs []string) error {
	query, args, err := sqlx.In(`UPDATE threads SET is_archived = FALSE, updated_at = NOW() WHERE id IN (?)`, threadIDs)
	if err != nil {
		return err
	}
	_, err = r.db.Exec(r.db.Rebind(query), args...)
	return err
}

func (r *ThreadRepository) GetStats() (*models.ThreadStats, error) {
	stats := &models.ThreadStats{}
	r.db.Get(&stats.TotalThreads, `SELECT COUNT(*) FROM threads`)
	r.db.Get(&stats.ActiveThreads, `SELECT COUNT(*) FROM threads WHERE is_resolved = FALSE`)
	r.db.Get(&stats.ResolvedThreads, `SELECT COUNT(*) FROM threads WHERE is_resolved = TRUE`)
	r.db.Get(&stats.TotalReplies, `SELECT COUNT(*) FROM thread_replies`)
	r.db.Get(&stats.TotalParticipants, `SELECT COUNT(*) FROM thread_participants`)
	return stats, nil
}

func (r *ThreadRepository) GetChannelStats(channelID string) (*models.ChannelThreadStats, error) {
	stats := &models.ChannelThreadStats{ChannelID: channelID}
	r.db.Get(&stats.ThreadCount, `SELECT COUNT(*) FROM threads WHERE channel_id = ?`, channelID)
	r.db.Get(&stats.ActiveCount, `SELECT COUNT(*) FROM threads WHERE channel_id = ? AND is_resolved = FALSE`, channelID)
	r.db.Get(&stats.AvgReplyCount, `SELECT COALESCE(AVG(reply_count), 0) FROM threads WHERE channel_id = ?`, channelID)
	return stats, nil
}

func (r *ThreadRepository) GetRecentlyActive(channelID string, limit int) ([]models.Thread, error) {
	var threads []models.Thread
	err := r.db.Select(&threads, `SELECT * FROM threads WHERE channel_id = ? AND is_archived = FALSE ORDER BY COALESCE(last_reply_at, updated_at) DESC LIMIT ?`, channelID, limit)
	return threads, err
}

func (r *ThreadRepository) Beginx() (*sqlx.Tx, error) {
	return r.db.Beginx()
}
