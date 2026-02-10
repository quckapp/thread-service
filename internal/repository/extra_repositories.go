package repository

import (
	"database/sql"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/quckapp/thread-service/internal/models"
)

// ── Draft Repository ──

type DraftRepository struct {
	db *sqlx.DB
}

func NewDraftRepository(db *sqlx.DB) *DraftRepository {
	return &DraftRepository{db: db}
}

func (r *DraftRepository) Save(draft *models.ThreadDraft) error {
	// Upsert: delete existing + insert new
	r.db.Exec(`DELETE FROM thread_drafts WHERE thread_id = ? AND user_id = ?`, draft.ThreadID, draft.UserID)
	draft.ID = uuid.New().String()
	draft.UpdatedAt = time.Now()
	_, err := r.db.Exec(`INSERT INTO thread_drafts (id, thread_id, user_id, content, updated_at) VALUES (?, ?, ?, ?, ?)`,
		draft.ID, draft.ThreadID, draft.UserID, draft.Content, draft.UpdatedAt)
	return err
}

func (r *DraftRepository) Get(threadID, userID string) (*models.ThreadDraft, error) {
	var draft models.ThreadDraft
	err := r.db.Get(&draft, `SELECT * FROM thread_drafts WHERE thread_id = ? AND user_id = ?`, threadID, userID)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return &draft, err
}

func (r *DraftRepository) Delete(threadID, userID string) error {
	_, err := r.db.Exec(`DELETE FROM thread_drafts WHERE thread_id = ? AND user_id = ?`, threadID, userID)
	return err
}

func (r *DraftRepository) ListByUser(userID string) ([]models.ThreadDraft, error) {
	var drafts []models.ThreadDraft
	err := r.db.Select(&drafts, `SELECT * FROM thread_drafts WHERE user_id = ? ORDER BY updated_at DESC`, userID)
	return drafts, err
}

// ── Mention Repository ──

type MentionRepository struct {
	db *sqlx.DB
}

func NewMentionRepository(db *sqlx.DB) *MentionRepository {
	return &MentionRepository{db: db}
}

func (r *MentionRepository) Create(m *models.ThreadMention) error {
	m.ID = uuid.New().String()
	m.CreatedAt = time.Now()
	_, err := r.db.Exec(`INSERT INTO thread_mentions (id, thread_id, reply_id, mentioned_user, mentioned_by, is_read, created_at) VALUES (?, ?, ?, ?, ?, FALSE, ?)`,
		m.ID, m.ThreadID, m.ReplyID, m.MentionedUser, m.MentionedBy, m.CreatedAt)
	return err
}

func (r *MentionRepository) ListByUser(userID string, unreadOnly bool, limit, offset int) ([]models.ThreadMention, error) {
	query := `SELECT * FROM thread_mentions WHERE mentioned_user = ?`
	args := []interface{}{userID}
	if unreadOnly {
		query += ` AND is_read = FALSE`
	}
	query += ` ORDER BY created_at DESC LIMIT ? OFFSET ?`
	args = append(args, limit, offset)
	var mentions []models.ThreadMention
	err := r.db.Select(&mentions, query, args...)
	return mentions, err
}

func (r *MentionRepository) MarkRead(id string) error {
	_, err := r.db.Exec(`UPDATE thread_mentions SET is_read = TRUE WHERE id = ?`, id)
	return err
}

func (r *MentionRepository) MarkAllRead(userID string) error {
	_, err := r.db.Exec(`UPDATE thread_mentions SET is_read = TRUE WHERE mentioned_user = ?`, userID)
	return err
}

func (r *MentionRepository) CountUnread(userID string) (int, error) {
	var count int
	err := r.db.Get(&count, `SELECT COUNT(*) FROM thread_mentions WHERE mentioned_user = ? AND is_read = FALSE`, userID)
	return count, err
}

// ── Assignment Repository ──

type AssignmentRepository struct {
	db *sqlx.DB
}

func NewAssignmentRepository(db *sqlx.DB) *AssignmentRepository {
	return &AssignmentRepository{db: db}
}

func (r *AssignmentRepository) Create(a *models.ThreadAssignment) error {
	a.ID = uuid.New().String()
	a.CreatedAt = time.Now()
	a.UpdatedAt = time.Now()
	if a.Status == "" {
		a.Status = "open"
	}
	_, err := r.db.Exec(`INSERT INTO thread_assignments (id, thread_id, assignee_id, assigned_by, status, due_date, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		a.ID, a.ThreadID, a.AssigneeID, a.AssignedBy, a.Status, a.DueDate, a.CreatedAt, a.UpdatedAt)
	return err
}

func (r *AssignmentRepository) GetByID(id string) (*models.ThreadAssignment, error) {
	var a models.ThreadAssignment
	err := r.db.Get(&a, `SELECT * FROM thread_assignments WHERE id = ?`, id)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return &a, err
}

func (r *AssignmentRepository) ListByThread(threadID string) ([]models.ThreadAssignment, error) {
	var assignments []models.ThreadAssignment
	err := r.db.Select(&assignments, `SELECT * FROM thread_assignments WHERE thread_id = ? ORDER BY created_at DESC`, threadID)
	return assignments, err
}

func (r *AssignmentRepository) ListByAssignee(userID string, status string) ([]models.ThreadAssignment, error) {
	query := `SELECT * FROM thread_assignments WHERE assignee_id = ?`
	args := []interface{}{userID}
	if status != "" {
		query += ` AND status = ?`
		args = append(args, status)
	}
	query += ` ORDER BY due_date ASC, created_at DESC`
	var assignments []models.ThreadAssignment
	err := r.db.Select(&assignments, query, args...)
	return assignments, err
}

func (r *AssignmentRepository) Update(id string, status string, dueDate *time.Time) error {
	_, err := r.db.Exec(`UPDATE thread_assignments SET status = ?, due_date = ?, updated_at = NOW() WHERE id = ?`, status, dueDate, id)
	return err
}

func (r *AssignmentRepository) Delete(id string) error {
	_, err := r.db.Exec(`DELETE FROM thread_assignments WHERE id = ?`, id)
	return err
}

// ── Activity Repository ──

type ActivityRepository struct {
	db *sqlx.DB
}

func NewActivityRepository(db *sqlx.DB) *ActivityRepository {
	return &ActivityRepository{db: db}
}

func (r *ActivityRepository) Create(a *models.ThreadActivity) error {
	a.ID = uuid.New().String()
	a.CreatedAt = time.Now()
	_, err := r.db.Exec(`INSERT INTO thread_activity_log (id, thread_id, user_id, action, details, created_at) VALUES (?, ?, ?, ?, ?, ?)`,
		a.ID, a.ThreadID, a.UserID, a.Action, a.Details, a.CreatedAt)
	return err
}

func (r *ActivityRepository) ListByThread(threadID string, limit, offset int) ([]models.ThreadActivity, error) {
	var activities []models.ThreadActivity
	err := r.db.Select(&activities, `SELECT * FROM thread_activity_log WHERE thread_id = ? ORDER BY created_at DESC LIMIT ? OFFSET ?`, threadID, limit, offset)
	return activities, err
}

// ── Attachment Repository ──

type AttachmentRepository struct {
	db *sqlx.DB
}

func NewAttachmentRepository(db *sqlx.DB) *AttachmentRepository {
	return &AttachmentRepository{db: db}
}

func (r *AttachmentRepository) Create(a *models.ThreadAttachment) error {
	a.ID = uuid.New().String()
	a.CreatedAt = time.Now()
	_, err := r.db.Exec(`INSERT INTO thread_attachments (id, thread_id, reply_id, user_id, file_name, file_size, mime_type, url, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		a.ID, a.ThreadID, a.ReplyID, a.UserID, a.FileName, a.FileSize, a.MimeType, a.URL, a.CreatedAt)
	return err
}

func (r *AttachmentRepository) ListByThread(threadID string) ([]models.ThreadAttachment, error) {
	var attachments []models.ThreadAttachment
	err := r.db.Select(&attachments, `SELECT * FROM thread_attachments WHERE thread_id = ? ORDER BY created_at DESC`, threadID)
	return attachments, err
}

func (r *AttachmentRepository) Delete(id string) error {
	_, err := r.db.Exec(`DELETE FROM thread_attachments WHERE id = ?`, id)
	return err
}

func (r *AttachmentRepository) GetByID(id string) (*models.ThreadAttachment, error) {
	var a models.ThreadAttachment
	err := r.db.Get(&a, `SELECT * FROM thread_attachments WHERE id = ?`, id)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return &a, err
}

// ── Template Repository ──

type TemplateRepository struct {
	db *sqlx.DB
}

func NewTemplateRepository(db *sqlx.DB) *TemplateRepository {
	return &TemplateRepository{db: db}
}

func (r *TemplateRepository) Create(t *models.ThreadTemplate) error {
	t.ID = uuid.New().String()
	t.CreatedAt = time.Now()
	t.UpdatedAt = time.Now()
	_, err := r.db.Exec(`INSERT INTO thread_templates (id, channel_id, name, description, content, labels, priority, created_by, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		t.ID, t.ChannelID, t.Name, t.Description, t.Content, t.Labels, t.Priority, t.CreatedBy, t.CreatedAt, t.UpdatedAt)
	return err
}

func (r *TemplateRepository) GetByID(id string) (*models.ThreadTemplate, error) {
	var t models.ThreadTemplate
	err := r.db.Get(&t, `SELECT * FROM thread_templates WHERE id = ?`, id)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return &t, err
}

func (r *TemplateRepository) ListByChannel(channelID string) ([]models.ThreadTemplate, error) {
	var templates []models.ThreadTemplate
	err := r.db.Select(&templates, `SELECT * FROM thread_templates WHERE channel_id = ? ORDER BY name ASC`, channelID)
	return templates, err
}

func (r *TemplateRepository) Update(id string, updates map[string]interface{}) error {
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
	_, err := r.db.Exec(`UPDATE thread_templates SET `+setClauses+` WHERE id = ?`, args...)
	return err
}

func (r *TemplateRepository) Delete(id string) error {
	_, err := r.db.Exec(`DELETE FROM thread_templates WHERE id = ?`, id)
	return err
}

// ── FollowUp Repository ──

type FollowUpRepository struct {
	db *sqlx.DB
}

func NewFollowUpRepository(db *sqlx.DB) *FollowUpRepository {
	return &FollowUpRepository{db: db}
}

func (r *FollowUpRepository) Create(f *models.ThreadFollowUp) error {
	f.ID = uuid.New().String()
	f.CreatedAt = time.Now()
	_, err := r.db.Exec(`INSERT INTO thread_follow_ups (id, thread_id, user_id, remind_at, note, is_triggered, created_at) VALUES (?, ?, ?, ?, ?, FALSE, ?)`,
		f.ID, f.ThreadID, f.UserID, f.RemindAt, f.Note, f.CreatedAt)
	return err
}

func (r *FollowUpRepository) ListByUser(userID string) ([]models.ThreadFollowUp, error) {
	var followUps []models.ThreadFollowUp
	err := r.db.Select(&followUps, `SELECT * FROM thread_follow_ups WHERE user_id = ? AND is_triggered = FALSE ORDER BY remind_at ASC`, userID)
	return followUps, err
}

func (r *FollowUpRepository) ListDue() ([]models.ThreadFollowUp, error) {
	var followUps []models.ThreadFollowUp
	err := r.db.Select(&followUps, `SELECT * FROM thread_follow_ups WHERE is_triggered = FALSE AND remind_at <= NOW()`)
	return followUps, err
}

func (r *FollowUpRepository) MarkTriggered(id string) error {
	_, err := r.db.Exec(`UPDATE thread_follow_ups SET is_triggered = TRUE WHERE id = ?`, id)
	return err
}

func (r *FollowUpRepository) Delete(id string) error {
	_, err := r.db.Exec(`DELETE FROM thread_follow_ups WHERE id = ?`, id)
	return err
}

// ── Summary Repository ──

type SummaryRepository struct {
	db *sqlx.DB
}

func NewSummaryRepository(db *sqlx.DB) *SummaryRepository {
	return &SummaryRepository{db: db}
}

func (r *SummaryRepository) Create(s *models.ThreadSummary) error {
	s.ID = uuid.New().String()
	s.CreatedAt = time.Now()
	_, err := r.db.Exec(`INSERT INTO thread_summaries (id, thread_id, summary, created_by, created_at) VALUES (?, ?, ?, ?, ?)`,
		s.ID, s.ThreadID, s.Summary, s.CreatedBy, s.CreatedAt)
	return err
}

func (r *SummaryRepository) GetByThread(threadID string) (*models.ThreadSummary, error) {
	var s models.ThreadSummary
	err := r.db.Get(&s, `SELECT * FROM thread_summaries WHERE thread_id = ? ORDER BY created_at DESC LIMIT 1`, threadID)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return &s, err
}

func (r *SummaryRepository) ListByThread(threadID string) ([]models.ThreadSummary, error) {
	var summaries []models.ThreadSummary
	err := r.db.Select(&summaries, `SELECT * FROM thread_summaries WHERE thread_id = ? ORDER BY created_at DESC`, threadID)
	return summaries, err
}

// ── Link Repository ──

type LinkRepository struct {
	db *sqlx.DB
}

func NewLinkRepository(db *sqlx.DB) *LinkRepository {
	return &LinkRepository{db: db}
}

func (r *LinkRepository) Create(l *models.ThreadLink) error {
	l.ID = uuid.New().String()
	l.CreatedAt = time.Now()
	if l.LinkType == "" {
		l.LinkType = "related"
	}
	_, err := r.db.Exec(`INSERT INTO thread_links (id, source_thread, target_thread, link_type, created_by, created_at) VALUES (?, ?, ?, ?, ?, ?)`,
		l.ID, l.SourceThread, l.TargetThread, l.LinkType, l.CreatedBy, l.CreatedAt)
	return err
}

func (r *LinkRepository) ListByThread(threadID string) ([]models.ThreadLink, error) {
	var links []models.ThreadLink
	err := r.db.Select(&links, `SELECT * FROM thread_links WHERE source_thread = ? OR target_thread = ? ORDER BY created_at DESC`, threadID, threadID)
	return links, err
}

func (r *LinkRepository) Delete(id string) error {
	_, err := r.db.Exec(`DELETE FROM thread_links WHERE id = ?`, id)
	return err
}
