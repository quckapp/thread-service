package repository

import (
	"database/sql"

	"github.com/jmoiron/sqlx"
	"github.com/quckapp/thread-service/internal/models"
)

// ── Permission Repository ──

type PermissionRepository struct{ db *sqlx.DB }

func NewPermissionRepository(db *sqlx.DB) *PermissionRepository { return &PermissionRepository{db: db} }

func (r *PermissionRepository) Set(p *models.ThreadPermission) error {
	_, err := r.db.NamedExec(`REPLACE INTO thread_permissions (id, thread_id, user_id, can_reply, can_edit, can_delete, can_pin, can_lock, can_invite, granted_by, created_at, updated_at) VALUES (:id, :thread_id, :user_id, :can_reply, :can_edit, :can_delete, :can_pin, :can_lock, :can_invite, :granted_by, :created_at, :updated_at)`, p)
	return err
}

func (r *PermissionRepository) Get(threadID, userID string) (*models.ThreadPermission, error) {
	var p models.ThreadPermission
	err := r.db.Get(&p, "SELECT * FROM thread_permissions WHERE thread_id = ? AND user_id = ?", threadID, userID)
	if err == sql.ErrNoRows { return nil, nil }
	return &p, err
}

func (r *PermissionRepository) ListByThread(threadID string) ([]*models.ThreadPermission, error) {
	var perms []*models.ThreadPermission
	err := r.db.Select(&perms, "SELECT * FROM thread_permissions WHERE thread_id = ? ORDER BY created_at DESC", threadID)
	return perms, err
}

func (r *PermissionRepository) Delete(threadID, userID string) error {
	_, err := r.db.Exec("DELETE FROM thread_permissions WHERE thread_id = ? AND user_id = ?", threadID, userID)
	return err
}

// ── Watcher Repository ──

type WatcherRepository struct{ db *sqlx.DB }

func NewWatcherRepository(db *sqlx.DB) *WatcherRepository { return &WatcherRepository{db: db} }

func (r *WatcherRepository) Add(w *models.ThreadWatcher) error {
	_, err := r.db.NamedExec(`INSERT INTO thread_watchers (id, thread_id, user_id, notify_on, created_at) VALUES (:id, :thread_id, :user_id, :notify_on, :created_at)`, w)
	return err
}

func (r *WatcherRepository) Remove(threadID, userID string) error {
	_, err := r.db.Exec("DELETE FROM thread_watchers WHERE thread_id = ? AND user_id = ?", threadID, userID)
	return err
}

func (r *WatcherRepository) ListByThread(threadID string) ([]*models.ThreadWatcher, error) {
	var watchers []*models.ThreadWatcher
	err := r.db.Select(&watchers, "SELECT * FROM thread_watchers WHERE thread_id = ?", threadID)
	return watchers, err
}

func (r *WatcherRepository) ListByUser(userID string, limit, offset int) ([]*models.ThreadWatcher, error) {
	var watchers []*models.ThreadWatcher
	err := r.db.Select(&watchers, "SELECT * FROM thread_watchers WHERE user_id = ? ORDER BY created_at DESC LIMIT ? OFFSET ?", userID, limit, offset)
	return watchers, err
}

func (r *WatcherRepository) IsWatching(threadID, userID string) (bool, error) {
	var count int
	err := r.db.Get(&count, "SELECT COUNT(*) FROM thread_watchers WHERE thread_id = ? AND user_id = ?", threadID, userID)
	return count > 0, err
}

func (r *WatcherRepository) Update(threadID, userID, notifyOn string) error {
	_, err := r.db.Exec("UPDATE thread_watchers SET notify_on = ? WHERE thread_id = ? AND user_id = ?", notifyOn, threadID, userID)
	return err
}

// ── Read Receipt Repository ──

type ReadReceiptRepository struct{ db *sqlx.DB }

func NewReadReceiptRepository(db *sqlx.DB) *ReadReceiptRepository { return &ReadReceiptRepository{db: db} }

func (r *ReadReceiptRepository) Upsert(rr *models.ReadReceipt) error {
	_, err := r.db.NamedExec(`REPLACE INTO read_receipts (id, thread_id, user_id, reply_id, read_at) VALUES (:id, :thread_id, :user_id, :reply_id, :read_at)`, rr)
	return err
}

func (r *ReadReceiptRepository) ListByThread(threadID string) ([]*models.ReadReceipt, error) {
	var receipts []*models.ReadReceipt
	err := r.db.Select(&receipts, "SELECT * FROM read_receipts WHERE thread_id = ? ORDER BY read_at DESC", threadID)
	return receipts, err
}

func (r *ReadReceiptRepository) GetByUser(threadID, userID string) (*models.ReadReceipt, error) {
	var rr models.ReadReceipt
	err := r.db.Get(&rr, "SELECT * FROM read_receipts WHERE thread_id = ? AND user_id = ?", threadID, userID)
	if err == sql.ErrNoRows { return nil, nil }
	return &rr, err
}

func (r *ReadReceiptRepository) CountUnread(threadID, userID string) (int, error) {
	var lastRead models.ReadReceipt
	err := r.db.Get(&lastRead, "SELECT * FROM read_receipts WHERE thread_id = ? AND user_id = ?", threadID, userID)
	if err == sql.ErrNoRows {
		var total int
		_ = r.db.Get(&total, "SELECT COUNT(*) FROM thread_replies WHERE thread_id = ?", threadID)
		return total, nil
	}
	if err != nil { return 0, err }
	var count int
	err = r.db.Get(&count, "SELECT COUNT(*) FROM thread_replies WHERE thread_id = ? AND created_at > ?", threadID, lastRead.ReadAt)
	return count, err
}

// ── Scheduled Message Repository ──

type ScheduledMessageRepository struct{ db *sqlx.DB }

func NewScheduledMessageRepository(db *sqlx.DB) *ScheduledMessageRepository { return &ScheduledMessageRepository{db: db} }

func (r *ScheduledMessageRepository) Create(msg *models.ScheduledMessage) error {
	_, err := r.db.NamedExec(`INSERT INTO scheduled_messages (id, thread_id, user_id, content, scheduled_at, status, created_at) VALUES (:id, :thread_id, :user_id, :content, :scheduled_at, :status, :created_at)`, msg)
	return err
}

func (r *ScheduledMessageRepository) ListByThread(threadID string) ([]*models.ScheduledMessage, error) {
	var msgs []*models.ScheduledMessage
	err := r.db.Select(&msgs, "SELECT * FROM scheduled_messages WHERE thread_id = ? AND status = 'pending' ORDER BY scheduled_at ASC", threadID)
	return msgs, err
}

func (r *ScheduledMessageRepository) ListByUser(userID string, limit, offset int) ([]*models.ScheduledMessage, error) {
	var msgs []*models.ScheduledMessage
	err := r.db.Select(&msgs, "SELECT * FROM scheduled_messages WHERE user_id = ? ORDER BY scheduled_at ASC LIMIT ? OFFSET ?", userID, limit, offset)
	return msgs, err
}

func (r *ScheduledMessageRepository) Cancel(id string) error {
	_, err := r.db.Exec("UPDATE scheduled_messages SET status = 'cancelled' WHERE id = ?", id)
	return err
}

func (r *ScheduledMessageRepository) GetPending() ([]*models.ScheduledMessage, error) {
	var msgs []*models.ScheduledMessage
	err := r.db.Select(&msgs, "SELECT * FROM scheduled_messages WHERE status = 'pending' AND scheduled_at <= NOW()")
	return msgs, err
}

func (r *ScheduledMessageRepository) MarkSent(id string) error {
	_, err := r.db.Exec("UPDATE scheduled_messages SET status = 'sent', sent_at = NOW() WHERE id = ?", id)
	return err
}

// ── Saved Reply Repository ──

type SavedReplyRepository struct{ db *sqlx.DB }

func NewSavedReplyRepository(db *sqlx.DB) *SavedReplyRepository { return &SavedReplyRepository{db: db} }

func (r *SavedReplyRepository) Create(sr *models.SavedReply) error {
	_, err := r.db.NamedExec(`INSERT INTO saved_replies (id, user_id, title, content, shortcut, use_count, created_at, updated_at) VALUES (:id, :user_id, :title, :content, :shortcut, :use_count, :created_at, :updated_at)`, sr)
	return err
}

func (r *SavedReplyRepository) Get(id string) (*models.SavedReply, error) {
	var sr models.SavedReply
	err := r.db.Get(&sr, "SELECT * FROM saved_replies WHERE id = ?", id)
	if err == sql.ErrNoRows { return nil, nil }
	return &sr, err
}

func (r *SavedReplyRepository) ListByUser(userID string, limit, offset int) ([]*models.SavedReply, error) {
	var replies []*models.SavedReply
	err := r.db.Select(&replies, "SELECT * FROM saved_replies WHERE user_id = ? ORDER BY use_count DESC, created_at DESC LIMIT ? OFFSET ?", userID, limit, offset)
	return replies, err
}

func (r *SavedReplyRepository) Update(sr *models.SavedReply) error {
	_, err := r.db.NamedExec(`UPDATE saved_replies SET title = :title, content = :content, shortcut = :shortcut, updated_at = :updated_at WHERE id = :id`, sr)
	return err
}

func (r *SavedReplyRepository) Delete(id string) error {
	_, err := r.db.Exec("DELETE FROM saved_replies WHERE id = ?", id)
	return err
}

func (r *SavedReplyRepository) IncrementUseCount(id string) error {
	_, err := r.db.Exec("UPDATE saved_replies SET use_count = use_count + 1 WHERE id = ?", id)
	return err
}

func (r *SavedReplyRepository) SearchByUser(userID, query string) ([]*models.SavedReply, error) {
	var replies []*models.SavedReply
	err := r.db.Select(&replies, "SELECT * FROM saved_replies WHERE user_id = ? AND (title LIKE ? OR content LIKE ?) ORDER BY use_count DESC", userID, "%"+query+"%", "%"+query+"%")
	return replies, err
}

// ── Tag Repository ──

type TagRepository struct{ db *sqlx.DB }

func NewTagRepository(db *sqlx.DB) *TagRepository { return &TagRepository{db: db} }

func (r *TagRepository) Add(t *models.ThreadTag) error {
	_, err := r.db.NamedExec(`INSERT INTO thread_tags (id, thread_id, tag, added_by, created_at) VALUES (:id, :thread_id, :tag, :added_by, :created_at)`, t)
	return err
}

func (r *TagRepository) Remove(threadID, tag string) error {
	_, err := r.db.Exec("DELETE FROM thread_tags WHERE thread_id = ? AND tag = ?", threadID, tag)
	return err
}

func (r *TagRepository) ListByThread(threadID string) ([]*models.ThreadTag, error) {
	var tags []*models.ThreadTag
	err := r.db.Select(&tags, "SELECT * FROM thread_tags WHERE thread_id = ? ORDER BY created_at DESC", threadID)
	return tags, err
}

func (r *TagRepository) SearchByTag(tag string, limit, offset int) ([]*models.ThreadTag, error) {
	var tags []*models.ThreadTag
	err := r.db.Select(&tags, "SELECT * FROM thread_tags WHERE tag = ? LIMIT ? OFFSET ?", tag, limit, offset)
	return tags, err
}

func (r *TagRepository) ListPopular(limit int) ([]string, error) {
	var tags []string
	err := r.db.Select(&tags, "SELECT tag FROM thread_tags GROUP BY tag ORDER BY COUNT(*) DESC LIMIT ?", limit)
	return tags, err
}

// ── Favorite Repository ──

type FavoriteRepository struct{ db *sqlx.DB }

func NewFavoriteRepository(db *sqlx.DB) *FavoriteRepository { return &FavoriteRepository{db: db} }

func (r *FavoriteRepository) Add(f *models.ThreadFavorite) error {
	_, err := r.db.NamedExec(`INSERT INTO thread_favorites (id, thread_id, user_id, created_at) VALUES (:id, :thread_id, :user_id, :created_at)`, f)
	return err
}

func (r *FavoriteRepository) Remove(threadID, userID string) error {
	_, err := r.db.Exec("DELETE FROM thread_favorites WHERE thread_id = ? AND user_id = ?", threadID, userID)
	return err
}

func (r *FavoriteRepository) ListByUser(userID string, limit, offset int) ([]*models.ThreadFavorite, error) {
	var favs []*models.ThreadFavorite
	err := r.db.Select(&favs, "SELECT * FROM thread_favorites WHERE user_id = ? ORDER BY created_at DESC LIMIT ? OFFSET ?", userID, limit, offset)
	return favs, err
}

func (r *FavoriteRepository) IsFavorited(threadID, userID string) (bool, error) {
	var count int
	err := r.db.Get(&count, "SELECT COUNT(*) FROM thread_favorites WHERE thread_id = ? AND user_id = ?", threadID, userID)
	return count > 0, err
}

// ── Pin Repository ──

type PinRepository struct{ db *sqlx.DB }

func NewPinRepository(db *sqlx.DB) *PinRepository { return &PinRepository{db: db} }

func (r *PinRepository) Pin(p *models.ThreadPin) error {
	_, err := r.db.NamedExec(`INSERT INTO thread_pins (id, thread_id, channel_id, pinned_by, pinned_at) VALUES (:id, :thread_id, :channel_id, :pinned_by, :pinned_at)`, p)
	return err
}

func (r *PinRepository) Unpin(threadID string) error {
	_, err := r.db.Exec("DELETE FROM thread_pins WHERE thread_id = ?", threadID)
	return err
}

func (r *PinRepository) ListByChannel(channelID string) ([]*models.ThreadPin, error) {
	var pins []*models.ThreadPin
	err := r.db.Select(&pins, "SELECT * FROM thread_pins WHERE channel_id = ? ORDER BY pinned_at DESC", channelID)
	return pins, err
}

func (r *PinRepository) IsPinned(threadID string) (bool, error) {
	var count int
	err := r.db.Get(&count, "SELECT COUNT(*) FROM thread_pins WHERE thread_id = ?", threadID)
	return count > 0, err
}

// ── Notification Preference Repository ──

type NotificationPrefRepository struct{ db *sqlx.DB }

func NewNotificationPrefRepository(db *sqlx.DB) *NotificationPrefRepository { return &NotificationPrefRepository{db: db} }

func (r *NotificationPrefRepository) Upsert(np *models.NotificationPreference) error {
	_, err := r.db.NamedExec(`REPLACE INTO notification_preferences (id, thread_id, user_id, muted, mute_until, desktop, mobile, email, created_at) VALUES (:id, :thread_id, :user_id, :muted, :mute_until, :desktop, :mobile, :email, :created_at)`, np)
	return err
}

func (r *NotificationPrefRepository) Get(threadID, userID string) (*models.NotificationPreference, error) {
	var np models.NotificationPreference
	err := r.db.Get(&np, "SELECT * FROM notification_preferences WHERE thread_id = ? AND user_id = ?", threadID, userID)
	if err == sql.ErrNoRows { return nil, nil }
	return &np, err
}

func (r *NotificationPrefRepository) ListMuted(userID string) ([]*models.NotificationPreference, error) {
	var prefs []*models.NotificationPreference
	err := r.db.Select(&prefs, "SELECT * FROM notification_preferences WHERE user_id = ? AND muted = true", userID)
	return prefs, err
}
