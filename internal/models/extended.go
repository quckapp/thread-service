package models

import "time"

// ── Thread Permissions ──

type ThreadPermission struct {
	ID        string    `db:"id" json:"id"`
	ThreadID  string    `db:"thread_id" json:"thread_id"`
	UserID    string    `db:"user_id" json:"user_id"`
	CanReply  bool      `db:"can_reply" json:"can_reply"`
	CanEdit   bool      `db:"can_edit" json:"can_edit"`
	CanDelete bool      `db:"can_delete" json:"can_delete"`
	CanPin    bool      `db:"can_pin" json:"can_pin"`
	CanLock   bool      `db:"can_lock" json:"can_lock"`
	CanInvite bool      `db:"can_invite" json:"can_invite"`
	GrantedBy string    `db:"granted_by" json:"granted_by"`
	CreatedAt time.Time `db:"created_at" json:"created_at"`
	UpdatedAt time.Time `db:"updated_at" json:"updated_at"`
}

// ── Thread Watchers (separate from participants) ──

type ThreadWatcher struct {
	ID        string    `db:"id" json:"id"`
	ThreadID  string    `db:"thread_id" json:"thread_id"`
	UserID    string    `db:"user_id" json:"user_id"`
	NotifyOn  string    `db:"notify_on" json:"notify_on"` // all, mentions, none
	CreatedAt time.Time `db:"created_at" json:"created_at"`
}

// ── Read Receipts ──

type ReadReceipt struct {
	ID        string    `db:"id" json:"id"`
	ThreadID  string    `db:"thread_id" json:"thread_id"`
	UserID    string    `db:"user_id" json:"user_id"`
	ReplyID   string    `db:"reply_id" json:"reply_id"`
	ReadAt    time.Time `db:"read_at" json:"read_at"`
}

// ── Typing Indicators ──

type TypingStatus struct {
	ThreadID  string    `json:"thread_id"`
	UserID    string    `json:"user_id"`
	IsTyping  bool      `json:"is_typing"`
	Timestamp time.Time `json:"timestamp"`
}

// ── Scheduled Messages ──

type ScheduledMessage struct {
	ID          string     `db:"id" json:"id"`
	ThreadID    string     `db:"thread_id" json:"thread_id"`
	UserID      string     `db:"user_id" json:"user_id"`
	Content     string     `db:"content" json:"content"`
	ScheduledAt time.Time  `db:"scheduled_at" json:"scheduled_at"`
	SentAt      *time.Time `db:"sent_at" json:"sent_at"`
	Status      string     `db:"status" json:"status"` // pending, sent, cancelled
	CreatedAt   time.Time  `db:"created_at" json:"created_at"`
}

// ── Saved Replies ──

type SavedReply struct {
	ID        string    `db:"id" json:"id"`
	UserID    string    `db:"user_id" json:"user_id"`
	Title     string    `db:"title" json:"title"`
	Content   string    `db:"content" json:"content"`
	Shortcut  string    `db:"shortcut" json:"shortcut"`
	UseCount  int       `db:"use_count" json:"use_count"`
	CreatedAt time.Time `db:"created_at" json:"created_at"`
	UpdatedAt time.Time `db:"updated_at" json:"updated_at"`
}

// ── Thread Tags ──

type ThreadTag struct {
	ID        string    `db:"id" json:"id"`
	ThreadID  string    `db:"thread_id" json:"thread_id"`
	Tag       string    `db:"tag" json:"tag"`
	AddedBy   string    `db:"added_by" json:"added_by"`
	CreatedAt time.Time `db:"created_at" json:"created_at"`
}

// ── Thread Favorites ──

type ThreadFavorite struct {
	ID        string    `db:"id" json:"id"`
	ThreadID  string    `db:"thread_id" json:"thread_id"`
	UserID    string    `db:"user_id" json:"user_id"`
	CreatedAt time.Time `db:"created_at" json:"created_at"`
}

// ── Thread Pins ──

type ThreadPin struct {
	ID        string    `db:"id" json:"id"`
	ThreadID  string    `db:"thread_id" json:"thread_id"`
	ChannelID string    `db:"channel_id" json:"channel_id"`
	PinnedBy  string    `db:"pinned_by" json:"pinned_by"`
	PinnedAt  time.Time `db:"pinned_at" json:"pinned_at"`
}

// ── Thread Analytics ──

type ThreadAnalytics struct {
	ThreadID        string         `json:"thread_id"`
	TotalReplies    int            `json:"total_replies"`
	TotalReactions  int            `json:"total_reactions"`
	UniqueParticipants int         `json:"unique_participants"`
	AvgResponseTime float64        `json:"avg_response_time_seconds"`
	MostActiveUsers []UserActivity `json:"most_active_users"`
	PeakHours       map[int]int    `json:"peak_hours"`
	SentimentScore  float64        `json:"sentiment_score"`
}

type UserActivity struct {
	UserID     string `json:"user_id"`
	ReplyCount int    `json:"reply_count"`
	Reactions  int    `json:"reactions"`
}

type ChannelAnalytics struct {
	ChannelID      string  `json:"channel_id"`
	TotalThreads   int     `json:"total_threads"`
	ActiveThreads  int     `json:"active_threads"`
	AvgReplies     float64 `json:"avg_replies_per_thread"`
	ResolutionRate float64 `json:"resolution_rate"`
}

// ── Thread Export ──

type ThreadExportRequest struct {
	Format    string   `json:"format"` // json, csv, pdf, markdown
	ThreadIDs []string `json:"thread_ids"`
	IncludeReplies    bool `json:"include_replies"`
	IncludeReactions  bool `json:"include_reactions"`
	IncludeAttachments bool `json:"include_attachments"`
}

type ExportResult struct {
	ID         string    `json:"id"`
	Format     string    `json:"format"`
	Status     string    `json:"status"` // pending, processing, completed, failed
	URL        string    `json:"url"`
	Size       int64     `json:"size"`
	CreatedAt  time.Time `json:"created_at"`
	CompletedAt *time.Time `json:"completed_at"`
}

// ── Thread Merge/Split ──

type MergeRequest struct {
	SourceIDs   []string `json:"source_ids" binding:"required"`
	TargetID    string   `json:"target_id" binding:"required"`
}

type SplitRequest struct {
	ReplyID string `json:"reply_id" binding:"required"`
	NewTitle string `json:"new_title" binding:"required"`
}

// ── Thread Notification Preferences ──

type NotificationPreference struct {
	ID        string    `db:"id" json:"id"`
	ThreadID  string    `db:"thread_id" json:"thread_id"`
	UserID    string    `db:"user_id" json:"user_id"`
	Muted     bool      `db:"muted" json:"muted"`
	MuteUntil *time.Time `db:"mute_until" json:"mute_until"`
	Desktop   bool      `db:"desktop" json:"desktop"`
	Mobile    bool      `db:"mobile" json:"mobile"`
	Email     bool      `db:"email" json:"email"`
	CreatedAt time.Time `db:"created_at" json:"created_at"`
}

// ── Extended DTOs ──

type SetPermissionRequest struct {
	UserID    string `json:"user_id" binding:"required"`
	CanReply  bool   `json:"can_reply"`
	CanEdit   bool   `json:"can_edit"`
	CanDelete bool   `json:"can_delete"`
	CanPin    bool   `json:"can_pin"`
	CanLock   bool   `json:"can_lock"`
	CanInvite bool   `json:"can_invite"`
}

type ScheduleMessageRequest struct {
	Content     string `json:"content" binding:"required"`
	ScheduledAt string `json:"scheduled_at" binding:"required"`
}

type SavedReplyRequest struct {
	Title    string `json:"title" binding:"required"`
	Content  string `json:"content" binding:"required"`
	Shortcut string `json:"shortcut"`
}

type WatchRequest struct {
	NotifyOn string `json:"notify_on"` // all, mentions, none
}

type TagRequest struct {
	Tag string `json:"tag" binding:"required"`
}

type NotificationPrefRequest struct {
	Muted     *bool      `json:"muted"`
	MuteUntil *time.Time `json:"mute_until"`
	Desktop   *bool      `json:"desktop"`
	Mobile    *bool      `json:"mobile"`
	Email     *bool      `json:"email"`
}

type BulkActionRequest struct {
	ThreadIDs []string `json:"thread_ids" binding:"required"`
}

type BulkLabelRequest struct {
	ThreadIDs []string `json:"thread_ids" binding:"required"`
	LabelID   string   `json:"label_id" binding:"required"`
}

type BulkAssignRequest struct {
	ThreadIDs []string `json:"thread_ids" binding:"required"`
	UserID    string   `json:"user_id" binding:"required"`
}

type ReplyReactionRequest struct {
	ReplyID string `json:"reply_id" binding:"required"`
	Emoji   string `json:"emoji" binding:"required"`
}
