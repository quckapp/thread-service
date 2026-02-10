package models

import "time"

// ── Core Models (existing) ──

type Thread struct {
	ID               string     `json:"id" db:"id"`
	ChannelID        string     `json:"channel_id" db:"channel_id"`
	ParentMessageID  string     `json:"parent_message_id" db:"parent_message_id"`
	CreatedBy        string     `json:"created_by" db:"created_by"`
	Title            string     `json:"title" db:"title"`
	ReplyCount       int        `json:"reply_count" db:"reply_count"`
	ParticipantCount int        `json:"participant_count" db:"participant_count"`
	LastReplyAt      *time.Time `json:"last_reply_at" db:"last_reply_at"`
	LastReplyBy      *string    `json:"last_reply_by" db:"last_reply_by"`
	IsResolved       bool       `json:"is_resolved" db:"is_resolved"`
	IsPinned         bool       `json:"is_pinned" db:"is_pinned"`
	IsLocked         bool       `json:"is_locked" db:"is_locked"`
	IsArchived       bool       `json:"is_archived" db:"is_archived"`
	Priority         string     `json:"priority" db:"priority"`
	CreatedAt        time.Time  `json:"created_at" db:"created_at"`
	UpdatedAt        time.Time  `json:"updated_at" db:"updated_at"`
}

type ThreadReply struct {
	ID        string    `json:"id" db:"id"`
	ThreadID  string    `json:"thread_id" db:"thread_id"`
	MessageID string    `json:"message_id" db:"message_id"`
	UserID    string    `json:"user_id" db:"user_id"`
	Content   string    `json:"content" db:"content"`
	CreatedAt time.Time `json:"created_at" db:"created_at"`
	UpdatedAt time.Time `json:"updated_at" db:"updated_at"`
}

type ThreadParticipant struct {
	ID           string     `json:"id" db:"id"`
	ThreadID     string     `json:"thread_id" db:"thread_id"`
	UserID       string     `json:"user_id" db:"user_id"`
	LastReadAt   *time.Time `json:"last_read_at" db:"last_read_at"`
	IsSubscribed bool       `json:"is_subscribed" db:"is_subscribed"`
	JoinedAt     time.Time  `json:"joined_at" db:"joined_at"`
}

type ThreadEvent struct {
	Type      string      `json:"type"`
	ThreadID  string      `json:"thread_id"`
	ChannelID string      `json:"channel_id"`
	UserID    string      `json:"user_id"`
	Data      interface{} `json:"data"`
	Timestamp time.Time   `json:"timestamp"`
}

// ── Labels ──

type ThreadLabel struct {
	ID        string    `json:"id" db:"id"`
	Name      string    `json:"name" db:"name"`
	Color     string    `json:"color" db:"color"`
	ChannelID string    `json:"channel_id" db:"channel_id"`
	CreatedBy string    `json:"created_by" db:"created_by"`
	CreatedAt time.Time `json:"created_at" db:"created_at"`
}

type ThreadLabelAssignment struct {
	ID        string    `json:"id" db:"id"`
	ThreadID  string    `json:"thread_id" db:"thread_id"`
	LabelID   string    `json:"label_id" db:"label_id"`
	AssignedBy string   `json:"assigned_by" db:"assigned_by"`
	AssignedAt time.Time `json:"assigned_at" db:"assigned_at"`
}

// ── Bookmarks ──

type ThreadBookmark struct {
	ID         string    `json:"id" db:"id"`
	ThreadID   string    `json:"thread_id" db:"thread_id"`
	UserID     string    `json:"user_id" db:"user_id"`
	Note       string    `json:"note" db:"note"`
	CreatedAt  time.Time `json:"created_at" db:"created_at"`
}

// ── Reactions ──

type ThreadReaction struct {
	ID        string    `json:"id" db:"id"`
	ThreadID  string    `json:"thread_id" db:"thread_id"`
	ReplyID   string    `json:"reply_id" db:"reply_id"`
	UserID    string    `json:"user_id" db:"user_id"`
	Emoji     string    `json:"emoji" db:"emoji"`
	CreatedAt time.Time `json:"created_at" db:"created_at"`
}

type ReactionSummary struct {
	Emoji string `json:"emoji" db:"emoji"`
	Count int    `json:"count" db:"count"`
}

// ── Polls ──

type ThreadPoll struct {
	ID         string     `json:"id" db:"id"`
	ThreadID   string     `json:"thread_id" db:"thread_id"`
	Question   string     `json:"question" db:"question"`
	CreatedBy  string     `json:"created_by" db:"created_by"`
	MultiVote  bool       `json:"multi_vote" db:"multi_vote"`
	Anonymous  bool       `json:"anonymous" db:"anonymous"`
	ExpiresAt  *time.Time `json:"expires_at" db:"expires_at"`
	IsClosed   bool       `json:"is_closed" db:"is_closed"`
	CreatedAt  time.Time  `json:"created_at" db:"created_at"`
}

type PollOption struct {
	ID     string `json:"id" db:"id"`
	PollID string `json:"poll_id" db:"poll_id"`
	Text   string `json:"text" db:"text"`
	Position int  `json:"position" db:"position"`
}

type PollVote struct {
	ID       string    `json:"id" db:"id"`
	PollID   string    `json:"poll_id" db:"poll_id"`
	OptionID string    `json:"option_id" db:"option_id"`
	UserID   string    `json:"user_id" db:"user_id"`
	VotedAt  time.Time `json:"voted_at" db:"voted_at"`
}

type PollResult struct {
	OptionID string `json:"option_id" db:"option_id"`
	Text     string `json:"text" db:"text"`
	Votes    int    `json:"votes" db:"votes"`
}

// ── Thread Drafts ──

type ThreadDraft struct {
	ID        string    `json:"id" db:"id"`
	ThreadID  string    `json:"thread_id" db:"thread_id"`
	UserID    string    `json:"user_id" db:"user_id"`
	Content   string    `json:"content" db:"content"`
	UpdatedAt time.Time `json:"updated_at" db:"updated_at"`
}

// ── Thread Mentions ──

type ThreadMention struct {
	ID            string    `json:"id" db:"id"`
	ThreadID      string    `json:"thread_id" db:"thread_id"`
	ReplyID       string    `json:"reply_id" db:"reply_id"`
	MentionedUser string    `json:"mentioned_user" db:"mentioned_user"`
	MentionedBy   string    `json:"mentioned_by" db:"mentioned_by"`
	IsRead        bool      `json:"is_read" db:"is_read"`
	CreatedAt     time.Time `json:"created_at" db:"created_at"`
}

// ── Thread Assignments ──

type ThreadAssignment struct {
	ID         string     `json:"id" db:"id"`
	ThreadID   string     `json:"thread_id" db:"thread_id"`
	AssigneeID string     `json:"assignee_id" db:"assignee_id"`
	AssignedBy string     `json:"assigned_by" db:"assigned_by"`
	Status     string     `json:"status" db:"status"`
	DueDate    *time.Time `json:"due_date" db:"due_date"`
	CreatedAt  time.Time  `json:"created_at" db:"created_at"`
	UpdatedAt  time.Time  `json:"updated_at" db:"updated_at"`
}

// ── Thread Activity / Audit Log ──

type ThreadActivity struct {
	ID        string    `json:"id" db:"id"`
	ThreadID  string    `json:"thread_id" db:"thread_id"`
	UserID    string    `json:"user_id" db:"user_id"`
	Action    string    `json:"action" db:"action"`
	Details   string    `json:"details" db:"details"`
	CreatedAt time.Time `json:"created_at" db:"created_at"`
}

// ── Thread Attachments ──

type ThreadAttachment struct {
	ID        string    `json:"id" db:"id"`
	ThreadID  string    `json:"thread_id" db:"thread_id"`
	ReplyID   string    `json:"reply_id" db:"reply_id"`
	UserID    string    `json:"user_id" db:"user_id"`
	FileName  string    `json:"file_name" db:"file_name"`
	FileSize  int64     `json:"file_size" db:"file_size"`
	MimeType  string    `json:"mime_type" db:"mime_type"`
	URL       string    `json:"url" db:"url"`
	CreatedAt time.Time `json:"created_at" db:"created_at"`
}

// ── Thread Templates ──

type ThreadTemplate struct {
	ID          string    `json:"id" db:"id"`
	ChannelID   string    `json:"channel_id" db:"channel_id"`
	Name        string    `json:"name" db:"name"`
	Description string    `json:"description" db:"description"`
	Content     string    `json:"content" db:"content"`
	Labels      string    `json:"labels" db:"labels"`
	Priority    string    `json:"priority" db:"priority"`
	CreatedBy   string    `json:"created_by" db:"created_by"`
	CreatedAt   time.Time `json:"created_at" db:"created_at"`
	UpdatedAt   time.Time `json:"updated_at" db:"updated_at"`
}

// ── Thread Follow-ups / Reminders ──

type ThreadFollowUp struct {
	ID        string    `json:"id" db:"id"`
	ThreadID  string    `json:"thread_id" db:"thread_id"`
	UserID    string    `json:"user_id" db:"user_id"`
	RemindAt  time.Time `json:"remind_at" db:"remind_at"`
	Note      string    `json:"note" db:"note"`
	IsTriggered bool    `json:"is_triggered" db:"is_triggered"`
	CreatedAt time.Time `json:"created_at" db:"created_at"`
}

// ── Thread Summaries ──

type ThreadSummary struct {
	ID        string    `json:"id" db:"id"`
	ThreadID  string    `json:"thread_id" db:"thread_id"`
	Summary   string    `json:"summary" db:"summary"`
	CreatedBy string    `json:"created_by" db:"created_by"`
	CreatedAt time.Time `json:"created_at" db:"created_at"`
}

// ── Thread Links (cross-referencing) ──

type ThreadLink struct {
	ID            string    `json:"id" db:"id"`
	SourceThread  string    `json:"source_thread" db:"source_thread"`
	TargetThread  string    `json:"target_thread" db:"target_thread"`
	LinkType      string    `json:"link_type" db:"link_type"`
	CreatedBy     string    `json:"created_by" db:"created_by"`
	CreatedAt     time.Time `json:"created_at" db:"created_at"`
}

// ── Unread tracking ──

type UnreadCount struct {
	ThreadID    string `json:"thread_id"`
	UnreadCount int    `json:"unread_count"`
}

// ── Stats ──

type ThreadStats struct {
	TotalThreads      int `json:"total_threads" db:"total_threads"`
	ActiveThreads     int `json:"active_threads" db:"active_threads"`
	ResolvedThreads   int `json:"resolved_threads" db:"resolved_threads"`
	TotalReplies      int `json:"total_replies" db:"total_replies"`
	TotalParticipants int `json:"total_participants" db:"total_participants"`
}

type ChannelThreadStats struct {
	ChannelID        string  `json:"channel_id" db:"channel_id"`
	ThreadCount      int     `json:"thread_count" db:"thread_count"`
	ActiveCount      int     `json:"active_count" db:"active_count"`
	AvgReplyCount    float64 `json:"avg_reply_count" db:"avg_reply_count"`
	AvgResolutionMin float64 `json:"avg_resolution_min" db:"avg_resolution_min"`
}

// ── Request DTOs ──

type CreateThreadRequest struct {
	ChannelID       string `json:"channel_id" binding:"required"`
	ParentMessageID string `json:"parent_message_id" binding:"required"`
	CreatedBy       string `json:"created_by" binding:"required"`
	Title           string `json:"title"`
	Priority        string `json:"priority"`
}

type UpdateThreadRequest struct {
	IsResolved *bool   `json:"is_resolved"`
	IsPinned   *bool   `json:"is_pinned"`
	IsLocked   *bool   `json:"is_locked"`
	Title      string  `json:"title"`
	Priority   string  `json:"priority"`
}

type AddReplyRequest struct {
	MessageID string `json:"message_id" binding:"required"`
	UserID    string `json:"user_id" binding:"required"`
	Content   string `json:"content"`
}

type AddParticipantRequest struct {
	UserID string `json:"user_id" binding:"required"`
}

type UpdateSubscriptionRequest struct {
	IsSubscribed bool `json:"is_subscribed"`
}

type CreateLabelRequest struct {
	Name      string `json:"name" binding:"required"`
	Color     string `json:"color" binding:"required"`
	ChannelID string `json:"channel_id" binding:"required"`
}

type AssignLabelRequest struct {
	LabelID string `json:"label_id" binding:"required"`
}

type AddBookmarkRequest struct {
	Note string `json:"note"`
}

type AddReactionRequest struct {
	Emoji   string `json:"emoji" binding:"required"`
	ReplyID string `json:"reply_id"`
}

type RemoveReactionRequest struct {
	Emoji   string `json:"emoji" binding:"required"`
	ReplyID string `json:"reply_id"`
}

type CreatePollRequest struct {
	Question  string   `json:"question" binding:"required"`
	Options   []string `json:"options" binding:"required"`
	MultiVote bool     `json:"multi_vote"`
	Anonymous bool     `json:"anonymous"`
	ExpiresAt string   `json:"expires_at"`
}

type CastVoteRequest struct {
	OptionID string `json:"option_id" binding:"required"`
}

type SaveDraftRequest struct {
	Content string `json:"content" binding:"required"`
}

type CreateAssignmentRequest struct {
	AssigneeID string `json:"assignee_id" binding:"required"`
	DueDate    string `json:"due_date"`
}

type UpdateAssignmentRequest struct {
	Status  string `json:"status"`
	DueDate string `json:"due_date"`
}

type CreateTemplateRequest struct {
	Name        string `json:"name" binding:"required"`
	Description string `json:"description"`
	Content     string `json:"content"`
	Labels      string `json:"labels"`
	Priority    string `json:"priority"`
	ChannelID   string `json:"channel_id" binding:"required"`
}

type UpdateTemplateRequest struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Content     string `json:"content"`
	Labels      string `json:"labels"`
	Priority    string `json:"priority"`
}

type CreateFollowUpRequest struct {
	RemindAt string `json:"remind_at" binding:"required"`
	Note     string `json:"note"`
}

type CreateSummaryRequest struct {
	Summary string `json:"summary" binding:"required"`
}

type LinkThreadRequest struct {
	TargetThreadID string `json:"target_thread_id" binding:"required"`
	LinkType       string `json:"link_type"`
}

type BulkArchiveRequest struct {
	ThreadIDs []string `json:"thread_ids" binding:"required"`
}

type SearchThreadsRequest struct {
	Query     string `json:"query" form:"query"`
	ChannelID string `json:"channel_id" form:"channel_id"`
	CreatedBy string `json:"created_by" form:"created_by"`
	IsResolved *bool  `json:"is_resolved" form:"is_resolved"`
	Priority  string `json:"priority" form:"priority"`
	LabelID   string `json:"label_id" form:"label_id"`
	SortBy    string `json:"sort_by" form:"sort_by"`
	Limit     int    `json:"limit" form:"limit"`
	Offset    int    `json:"offset" form:"offset"`
}

type MergeThreadRequest struct {
	SourceThreadID string `json:"source_thread_id" binding:"required"`
}

type ExportRequest struct {
	Format string `json:"format" form:"format"`
}
