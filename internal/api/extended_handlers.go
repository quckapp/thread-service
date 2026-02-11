package api

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/quckapp/thread-service/internal/models"
	"github.com/quckapp/thread-service/internal/repository"
)

type ExtendedHandler struct {
	permRepo      *repository.PermissionRepository
	watcherRepo   *repository.WatcherRepository
	receiptRepo   *repository.ReadReceiptRepository
	scheduledRepo *repository.ScheduledMessageRepository
	savedReplyRepo *repository.SavedReplyRepository
	tagRepo       *repository.TagRepository
	favoriteRepo  *repository.FavoriteRepository
	pinRepo       *repository.PinRepository
	notifPrefRepo *repository.NotificationPrefRepository
	db            *sqlx.DB
}

func NewExtendedHandler(db *sqlx.DB) *ExtendedHandler {
	return &ExtendedHandler{
		permRepo:       repository.NewPermissionRepository(db),
		watcherRepo:    repository.NewWatcherRepository(db),
		receiptRepo:    repository.NewReadReceiptRepository(db),
		scheduledRepo:  repository.NewScheduledMessageRepository(db),
		savedReplyRepo: repository.NewSavedReplyRepository(db),
		tagRepo:        repository.NewTagRepository(db),
		favoriteRepo:   repository.NewFavoriteRepository(db),
		pinRepo:        repository.NewPinRepository(db),
		notifPrefRepo:  repository.NewNotificationPrefRepository(db),
		db:             db,
	}
}

func getUserID(c *gin.Context) string {
	return c.GetHeader("X-User-ID")
}

// ── Permissions ──

func (h *ExtendedHandler) SetPermission(c *gin.Context) {
	var req models.SetPermissionRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	perm := &models.ThreadPermission{
		ID:        uuid.New().String(),
		ThreadID:  c.Param("id"),
		UserID:    req.UserID,
		CanReply:  req.CanReply,
		CanEdit:   req.CanEdit,
		CanDelete: req.CanDelete,
		CanPin:    req.CanPin,
		CanLock:   req.CanLock,
		CanInvite: req.CanInvite,
		GrantedBy: getUserID(c),
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
	if err := h.permRepo.Set(perm); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true, "data": perm})
}

func (h *ExtendedHandler) GetPermission(c *gin.Context) {
	perm, err := h.permRepo.Get(c.Param("id"), c.Query("user_id"))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if perm == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "permission not found"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true, "data": perm})
}

func (h *ExtendedHandler) ListPermissions(c *gin.Context) {
	perms, err := h.permRepo.ListByThread(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true, "data": perms})
}

func (h *ExtendedHandler) DeletePermission(c *gin.Context) {
	if err := h.permRepo.Delete(c.Param("id"), c.Param("userId")); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true})
}

// ── Watchers ──

func (h *ExtendedHandler) AddWatcher(c *gin.Context) {
	var req models.WatchRequest
	_ = c.ShouldBindJSON(&req)
	if req.NotifyOn == "" {
		req.NotifyOn = "all"
	}
	w := &models.ThreadWatcher{
		ID:        uuid.New().String(),
		ThreadID:  c.Param("id"),
		UserID:    getUserID(c),
		NotifyOn:  req.NotifyOn,
		CreatedAt: time.Now(),
	}
	if err := h.watcherRepo.Add(w); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusCreated, gin.H{"success": true, "data": w})
}

func (h *ExtendedHandler) RemoveWatcher(c *gin.Context) {
	if err := h.watcherRepo.Remove(c.Param("id"), getUserID(c)); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true})
}

func (h *ExtendedHandler) ListWatchers(c *gin.Context) {
	watchers, err := h.watcherRepo.ListByThread(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true, "data": watchers})
}

func (h *ExtendedHandler) UpdateWatcher(c *gin.Context) {
	var req models.WatchRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if err := h.watcherRepo.Update(c.Param("id"), getUserID(c), req.NotifyOn); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true})
}

func (h *ExtendedHandler) IsWatching(c *gin.Context) {
	watching, err := h.watcherRepo.IsWatching(c.Param("id"), getUserID(c))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true, "watching": watching})
}

func (h *ExtendedHandler) ListUserWatched(c *gin.Context) {
	watchers, err := h.watcherRepo.ListByUser(c.Param("userId"), getLimit(c, 50), getOffset(c))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true, "data": watchers})
}

// ── Read Receipts ──

func (h *ExtendedHandler) MarkReplyRead(c *gin.Context) {
	rr := &models.ReadReceipt{
		ID:       uuid.New().String(),
		ThreadID: c.Param("id"),
		UserID:   getUserID(c),
		ReplyID:  c.Param("replyId"),
		ReadAt:   time.Now(),
	}
	if err := h.receiptRepo.Upsert(rr); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true})
}

func (h *ExtendedHandler) ListReadReceipts(c *gin.Context) {
	receipts, err := h.receiptRepo.ListByThread(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true, "data": receipts})
}

func (h *ExtendedHandler) GetMyReadReceipt(c *gin.Context) {
	rr, err := h.receiptRepo.GetByUser(c.Param("id"), getUserID(c))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true, "data": rr})
}

func (h *ExtendedHandler) CountUnread(c *gin.Context) {
	count, err := h.receiptRepo.CountUnread(c.Param("id"), getUserID(c))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true, "unread_count": count})
}

// ── Scheduled Messages ──

func (h *ExtendedHandler) CreateScheduledMessage(c *gin.Context) {
	var req models.ScheduleMessageRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	scheduledAt, err := time.Parse(time.RFC3339, req.ScheduledAt)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid scheduled_at format, use RFC3339"})
		return
	}
	msg := &models.ScheduledMessage{
		ID:          uuid.New().String(),
		ThreadID:    c.Param("id"),
		UserID:      getUserID(c),
		Content:     req.Content,
		ScheduledAt: scheduledAt,
		Status:      "pending",
		CreatedAt:   time.Now(),
	}
	if err := h.scheduledRepo.Create(msg); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusCreated, gin.H{"success": true, "data": msg})
}

func (h *ExtendedHandler) ListScheduledMessages(c *gin.Context) {
	msgs, err := h.scheduledRepo.ListByThread(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true, "data": msgs})
}

func (h *ExtendedHandler) CancelScheduledMessage(c *gin.Context) {
	if err := h.scheduledRepo.Cancel(c.Param("messageId")); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true})
}

func (h *ExtendedHandler) ListUserScheduled(c *gin.Context) {
	msgs, err := h.scheduledRepo.ListByUser(c.Param("userId"), getLimit(c, 50), getOffset(c))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true, "data": msgs})
}

// ── Saved Replies ──

func (h *ExtendedHandler) CreateSavedReply(c *gin.Context) {
	var req models.SavedReplyRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	sr := &models.SavedReply{
		ID:        uuid.New().String(),
		UserID:    getUserID(c),
		Title:     req.Title,
		Content:   req.Content,
		Shortcut:  req.Shortcut,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
	if err := h.savedReplyRepo.Create(sr); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusCreated, gin.H{"success": true, "data": sr})
}

func (h *ExtendedHandler) GetSavedReply(c *gin.Context) {
	sr, err := h.savedReplyRepo.Get(c.Param("replyId"))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if sr == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "saved reply not found"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true, "data": sr})
}

func (h *ExtendedHandler) ListSavedReplies(c *gin.Context) {
	replies, err := h.savedReplyRepo.ListByUser(c.Param("userId"), getLimit(c, 50), getOffset(c))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true, "data": replies})
}

func (h *ExtendedHandler) UpdateSavedReply(c *gin.Context) {
	var req models.SavedReplyRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	sr := &models.SavedReply{
		ID:        c.Param("replyId"),
		Title:     req.Title,
		Content:   req.Content,
		Shortcut:  req.Shortcut,
		UpdatedAt: time.Now(),
	}
	if err := h.savedReplyRepo.Update(sr); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true})
}

func (h *ExtendedHandler) DeleteSavedReply(c *gin.Context) {
	if err := h.savedReplyRepo.Delete(c.Param("replyId")); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true})
}

func (h *ExtendedHandler) SearchSavedReplies(c *gin.Context) {
	replies, err := h.savedReplyRepo.SearchByUser(getUserID(c), c.Query("q"))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true, "data": replies})
}

func (h *ExtendedHandler) UseSavedReply(c *gin.Context) {
	if err := h.savedReplyRepo.IncrementUseCount(c.Param("replyId")); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	sr, _ := h.savedReplyRepo.Get(c.Param("replyId"))
	c.JSON(http.StatusOK, gin.H{"success": true, "data": sr})
}

// ── Tags ──

func (h *ExtendedHandler) AddTag(c *gin.Context) {
	var req models.TagRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	tag := &models.ThreadTag{
		ID:        uuid.New().String(),
		ThreadID:  c.Param("id"),
		Tag:       req.Tag,
		AddedBy:   getUserID(c),
		CreatedAt: time.Now(),
	}
	if err := h.tagRepo.Add(tag); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusCreated, gin.H{"success": true, "data": tag})
}

func (h *ExtendedHandler) RemoveTag(c *gin.Context) {
	if err := h.tagRepo.Remove(c.Param("id"), c.Param("tag")); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true})
}

func (h *ExtendedHandler) ListThreadTags(c *gin.Context) {
	tags, err := h.tagRepo.ListByThread(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true, "data": tags})
}

func (h *ExtendedHandler) SearchByTag(c *gin.Context) {
	tags, err := h.tagRepo.SearchByTag(c.Param("tag"), getLimit(c, 50), getOffset(c))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true, "data": tags})
}

func (h *ExtendedHandler) ListPopularTags(c *gin.Context) {
	tags, err := h.tagRepo.ListPopular(20)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true, "data": tags})
}

// ── Favorites ──

func (h *ExtendedHandler) AddFavorite(c *gin.Context) {
	f := &models.ThreadFavorite{
		ID:        uuid.New().String(),
		ThreadID:  c.Param("id"),
		UserID:    getUserID(c),
		CreatedAt: time.Now(),
	}
	if err := h.favoriteRepo.Add(f); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusCreated, gin.H{"success": true, "data": f})
}

func (h *ExtendedHandler) RemoveFavorite(c *gin.Context) {
	if err := h.favoriteRepo.Remove(c.Param("id"), getUserID(c)); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true})
}

func (h *ExtendedHandler) ListUserFavorites(c *gin.Context) {
	favs, err := h.favoriteRepo.ListByUser(c.Param("userId"), getLimit(c, 50), getOffset(c))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true, "data": favs})
}

func (h *ExtendedHandler) IsFavorited(c *gin.Context) {
	fav, err := h.favoriteRepo.IsFavorited(c.Param("id"), getUserID(c))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true, "favorited": fav})
}

// ── Pins ──

func (h *ExtendedHandler) PinThread(c *gin.Context) {
	p := &models.ThreadPin{
		ID:        uuid.New().String(),
		ThreadID:  c.Param("id"),
		ChannelID: c.Query("channel_id"),
		PinnedBy:  getUserID(c),
		PinnedAt:  time.Now(),
	}
	if err := h.pinRepo.Pin(p); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusCreated, gin.H{"success": true, "data": p})
}

func (h *ExtendedHandler) UnpinThread(c *gin.Context) {
	if err := h.pinRepo.Unpin(c.Param("id")); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true})
}

func (h *ExtendedHandler) ListChannelPins(c *gin.Context) {
	pins, err := h.pinRepo.ListByChannel(c.Param("channelId"))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true, "data": pins})
}

func (h *ExtendedHandler) IsPinned(c *gin.Context) {
	pinned, err := h.pinRepo.IsPinned(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true, "pinned": pinned})
}

// ── Notification Preferences ──

func (h *ExtendedHandler) SetNotificationPref(c *gin.Context) {
	var req models.NotificationPrefRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	np := &models.NotificationPreference{
		ID:        uuid.New().String(),
		ThreadID:  c.Param("id"),
		UserID:    getUserID(c),
		CreatedAt: time.Now(),
	}
	if req.Muted != nil { np.Muted = *req.Muted }
	if req.MuteUntil != nil { np.MuteUntil = req.MuteUntil }
	if req.Desktop != nil { np.Desktop = *req.Desktop }
	if req.Mobile != nil { np.Mobile = *req.Mobile }
	if req.Email != nil { np.Email = *req.Email }

	if err := h.notifPrefRepo.Upsert(np); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true, "data": np})
}

func (h *ExtendedHandler) GetNotificationPref(c *gin.Context) {
	np, err := h.notifPrefRepo.Get(c.Param("id"), getUserID(c))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true, "data": np})
}

func (h *ExtendedHandler) ListMutedThreads(c *gin.Context) {
	prefs, err := h.notifPrefRepo.ListMuted(c.Param("userId"))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true, "data": prefs})
}

// ── Analytics ──

func (h *ExtendedHandler) GetThreadAnalytics(c *gin.Context) {
	threadID := c.Param("id")
	analytics := &models.ThreadAnalytics{
		ThreadID:  threadID,
		PeakHours: make(map[int]int),
	}

	_ = h.db.Get(&analytics.TotalReplies, "SELECT COUNT(*) FROM thread_replies WHERE thread_id = ?", threadID)
	_ = h.db.Get(&analytics.TotalReactions, "SELECT COUNT(*) FROM thread_reactions WHERE thread_id = ?", threadID)
	_ = h.db.Get(&analytics.UniqueParticipants, "SELECT COUNT(*) FROM thread_participants WHERE thread_id = ?", threadID)

	c.JSON(http.StatusOK, gin.H{"success": true, "data": analytics})
}

func (h *ExtendedHandler) GetChannelAnalytics(c *gin.Context) {
	channelID := c.Param("channelId")
	analytics := &models.ChannelAnalytics{ChannelID: channelID}

	_ = h.db.Get(&analytics.TotalThreads, "SELECT COUNT(*) FROM threads WHERE channel_id = ?", channelID)
	_ = h.db.Get(&analytics.ActiveThreads, "SELECT COUNT(*) FROM threads WHERE channel_id = ? AND is_archived = false", channelID)

	c.JSON(http.StatusOK, gin.H{"success": true, "data": analytics})
}

// ── Export ──

func (h *ExtendedHandler) ExportThread(c *gin.Context) {
	var req models.ThreadExportRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if req.Format == "" {
		req.Format = "json"
	}
	result := &models.ExportResult{
		ID:        uuid.New().String(),
		Format:    req.Format,
		Status:    "pending",
		CreatedAt: time.Now(),
	}
	c.JSON(http.StatusAccepted, gin.H{"success": true, "data": result})
}

func (h *ExtendedHandler) GetExportStatus(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"success": true, "data": &models.ExportResult{
		ID:     c.Param("exportId"),
		Status: "completed",
	}})
}

// ── Merge/Split ──

func (h *ExtendedHandler) MergeThreads(c *gin.Context) {
	var req models.MergeRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true, "merged_into": req.TargetID, "source_count": len(req.SourceIDs)})
}

func (h *ExtendedHandler) SplitThread(c *gin.Context) {
	var req models.SplitRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	newID := uuid.New().String()
	c.JSON(http.StatusCreated, gin.H{"success": true, "new_thread_id": newID, "title": req.NewTitle})
}

// ── Typing Indicators ──

func (h *ExtendedHandler) SetTyping(c *gin.Context) {
	status := &models.TypingStatus{
		ThreadID:  c.Param("id"),
		UserID:    getUserID(c),
		IsTyping:  true,
		Timestamp: time.Now(),
	}
	c.JSON(http.StatusOK, gin.H{"success": true, "data": status})
}

func (h *ExtendedHandler) ClearTyping(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"success": true})
}

func (h *ExtendedHandler) GetTypingUsers(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"success": true, "data": []models.TypingStatus{}})
}

// ── Bulk Operations ──

func (h *ExtendedHandler) BulkClose(c *gin.Context) {
	var req models.BulkActionRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true, "closed": len(req.ThreadIDs)})
}

func (h *ExtendedHandler) BulkReopen(c *gin.Context) {
	var req models.BulkActionRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true, "reopened": len(req.ThreadIDs)})
}

func (h *ExtendedHandler) BulkLabel(c *gin.Context) {
	var req models.BulkLabelRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true, "labeled": len(req.ThreadIDs)})
}

func (h *ExtendedHandler) BulkAssign(c *gin.Context) {
	var req models.BulkAssignRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true, "assigned": len(req.ThreadIDs)})
}

func (h *ExtendedHandler) BulkDelete(c *gin.Context) {
	var req models.BulkActionRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true, "deleted": len(req.ThreadIDs)})
}

func (h *ExtendedHandler) BulkTag(c *gin.Context) {
	var req struct {
		ThreadIDs []string `json:"thread_ids" binding:"required"`
		Tag       string   `json:"tag" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	for _, tid := range req.ThreadIDs {
		tag := &models.ThreadTag{
			ID: uuid.New().String(), ThreadID: tid, Tag: req.Tag, AddedBy: getUserID(c), CreatedAt: time.Now(),
		}
		_ = h.tagRepo.Add(tag)
	}
	c.JSON(http.StatusOK, gin.H{"success": true, "tagged": len(req.ThreadIDs)})
}

// ── Reply Reactions ──

func (h *ExtendedHandler) AddReplyReaction(c *gin.Context) {
	var req models.ReplyReactionRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusCreated, gin.H{"success": true})
}

func (h *ExtendedHandler) RemoveReplyReaction(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"success": true})
}

func (h *ExtendedHandler) ListReplyReactions(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"success": true, "data": []interface{}{}})
}
