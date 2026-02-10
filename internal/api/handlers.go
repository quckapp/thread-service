package api

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/quckapp/thread-service/internal/models"
	"github.com/quckapp/thread-service/internal/service"
)

type Handler struct {
	svc *service.ThreadService
}

func NewHandler(svc *service.ThreadService) *Handler {
	return &Handler{svc: svc}
}

func (h *Handler) handleError(c *gin.Context, err error) {
	switch err {
	case service.ErrThreadNotFound:
		c.JSON(http.StatusNotFound, gin.H{"error": "thread not found"})
	case service.ErrReplyNotFound:
		c.JSON(http.StatusNotFound, gin.H{"error": "reply not found"})
	case service.ErrAlreadyExists:
		c.JSON(http.StatusConflict, gin.H{"error": "thread already exists for this message"})
	case service.ErrAlreadyParticipant:
		c.JSON(http.StatusConflict, gin.H{"error": "user is already a participant"})
	case service.ErrNotParticipant:
		c.JSON(http.StatusNotFound, gin.H{"error": "user is not a participant"})
	case service.ErrCannotRemoveCreator:
		c.JSON(http.StatusBadRequest, gin.H{"error": "cannot remove thread creator"})
	case service.ErrPollNotFound:
		c.JSON(http.StatusNotFound, gin.H{"error": "poll not found"})
	case service.ErrPollClosed:
		c.JSON(http.StatusBadRequest, gin.H{"error": "poll is closed"})
	case service.ErrAlreadyVoted:
		c.JSON(http.StatusConflict, gin.H{"error": "already voted"})
	case service.ErrLabelNotFound:
		c.JSON(http.StatusNotFound, gin.H{"error": "label not found"})
	case service.ErrBookmarkExists:
		c.JSON(http.StatusConflict, gin.H{"error": "bookmark already exists"})
	case service.ErrTemplateNotFound:
		c.JSON(http.StatusNotFound, gin.H{"error": "template not found"})
	case service.ErrAssignmentNotFound:
		c.JSON(http.StatusNotFound, gin.H{"error": "assignment not found"})
	case service.ErrThreadLocked:
		c.JSON(http.StatusForbidden, gin.H{"error": "thread is locked"})
	default:
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
	}
}

func getLimit(c *gin.Context, def int) int {
	l, _ := strconv.Atoi(c.DefaultQuery("limit", strconv.Itoa(def)))
	if l > 100 {
		l = 100
	}
	if l <= 0 {
		l = def
	}
	return l
}

func getOffset(c *gin.Context) int {
	o, _ := strconv.Atoi(c.DefaultQuery("offset", "0"))
	if o < 0 {
		o = 0
	}
	return o
}

// ── Thread CRUD ──

func (h *Handler) CreateThread(c *gin.Context) {
	var req models.CreateThreadRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	thread, err := h.svc.CreateThread(&req)
	if err != nil {
		h.handleError(c, err)
		return
	}
	c.JSON(http.StatusCreated, thread)
}

func (h *Handler) GetThread(c *gin.Context) {
	thread, err := h.svc.GetThread(c.Param("id"))
	if err != nil {
		h.handleError(c, err)
		return
	}
	c.JSON(http.StatusOK, thread)
}

func (h *Handler) GetThreadByMessage(c *gin.Context) {
	thread, err := h.svc.GetThreadByMessage(c.Param("messageId"))
	if err != nil {
		h.handleError(c, err)
		return
	}
	c.JSON(http.StatusOK, thread)
}

func (h *Handler) ListThreads(c *gin.Context) {
	limit := getLimit(c, 50)
	offset := getOffset(c)
	threads, total, err := h.svc.ListThreads(limit, offset)
	if err != nil {
		h.handleError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"threads": threads, "total": total, "limit": limit, "offset": offset})
}

func (h *Handler) GetChannelThreads(c *gin.Context) {
	channelID := c.Param("channelId")
	limit := getLimit(c, 50)
	offset := getOffset(c)
	includeResolved := c.DefaultQuery("include_resolved", "true") == "true"
	threads, total, err := h.svc.ListChannelThreads(channelID, includeResolved, limit, offset)
	if err != nil {
		h.handleError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"threads": threads, "total": total, "limit": limit, "offset": offset})
}

func (h *Handler) GetUserThreads(c *gin.Context) {
	userID := c.Param("userId")
	limit := getLimit(c, 50)
	offset := getOffset(c)
	subscribedOnly := c.DefaultQuery("subscribed_only", "false") == "true"
	threads, err := h.svc.ListUserThreads(userID, subscribedOnly, limit, offset)
	if err != nil {
		h.handleError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"threads": threads, "limit": limit, "offset": offset})
}

func (h *Handler) UpdateThread(c *gin.Context) {
	var req models.UpdateThreadRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	thread, err := h.svc.UpdateThread(c.Param("id"), &req)
	if err != nil {
		h.handleError(c, err)
		return
	}
	c.JSON(http.StatusOK, thread)
}

func (h *Handler) DeleteThread(c *gin.Context) {
	if err := h.svc.DeleteThread(c.Param("id")); err != nil {
		h.handleError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "thread deleted"})
}

// ── Replies ──

func (h *Handler) GetReplies(c *gin.Context) {
	limit := getLimit(c, 50)
	offset := getOffset(c)
	order := c.DefaultQuery("order", "asc")
	replies, total, err := h.svc.GetReplies(c.Param("id"), order, limit, offset)
	if err != nil {
		h.handleError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"replies": replies, "total": total, "limit": limit, "offset": offset})
}

func (h *Handler) AddReply(c *gin.Context) {
	var req models.AddReplyRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	reply, err := h.svc.AddReply(c.Param("id"), &req)
	if err != nil {
		h.handleError(c, err)
		return
	}
	c.JSON(http.StatusCreated, reply)
}

func (h *Handler) DeleteReply(c *gin.Context) {
	if err := h.svc.DeleteReply(c.Param("id"), c.Param("replyId")); err != nil {
		h.handleError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "reply deleted"})
}

// ── Participants ──

func (h *Handler) GetParticipants(c *gin.Context) {
	participants, err := h.svc.GetParticipants(c.Param("id"))
	if err != nil {
		h.handleError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"participants": participants})
}

func (h *Handler) AddParticipant(c *gin.Context) {
	var req models.AddParticipantRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	p, err := h.svc.AddParticipant(c.Param("id"), &req)
	if err != nil {
		h.handleError(c, err)
		return
	}
	c.JSON(http.StatusCreated, p)
}

func (h *Handler) RemoveParticipant(c *gin.Context) {
	if err := h.svc.RemoveParticipant(c.Param("id"), c.Param("userId")); err != nil {
		h.handleError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "participant removed"})
}

func (h *Handler) UpdateSubscription(c *gin.Context) {
	var req models.UpdateSubscriptionRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if err := h.svc.UpdateSubscription(c.Param("id"), c.Param("userId"), req.IsSubscribed); err != nil {
		h.handleError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "subscription updated", "is_subscribed": req.IsSubscribed})
}

func (h *Handler) MarkAsRead(c *gin.Context) {
	if err := h.svc.MarkAsRead(c.Param("id"), c.Param("userId")); err != nil {
		h.handleError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "marked as read"})
}

// ── Labels ──

func (h *Handler) CreateLabel(c *gin.Context) {
	var req models.CreateLabelRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	userID := c.Query("user_id")
	label, err := h.svc.CreateLabel(&req, userID)
	if err != nil {
		h.handleError(c, err)
		return
	}
	c.JSON(http.StatusCreated, label)
}

func (h *Handler) ListLabels(c *gin.Context) {
	channelID := c.Query("channel_id")
	labels, err := h.svc.ListLabels(channelID)
	if err != nil {
		h.handleError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"labels": labels})
}

func (h *Handler) DeleteLabel(c *gin.Context) {
	if err := h.svc.DeleteLabel(c.Param("labelId")); err != nil {
		h.handleError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "label deleted"})
}

func (h *Handler) AssignLabel(c *gin.Context) {
	var req models.AssignLabelRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	userID := c.Query("user_id")
	if err := h.svc.AssignLabel(c.Param("id"), req.LabelID, userID); err != nil {
		h.handleError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "label assigned"})
}

func (h *Handler) RemoveLabel(c *gin.Context) {
	if err := h.svc.RemoveLabel(c.Param("id"), c.Param("labelId")); err != nil {
		h.handleError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "label removed"})
}

func (h *Handler) GetThreadLabels(c *gin.Context) {
	labels, err := h.svc.GetThreadLabels(c.Param("id"))
	if err != nil {
		h.handleError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"labels": labels})
}

// ── Bookmarks ──

func (h *Handler) AddBookmark(c *gin.Context) {
	var req models.AddBookmarkRequest
	c.ShouldBindJSON(&req)
	userID := c.Query("user_id")
	bm, err := h.svc.AddBookmark(c.Param("id"), userID, req.Note)
	if err != nil {
		h.handleError(c, err)
		return
	}
	c.JSON(http.StatusCreated, bm)
}

func (h *Handler) RemoveBookmark(c *gin.Context) {
	userID := c.Query("user_id")
	if err := h.svc.RemoveBookmark(c.Param("id"), userID); err != nil {
		h.handleError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "bookmark removed"})
}

func (h *Handler) ListBookmarks(c *gin.Context) {
	userID := c.Param("userId")
	limit := getLimit(c, 50)
	offset := getOffset(c)
	bms, err := h.svc.ListBookmarks(userID, limit, offset)
	if err != nil {
		h.handleError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"bookmarks": bms})
}

// ── Reactions ──

func (h *Handler) AddReaction(c *gin.Context) {
	var req models.AddReactionRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	userID := c.Query("user_id")
	if err := h.svc.AddReaction(c.Param("id"), userID, &req); err != nil {
		h.handleError(c, err)
		return
	}
	c.JSON(http.StatusCreated, gin.H{"message": "reaction added"})
}

func (h *Handler) RemoveReaction(c *gin.Context) {
	var req models.RemoveReactionRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	userID := c.Query("user_id")
	if err := h.svc.RemoveReaction(c.Param("id"), userID, &req); err != nil {
		h.handleError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "reaction removed"})
}

func (h *Handler) GetReactions(c *gin.Context) {
	reactions, err := h.svc.GetReactions(c.Param("id"))
	if err != nil {
		h.handleError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"reactions": reactions})
}

func (h *Handler) GetReactionSummary(c *gin.Context) {
	summary, err := h.svc.GetReactionSummary(c.Param("id"))
	if err != nil {
		h.handleError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"summary": summary})
}

// ── Polls ──

func (h *Handler) CreatePoll(c *gin.Context) {
	var req models.CreatePollRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	userID := c.Query("user_id")
	poll, err := h.svc.CreatePoll(c.Param("id"), userID, &req)
	if err != nil {
		h.handleError(c, err)
		return
	}
	c.JSON(http.StatusCreated, poll)
}

func (h *Handler) GetPoll(c *gin.Context) {
	poll, options, results, err := h.svc.GetPoll(c.Param("id"))
	if err != nil {
		h.handleError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"poll": poll, "options": options, "results": results})
}

func (h *Handler) CastVote(c *gin.Context) {
	var req models.CastVoteRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	userID := c.Query("user_id")
	pollID := c.Param("pollId")
	if err := h.svc.CastVote(pollID, userID, req.OptionID); err != nil {
		h.handleError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "vote cast"})
}

func (h *Handler) ClosePoll(c *gin.Context) {
	if err := h.svc.ClosePoll(c.Param("pollId")); err != nil {
		h.handleError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "poll closed"})
}

// ── Drafts ──

func (h *Handler) SaveDraft(c *gin.Context) {
	var req models.SaveDraftRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	userID := c.Query("user_id")
	if err := h.svc.SaveDraft(c.Param("id"), userID, req.Content); err != nil {
		h.handleError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "draft saved"})
}

func (h *Handler) GetDraft(c *gin.Context) {
	userID := c.Query("user_id")
	draft, err := h.svc.GetDraft(c.Param("id"), userID)
	if err != nil {
		h.handleError(c, err)
		return
	}
	c.JSON(http.StatusOK, draft)
}

func (h *Handler) DeleteDraft(c *gin.Context) {
	userID := c.Query("user_id")
	if err := h.svc.DeleteDraft(c.Param("id"), userID); err != nil {
		h.handleError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "draft deleted"})
}

func (h *Handler) ListDrafts(c *gin.Context) {
	userID := c.Param("userId")
	drafts, err := h.svc.ListDrafts(userID)
	if err != nil {
		h.handleError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"drafts": drafts})
}

// ── Mentions ──

func (h *Handler) ListMentions(c *gin.Context) {
	userID := c.Param("userId")
	limit := getLimit(c, 50)
	offset := getOffset(c)
	unreadOnly := c.DefaultQuery("unread_only", "false") == "true"
	mentions, err := h.svc.ListMentions(userID, unreadOnly, limit, offset)
	if err != nil {
		h.handleError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"mentions": mentions})
}

func (h *Handler) MarkMentionRead(c *gin.Context) {
	if err := h.svc.MarkMentionRead(c.Param("mentionId")); err != nil {
		h.handleError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "mention marked as read"})
}

func (h *Handler) MarkAllMentionsRead(c *gin.Context) {
	userID := c.Param("userId")
	if err := h.svc.MarkAllMentionsRead(userID); err != nil {
		h.handleError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "all mentions marked as read"})
}

func (h *Handler) CountUnreadMentions(c *gin.Context) {
	userID := c.Param("userId")
	count, err := h.svc.CountUnreadMentions(userID)
	if err != nil {
		h.handleError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"unread_count": count})
}

// ── Assignments ──

func (h *Handler) CreateAssignment(c *gin.Context) {
	var req models.CreateAssignmentRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	assignedBy := c.Query("user_id")
	a, err := h.svc.CreateAssignment(c.Param("id"), assignedBy, &req)
	if err != nil {
		h.handleError(c, err)
		return
	}
	c.JSON(http.StatusCreated, a)
}

func (h *Handler) ListAssignments(c *gin.Context) {
	assignments, err := h.svc.ListAssignments(c.Param("id"))
	if err != nil {
		h.handleError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"assignments": assignments})
}

func (h *Handler) ListMyAssignments(c *gin.Context) {
	userID := c.Param("userId")
	status := c.Query("status")
	assignments, err := h.svc.ListMyAssignments(userID, status)
	if err != nil {
		h.handleError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"assignments": assignments})
}

func (h *Handler) UpdateAssignment(c *gin.Context) {
	var req models.UpdateAssignmentRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if err := h.svc.UpdateAssignment(c.Param("assignmentId"), &req); err != nil {
		h.handleError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "assignment updated"})
}

func (h *Handler) DeleteAssignment(c *gin.Context) {
	if err := h.svc.DeleteAssignment(c.Param("assignmentId")); err != nil {
		h.handleError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "assignment deleted"})
}

// ── Activity Log ──

func (h *Handler) GetActivity(c *gin.Context) {
	limit := getLimit(c, 50)
	offset := getOffset(c)
	activities, err := h.svc.GetActivity(c.Param("id"), limit, offset)
	if err != nil {
		h.handleError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"activities": activities})
}

// ── Attachments ──

func (h *Handler) ListAttachments(c *gin.Context) {
	attachments, err := h.svc.ListAttachments(c.Param("id"))
	if err != nil {
		h.handleError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"attachments": attachments})
}

// ── Templates ──

func (h *Handler) CreateTemplate(c *gin.Context) {
	var req models.CreateTemplateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	userID := c.Query("user_id")
	t, err := h.svc.CreateTemplate(&req, userID)
	if err != nil {
		h.handleError(c, err)
		return
	}
	c.JSON(http.StatusCreated, t)
}

func (h *Handler) GetTemplate(c *gin.Context) {
	t, err := h.svc.GetTemplate(c.Param("templateId"))
	if err != nil {
		h.handleError(c, err)
		return
	}
	c.JSON(http.StatusOK, t)
}

func (h *Handler) ListTemplates(c *gin.Context) {
	channelID := c.Query("channel_id")
	templates, err := h.svc.ListTemplates(channelID)
	if err != nil {
		h.handleError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"templates": templates})
}

func (h *Handler) UpdateTemplate(c *gin.Context) {
	var req models.UpdateTemplateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if err := h.svc.UpdateTemplate(c.Param("templateId"), &req); err != nil {
		h.handleError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "template updated"})
}

func (h *Handler) DeleteTemplate(c *gin.Context) {
	if err := h.svc.DeleteTemplate(c.Param("templateId")); err != nil {
		h.handleError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "template deleted"})
}

// ── Follow-ups ──

func (h *Handler) CreateFollowUp(c *gin.Context) {
	var req models.CreateFollowUpRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	userID := c.Query("user_id")
	f, err := h.svc.CreateFollowUp(c.Param("id"), userID, &req)
	if err != nil {
		h.handleError(c, err)
		return
	}
	c.JSON(http.StatusCreated, f)
}

func (h *Handler) ListFollowUps(c *gin.Context) {
	userID := c.Param("userId")
	followUps, err := h.svc.ListFollowUps(userID)
	if err != nil {
		h.handleError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"follow_ups": followUps})
}

func (h *Handler) DeleteFollowUp(c *gin.Context) {
	if err := h.svc.DeleteFollowUp(c.Param("followUpId")); err != nil {
		h.handleError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "follow-up deleted"})
}

// ── Summaries ──

func (h *Handler) CreateSummary(c *gin.Context) {
	var req models.CreateSummaryRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	userID := c.Query("user_id")
	s, err := h.svc.CreateSummary(c.Param("id"), userID, &req)
	if err != nil {
		h.handleError(c, err)
		return
	}
	c.JSON(http.StatusCreated, s)
}

func (h *Handler) GetSummary(c *gin.Context) {
	s, err := h.svc.GetSummary(c.Param("id"))
	if err != nil {
		h.handleError(c, err)
		return
	}
	c.JSON(http.StatusOK, s)
}

// ── Links ──

func (h *Handler) LinkThread(c *gin.Context) {
	var req models.LinkThreadRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	userID := c.Query("user_id")
	link, err := h.svc.LinkThread(c.Param("id"), userID, &req)
	if err != nil {
		h.handleError(c, err)
		return
	}
	c.JSON(http.StatusCreated, link)
}

func (h *Handler) GetLinks(c *gin.Context) {
	links, err := h.svc.GetLinks(c.Param("id"))
	if err != nil {
		h.handleError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"links": links})
}

func (h *Handler) DeleteLink(c *gin.Context) {
	if err := h.svc.DeleteLink(c.Param("linkId")); err != nil {
		h.handleError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "link deleted"})
}

// ── Search ──

func (h *Handler) SearchThreads(c *gin.Context) {
	var req models.SearchThreadsRequest
	if err := c.ShouldBindQuery(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	threads, total, err := h.svc.SearchThreads(&req)
	if err != nil {
		h.handleError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"threads": threads, "total": total})
}

// ── Pinned ──

func (h *Handler) ListPinned(c *gin.Context) {
	channelID := c.Param("channelId")
	threads, err := h.svc.ListPinned(channelID)
	if err != nil {
		h.handleError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"threads": threads})
}

// ── Archives ──

func (h *Handler) BulkArchive(c *gin.Context) {
	var req models.BulkArchiveRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if err := h.svc.BulkArchive(req.ThreadIDs); err != nil {
		h.handleError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "threads archived"})
}

func (h *Handler) BulkUnarchive(c *gin.Context) {
	var req models.BulkArchiveRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if err := h.svc.BulkUnarchive(req.ThreadIDs); err != nil {
		h.handleError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "threads unarchived"})
}

// ── Stats ──

func (h *Handler) GetStats(c *gin.Context) {
	stats, err := h.svc.GetStats()
	if err != nil {
		h.handleError(c, err)
		return
	}
	c.JSON(http.StatusOK, stats)
}

func (h *Handler) GetChannelStats(c *gin.Context) {
	stats, err := h.svc.GetChannelStats(c.Param("channelId"))
	if err != nil {
		h.handleError(c, err)
		return
	}
	c.JSON(http.StatusOK, stats)
}

func (h *Handler) GetRecentlyActive(c *gin.Context) {
	limit := getLimit(c, 20)
	threads, err := h.svc.GetRecentlyActive(c.Param("channelId"), limit)
	if err != nil {
		h.handleError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"threads": threads})
}
