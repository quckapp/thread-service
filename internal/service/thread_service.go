package service

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/quckapp/thread-service/internal/models"
	"github.com/quckapp/thread-service/internal/repository"
	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
)

var (
	ErrThreadNotFound     = errors.New("thread not found")
	ErrReplyNotFound      = errors.New("reply not found")
	ErrAlreadyExists      = errors.New("thread already exists for this message")
	ErrAlreadyParticipant = errors.New("user is already a participant")
	ErrNotParticipant     = errors.New("user is not a participant")
	ErrCannotRemoveCreator = errors.New("cannot remove thread creator")
	ErrPollNotFound       = errors.New("poll not found")
	ErrPollClosed         = errors.New("poll is closed")
	ErrAlreadyVoted       = errors.New("already voted")
	ErrLabelNotFound      = errors.New("label not found")
	ErrBookmarkExists     = errors.New("bookmark already exists")
	ErrTemplateNotFound   = errors.New("template not found")
	ErrAssignmentNotFound = errors.New("assignment not found")
	ErrThreadLocked       = errors.New("thread is locked")
)

type ThreadService struct {
	threadRepo      *repository.ThreadRepository
	replyRepo       *repository.ReplyRepository
	participantRepo *repository.ParticipantRepository
	labelRepo       *repository.LabelRepository
	bookmarkRepo    *repository.BookmarkRepository
	reactionRepo    *repository.ReactionRepository
	pollRepo        *repository.PollRepository
	draftRepo       *repository.DraftRepository
	mentionRepo     *repository.MentionRepository
	assignmentRepo  *repository.AssignmentRepository
	activityRepo    *repository.ActivityRepository
	attachmentRepo  *repository.AttachmentRepository
	templateRepo    *repository.TemplateRepository
	followUpRepo    *repository.FollowUpRepository
	summaryRepo     *repository.SummaryRepository
	linkRepo        *repository.LinkRepository
	kafka           *kafka.Writer
	logger          *logrus.Logger
}

func NewThreadService(
	threadRepo *repository.ThreadRepository,
	replyRepo *repository.ReplyRepository,
	participantRepo *repository.ParticipantRepository,
	labelRepo *repository.LabelRepository,
	bookmarkRepo *repository.BookmarkRepository,
	reactionRepo *repository.ReactionRepository,
	pollRepo *repository.PollRepository,
	draftRepo *repository.DraftRepository,
	mentionRepo *repository.MentionRepository,
	assignmentRepo *repository.AssignmentRepository,
	activityRepo *repository.ActivityRepository,
	attachmentRepo *repository.AttachmentRepository,
	templateRepo *repository.TemplateRepository,
	followUpRepo *repository.FollowUpRepository,
	summaryRepo *repository.SummaryRepository,
	linkRepo *repository.LinkRepository,
	kafkaWriter *kafka.Writer,
	logger *logrus.Logger,
) *ThreadService {
	return &ThreadService{
		threadRepo:      threadRepo,
		replyRepo:       replyRepo,
		participantRepo: participantRepo,
		labelRepo:       labelRepo,
		bookmarkRepo:    bookmarkRepo,
		reactionRepo:    reactionRepo,
		pollRepo:        pollRepo,
		draftRepo:       draftRepo,
		mentionRepo:     mentionRepo,
		assignmentRepo:  assignmentRepo,
		activityRepo:    activityRepo,
		attachmentRepo:  attachmentRepo,
		templateRepo:    templateRepo,
		followUpRepo:    followUpRepo,
		summaryRepo:     summaryRepo,
		linkRepo:        linkRepo,
		kafka:           kafkaWriter,
		logger:          logger,
	}
}

// ── Thread CRUD ──

func (s *ThreadService) CreateThread(req *models.CreateThreadRequest) (*models.Thread, error) {
	existing, _ := s.threadRepo.GetByMessageID(req.ParentMessageID)
	if existing != nil {
		return nil, ErrAlreadyExists
	}

	thread := &models.Thread{
		ChannelID:       req.ChannelID,
		ParentMessageID: req.ParentMessageID,
		CreatedBy:       req.CreatedBy,
		Title:           req.Title,
		Priority:        req.Priority,
	}
	if err := s.threadRepo.Create(thread); err != nil {
		return nil, err
	}

	// Add creator as first participant
	p := &models.ThreadParticipant{
		ThreadID:     thread.ID,
		UserID:       req.CreatedBy,
		IsSubscribed: true,
	}
	s.participantRepo.Create(p)

	s.logActivity(thread.ID, req.CreatedBy, "thread_created", "")
	s.publishEvent("thread.created", thread.ID, thread.ChannelID, req.CreatedBy, map[string]interface{}{
		"parent_message_id": req.ParentMessageID,
	})

	return thread, nil
}

func (s *ThreadService) GetThread(id string) (*models.Thread, error) {
	thread, err := s.threadRepo.GetByID(id)
	if err != nil {
		return nil, err
	}
	if thread == nil {
		return nil, ErrThreadNotFound
	}
	return thread, nil
}

func (s *ThreadService) GetThreadByMessage(messageID string) (*models.Thread, error) {
	thread, err := s.threadRepo.GetByMessageID(messageID)
	if err != nil {
		return nil, err
	}
	if thread == nil {
		return nil, ErrThreadNotFound
	}
	return thread, nil
}

func (s *ThreadService) ListThreads(limit, offset int) ([]models.Thread, int, error) {
	return s.threadRepo.List(limit, offset)
}

func (s *ThreadService) ListChannelThreads(channelID string, includeResolved bool, limit, offset int) ([]models.Thread, int, error) {
	return s.threadRepo.ListByChannel(channelID, includeResolved, limit, offset)
}

func (s *ThreadService) ListUserThreads(userID string, subscribedOnly bool, limit, offset int) ([]models.Thread, error) {
	return s.threadRepo.ListByUser(userID, subscribedOnly, limit, offset)
}

func (s *ThreadService) UpdateThread(id string, req *models.UpdateThreadRequest) (*models.Thread, error) {
	thread, err := s.GetThread(id)
	if err != nil {
		return nil, err
	}

	updates := map[string]interface{}{}
	if req.IsResolved != nil {
		updates["is_resolved"] = *req.IsResolved
	}
	if req.IsPinned != nil {
		updates["is_pinned"] = *req.IsPinned
	}
	if req.IsLocked != nil {
		updates["is_locked"] = *req.IsLocked
	}
	if req.Title != "" {
		updates["title"] = req.Title
	}
	if req.Priority != "" {
		updates["priority"] = req.Priority
	}

	if len(updates) > 0 {
		if err := s.threadRepo.Update(id, updates); err != nil {
			return nil, err
		}
		if req.IsResolved != nil {
			eventType := "thread.resolved"
			if !*req.IsResolved {
				eventType = "thread.reopened"
			}
			s.publishEvent(eventType, id, thread.ChannelID, "", nil)
		}
	}

	return s.threadRepo.GetByID(id)
}

func (s *ThreadService) DeleteThread(id string) error {
	thread, err := s.GetThread(id)
	if err != nil {
		return err
	}
	if err := s.threadRepo.Delete(id); err != nil {
		return err
	}
	s.publishEvent("thread.deleted", id, thread.ChannelID, "", nil)
	return nil
}

// ── Replies ──

func (s *ThreadService) AddReply(threadID string, req *models.AddReplyRequest) (*models.ThreadReply, error) {
	thread, err := s.GetThread(threadID)
	if err != nil {
		return nil, err
	}
	if thread.IsLocked {
		return nil, ErrThreadLocked
	}

	reply := &models.ThreadReply{
		ThreadID:  threadID,
		MessageID: req.MessageID,
		UserID:    req.UserID,
		Content:   req.Content,
	}
	if err := s.replyRepo.Create(reply); err != nil {
		return nil, err
	}

	s.threadRepo.IncrementReplyCount(threadID, reply.CreatedAt, req.UserID)

	// Auto-join as participant
	exists, _ := s.participantRepo.Exists(threadID, req.UserID)
	if !exists {
		p := &models.ThreadParticipant{ThreadID: threadID, UserID: req.UserID, IsSubscribed: true}
		s.participantRepo.Create(p)
		s.threadRepo.IncrementParticipantCount(threadID)
	}
	s.participantRepo.MarkRead(threadID, req.UserID)

	s.publishEvent("thread.reply.added", threadID, thread.ChannelID, req.UserID, map[string]interface{}{
		"reply_id": reply.ID, "message_id": req.MessageID,
	})

	return reply, nil
}

func (s *ThreadService) GetReplies(threadID, order string, limit, offset int) ([]models.ThreadReply, int, error) {
	return s.replyRepo.ListByThread(threadID, order, limit, offset)
}

func (s *ThreadService) DeleteReply(threadID, replyID string) error {
	reply, err := s.replyRepo.GetByID(replyID)
	if err != nil || reply == nil {
		return ErrReplyNotFound
	}

	if err := s.replyRepo.Delete(replyID); err != nil {
		return err
	}
	s.threadRepo.DecrementReplyCount(threadID)

	lastReply, _ := s.replyRepo.GetLastReply(threadID)
	if lastReply == nil {
		s.threadRepo.SetLastReply(threadID, nil, nil)
	} else {
		s.threadRepo.SetLastReply(threadID, &lastReply.CreatedAt, &lastReply.UserID)
	}
	return nil
}

// ── Participants ──

func (s *ThreadService) GetParticipants(threadID string) ([]models.ThreadParticipant, error) {
	return s.participantRepo.ListByThread(threadID)
}

func (s *ThreadService) AddParticipant(threadID string, req *models.AddParticipantRequest) (*models.ThreadParticipant, error) {
	if _, err := s.GetThread(threadID); err != nil {
		return nil, err
	}
	exists, _ := s.participantRepo.Exists(threadID, req.UserID)
	if exists {
		return nil, ErrAlreadyParticipant
	}

	p := &models.ThreadParticipant{ThreadID: threadID, UserID: req.UserID, IsSubscribed: true}
	if err := s.participantRepo.Create(p); err != nil {
		return nil, err
	}
	s.threadRepo.IncrementParticipantCount(threadID)
	return p, nil
}

func (s *ThreadService) RemoveParticipant(threadID, userID string) error {
	thread, err := s.GetThread(threadID)
	if err != nil {
		return err
	}
	if thread.CreatedBy == userID {
		return ErrCannotRemoveCreator
	}
	if err := s.participantRepo.Delete(threadID, userID); err != nil {
		return err
	}
	s.threadRepo.DecrementParticipantCount(threadID)
	return nil
}

func (s *ThreadService) UpdateSubscription(threadID, userID string, isSubscribed bool) error {
	return s.participantRepo.UpdateSubscription(threadID, userID, isSubscribed)
}

func (s *ThreadService) MarkAsRead(threadID, userID string) error {
	return s.participantRepo.MarkRead(threadID, userID)
}

// ── Labels ──

func (s *ThreadService) CreateLabel(req *models.CreateLabelRequest, userID string) (*models.ThreadLabel, error) {
	label := &models.ThreadLabel{Name: req.Name, Color: req.Color, ChannelID: req.ChannelID, CreatedBy: userID}
	if err := s.labelRepo.Create(label); err != nil {
		return nil, err
	}
	return label, nil
}

func (s *ThreadService) ListLabels(channelID string) ([]models.ThreadLabel, error) {
	return s.labelRepo.ListByChannel(channelID)
}

func (s *ThreadService) DeleteLabel(id string) error {
	return s.labelRepo.Delete(id)
}

func (s *ThreadService) AssignLabel(threadID, labelID, userID string) error {
	if _, err := s.GetThread(threadID); err != nil {
		return err
	}
	label, err := s.labelRepo.GetByID(labelID)
	if err != nil || label == nil {
		return ErrLabelNotFound
	}
	return s.labelRepo.AssignToThread(threadID, labelID, userID)
}

func (s *ThreadService) RemoveLabel(threadID, labelID string) error {
	return s.labelRepo.RemoveFromThread(threadID, labelID)
}

func (s *ThreadService) GetThreadLabels(threadID string) ([]models.ThreadLabel, error) {
	return s.labelRepo.ListByThread(threadID)
}

// ── Bookmarks ──

func (s *ThreadService) AddBookmark(threadID, userID, note string) (*models.ThreadBookmark, error) {
	exists, _ := s.bookmarkRepo.Exists(threadID, userID)
	if exists {
		return nil, ErrBookmarkExists
	}
	bm := &models.ThreadBookmark{ThreadID: threadID, UserID: userID, Note: note}
	if err := s.bookmarkRepo.Create(bm); err != nil {
		return nil, err
	}
	return bm, nil
}

func (s *ThreadService) RemoveBookmark(threadID, userID string) error {
	return s.bookmarkRepo.Delete(threadID, userID)
}

func (s *ThreadService) ListBookmarks(userID string, limit, offset int) ([]models.ThreadBookmark, error) {
	return s.bookmarkRepo.ListByUser(userID, limit, offset)
}

// ── Reactions ──

func (s *ThreadService) AddReaction(threadID, userID string, req *models.AddReactionRequest) error {
	exists, _ := s.reactionRepo.Exists(threadID, req.ReplyID, userID, req.Emoji)
	if exists {
		return nil
	}
	reaction := &models.ThreadReaction{ThreadID: threadID, ReplyID: req.ReplyID, UserID: userID, Emoji: req.Emoji}
	return s.reactionRepo.Create(reaction)
}

func (s *ThreadService) RemoveReaction(threadID, userID string, req *models.RemoveReactionRequest) error {
	return s.reactionRepo.Delete(threadID, req.ReplyID, userID, req.Emoji)
}

func (s *ThreadService) GetReactions(threadID string) ([]models.ThreadReaction, error) {
	return s.reactionRepo.ListByThread(threadID)
}

func (s *ThreadService) GetReactionSummary(threadID string) ([]models.ReactionSummary, error) {
	return s.reactionRepo.GetSummary(threadID)
}

// ── Polls ──

func (s *ThreadService) CreatePoll(threadID, userID string, req *models.CreatePollRequest) (*models.ThreadPoll, error) {
	if _, err := s.GetThread(threadID); err != nil {
		return nil, err
	}

	var expiresAt *time.Time
	if req.ExpiresAt != "" {
		t, err := time.Parse(time.RFC3339, req.ExpiresAt)
		if err == nil {
			expiresAt = &t
		}
	}

	poll := &models.ThreadPoll{
		ThreadID:  threadID,
		Question:  req.Question,
		CreatedBy: userID,
		MultiVote: req.MultiVote,
		Anonymous: req.Anonymous,
		ExpiresAt: expiresAt,
	}
	if err := s.pollRepo.Create(poll); err != nil {
		return nil, err
	}

	for i, optText := range req.Options {
		opt := &models.PollOption{PollID: poll.ID, Text: optText, Position: i}
		s.pollRepo.CreateOption(opt)
	}

	return poll, nil
}

func (s *ThreadService) GetPoll(threadID string) (*models.ThreadPoll, []models.PollOption, []models.PollResult, error) {
	poll, err := s.pollRepo.GetByThread(threadID)
	if err != nil || poll == nil {
		return nil, nil, nil, ErrPollNotFound
	}
	options, _ := s.pollRepo.ListOptions(poll.ID)
	results, _ := s.pollRepo.GetResults(poll.ID)
	return poll, options, results, nil
}

func (s *ThreadService) CastVote(pollID, userID, optionID string) error {
	poll, err := s.pollRepo.GetByID(pollID)
	if err != nil || poll == nil {
		return ErrPollNotFound
	}
	if poll.IsClosed {
		return ErrPollClosed
	}
	if !poll.MultiVote {
		voted, _ := s.pollRepo.HasVoted(pollID, userID)
		if voted {
			return ErrAlreadyVoted
		}
	}
	return s.pollRepo.CastVote(pollID, optionID, userID)
}

func (s *ThreadService) ClosePoll(pollID string) error {
	return s.pollRepo.Close(pollID)
}

// ── Drafts ──

func (s *ThreadService) SaveDraft(threadID, userID, content string) error {
	draft := &models.ThreadDraft{ThreadID: threadID, UserID: userID, Content: content}
	return s.draftRepo.Save(draft)
}

func (s *ThreadService) GetDraft(threadID, userID string) (*models.ThreadDraft, error) {
	return s.draftRepo.Get(threadID, userID)
}

func (s *ThreadService) DeleteDraft(threadID, userID string) error {
	return s.draftRepo.Delete(threadID, userID)
}

func (s *ThreadService) ListDrafts(userID string) ([]models.ThreadDraft, error) {
	return s.draftRepo.ListByUser(userID)
}

// ── Mentions ──

func (s *ThreadService) ListMentions(userID string, unreadOnly bool, limit, offset int) ([]models.ThreadMention, error) {
	return s.mentionRepo.ListByUser(userID, unreadOnly, limit, offset)
}

func (s *ThreadService) MarkMentionRead(id string) error {
	return s.mentionRepo.MarkRead(id)
}

func (s *ThreadService) MarkAllMentionsRead(userID string) error {
	return s.mentionRepo.MarkAllRead(userID)
}

func (s *ThreadService) CountUnreadMentions(userID string) (int, error) {
	return s.mentionRepo.CountUnread(userID)
}

// ── Assignments ──

func (s *ThreadService) CreateAssignment(threadID, assignedBy string, req *models.CreateAssignmentRequest) (*models.ThreadAssignment, error) {
	if _, err := s.GetThread(threadID); err != nil {
		return nil, err
	}
	var dueDate *time.Time
	if req.DueDate != "" {
		t, err := time.Parse(time.RFC3339, req.DueDate)
		if err == nil {
			dueDate = &t
		}
	}
	a := &models.ThreadAssignment{ThreadID: threadID, AssigneeID: req.AssigneeID, AssignedBy: assignedBy, DueDate: dueDate}
	if err := s.assignmentRepo.Create(a); err != nil {
		return nil, err
	}
	s.logActivity(threadID, assignedBy, "assignment_created", req.AssigneeID)
	return a, nil
}

func (s *ThreadService) ListAssignments(threadID string) ([]models.ThreadAssignment, error) {
	return s.assignmentRepo.ListByThread(threadID)
}

func (s *ThreadService) ListMyAssignments(userID, status string) ([]models.ThreadAssignment, error) {
	return s.assignmentRepo.ListByAssignee(userID, status)
}

func (s *ThreadService) UpdateAssignment(id string, req *models.UpdateAssignmentRequest) error {
	a, err := s.assignmentRepo.GetByID(id)
	if err != nil || a == nil {
		return ErrAssignmentNotFound
	}
	status := a.Status
	if req.Status != "" {
		status = req.Status
	}
	var dueDate *time.Time
	if req.DueDate != "" {
		t, err := time.Parse(time.RFC3339, req.DueDate)
		if err == nil {
			dueDate = &t
		}
	} else {
		dueDate = a.DueDate
	}
	return s.assignmentRepo.Update(id, status, dueDate)
}

func (s *ThreadService) DeleteAssignment(id string) error {
	return s.assignmentRepo.Delete(id)
}

// ── Activity Log ──

func (s *ThreadService) GetActivity(threadID string, limit, offset int) ([]models.ThreadActivity, error) {
	return s.activityRepo.ListByThread(threadID, limit, offset)
}

// ── Attachments ──

func (s *ThreadService) ListAttachments(threadID string) ([]models.ThreadAttachment, error) {
	return s.attachmentRepo.ListByThread(threadID)
}

// ── Templates ──

func (s *ThreadService) CreateTemplate(req *models.CreateTemplateRequest, userID string) (*models.ThreadTemplate, error) {
	t := &models.ThreadTemplate{
		ChannelID: req.ChannelID, Name: req.Name, Description: req.Description,
		Content: req.Content, Labels: req.Labels, Priority: req.Priority, CreatedBy: userID,
	}
	if err := s.templateRepo.Create(t); err != nil {
		return nil, err
	}
	return t, nil
}

func (s *ThreadService) GetTemplate(id string) (*models.ThreadTemplate, error) {
	t, err := s.templateRepo.GetByID(id)
	if err != nil || t == nil {
		return nil, ErrTemplateNotFound
	}
	return t, nil
}

func (s *ThreadService) ListTemplates(channelID string) ([]models.ThreadTemplate, error) {
	return s.templateRepo.ListByChannel(channelID)
}

func (s *ThreadService) UpdateTemplate(id string, req *models.UpdateTemplateRequest) error {
	updates := map[string]interface{}{}
	if req.Name != "" {
		updates["name"] = req.Name
	}
	if req.Description != "" {
		updates["description"] = req.Description
	}
	if req.Content != "" {
		updates["content"] = req.Content
	}
	if req.Labels != "" {
		updates["labels"] = req.Labels
	}
	if req.Priority != "" {
		updates["priority"] = req.Priority
	}
	if len(updates) == 0 {
		return nil
	}
	return s.templateRepo.Update(id, updates)
}

func (s *ThreadService) DeleteTemplate(id string) error {
	return s.templateRepo.Delete(id)
}

// ── Follow-ups ──

func (s *ThreadService) CreateFollowUp(threadID, userID string, req *models.CreateFollowUpRequest) (*models.ThreadFollowUp, error) {
	remindAt, err := time.Parse(time.RFC3339, req.RemindAt)
	if err != nil {
		return nil, err
	}
	f := &models.ThreadFollowUp{ThreadID: threadID, UserID: userID, RemindAt: remindAt, Note: req.Note}
	if err := s.followUpRepo.Create(f); err != nil {
		return nil, err
	}
	return f, nil
}

func (s *ThreadService) ListFollowUps(userID string) ([]models.ThreadFollowUp, error) {
	return s.followUpRepo.ListByUser(userID)
}

func (s *ThreadService) DeleteFollowUp(id string) error {
	return s.followUpRepo.Delete(id)
}

// ── Summaries ──

func (s *ThreadService) CreateSummary(threadID, userID string, req *models.CreateSummaryRequest) (*models.ThreadSummary, error) {
	sum := &models.ThreadSummary{ThreadID: threadID, Summary: req.Summary, CreatedBy: userID}
	if err := s.summaryRepo.Create(sum); err != nil {
		return nil, err
	}
	return sum, nil
}

func (s *ThreadService) GetSummary(threadID string) (*models.ThreadSummary, error) {
	return s.summaryRepo.GetByThread(threadID)
}

// ── Links ──

func (s *ThreadService) LinkThread(sourceID, userID string, req *models.LinkThreadRequest) (*models.ThreadLink, error) {
	link := &models.ThreadLink{
		SourceThread: sourceID, TargetThread: req.TargetThreadID,
		LinkType: req.LinkType, CreatedBy: userID,
	}
	if err := s.linkRepo.Create(link); err != nil {
		return nil, err
	}
	return link, nil
}

func (s *ThreadService) GetLinks(threadID string) ([]models.ThreadLink, error) {
	return s.linkRepo.ListByThread(threadID)
}

func (s *ThreadService) DeleteLink(id string) error {
	return s.linkRepo.Delete(id)
}

// ── Search ──

func (s *ThreadService) SearchThreads(req *models.SearchThreadsRequest) ([]models.Thread, int, error) {
	return s.threadRepo.Search(req)
}

// ── Pinned ──

func (s *ThreadService) ListPinned(channelID string) ([]models.Thread, error) {
	return s.threadRepo.ListPinned(channelID)
}

// ── Archives ──

func (s *ThreadService) BulkArchive(threadIDs []string) error {
	return s.threadRepo.BulkArchive(threadIDs)
}

func (s *ThreadService) BulkUnarchive(threadIDs []string) error {
	return s.threadRepo.BulkUnarchive(threadIDs)
}

// ── Stats ──

func (s *ThreadService) GetStats() (*models.ThreadStats, error) {
	return s.threadRepo.GetStats()
}

func (s *ThreadService) GetChannelStats(channelID string) (*models.ChannelThreadStats, error) {
	return s.threadRepo.GetChannelStats(channelID)
}

func (s *ThreadService) GetRecentlyActive(channelID string, limit int) ([]models.Thread, error) {
	return s.threadRepo.GetRecentlyActive(channelID, limit)
}

// ── Helpers ──

func (s *ThreadService) logActivity(threadID, userID, action, details string) {
	a := &models.ThreadActivity{ThreadID: threadID, UserID: userID, Action: action, Details: details}
	s.activityRepo.Create(a)
}

func (s *ThreadService) publishEvent(eventType, threadID, channelID, userID string, data interface{}) {
	if s.kafka == nil {
		return
	}
	event := models.ThreadEvent{
		Type:      eventType,
		ThreadID:  threadID,
		ChannelID: channelID,
		UserID:    userID,
		Data:      data,
		Timestamp: time.Now(),
	}
	payload, err := json.Marshal(event)
	if err != nil {
		s.logger.Errorf("Failed to marshal event: %v", err)
		return
	}
	go func() {
		err := s.kafka.WriteMessages(context.Background(), kafka.Message{
			Key:   []byte(threadID),
			Value: payload,
			Headers: []kafka.Header{
				{Key: "event_type", Value: []byte(eventType)},
				{Key: "channel_id", Value: []byte(channelID)},
			},
		})
		if err != nil {
			s.logger.Errorf("Failed to publish event %s: %v", eventType, err)
		}
	}()
}
