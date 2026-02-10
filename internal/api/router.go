package api

import (
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"
)

func SetupRouter(db *sqlx.DB, h *Handler, logger *logrus.Logger) *gin.Engine {
	r := gin.New()
	r.Use(gin.Recovery())
	r.Use(requestLogger(logger))

	// Health
	r.GET("/health", func(c *gin.Context) {
		c.JSON(200, gin.H{"status": "healthy", "service": "thread-service", "time": time.Now().UTC()})
	})
	r.GET("/ready", func(c *gin.Context) {
		if err := db.Ping(); err != nil {
			c.JSON(503, gin.H{"status": "not ready", "error": "database unavailable"})
			return
		}
		c.JSON(200, gin.H{"status": "ready"})
	})

	api := r.Group("/api/v1")
	{
		threads := api.Group("/threads")
		{
			// Core CRUD
			threads.POST("", h.CreateThread)
			threads.GET("", h.ListThreads)
			threads.GET("/search", h.SearchThreads)
			threads.GET("/:id", h.GetThread)
			threads.PUT("/:id", h.UpdateThread)
			threads.DELETE("/:id", h.DeleteThread)
			threads.GET("/message/:messageId", h.GetThreadByMessage)
			threads.GET("/channel/:channelId", h.GetChannelThreads)
			threads.GET("/channel/:channelId/pinned", h.ListPinned)
			threads.GET("/channel/:channelId/stats", h.GetChannelStats)
			threads.GET("/channel/:channelId/recent", h.GetRecentlyActive)

			// Replies
			threads.GET("/:id/replies", h.GetReplies)
			threads.POST("/:id/replies", h.AddReply)
			threads.DELETE("/:id/replies/:replyId", h.DeleteReply)

			// Participants
			threads.GET("/:id/participants", h.GetParticipants)
			threads.POST("/:id/participants", h.AddParticipant)
			threads.DELETE("/:id/participants/:userId", h.RemoveParticipant)
			threads.PUT("/:id/participants/:userId/subscribe", h.UpdateSubscription)
			threads.POST("/:id/participants/:userId/read", h.MarkAsRead)

			// Labels on threads
			threads.GET("/:id/labels", h.GetThreadLabels)
			threads.POST("/:id/labels", h.AssignLabel)
			threads.DELETE("/:id/labels/:labelId", h.RemoveLabel)

			// Bookmarks
			threads.POST("/:id/bookmark", h.AddBookmark)
			threads.DELETE("/:id/bookmark", h.RemoveBookmark)

			// Reactions
			threads.GET("/:id/reactions", h.GetReactions)
			threads.GET("/:id/reactions/summary", h.GetReactionSummary)
			threads.POST("/:id/reactions", h.AddReaction)
			threads.DELETE("/:id/reactions", h.RemoveReaction)

			// Polls
			threads.GET("/:id/poll", h.GetPoll)
			threads.POST("/:id/poll", h.CreatePoll)
			threads.POST("/:id/poll/:pollId/vote", h.CastVote)
			threads.POST("/:id/poll/:pollId/close", h.ClosePoll)

			// Drafts
			threads.GET("/:id/draft", h.GetDraft)
			threads.PUT("/:id/draft", h.SaveDraft)
			threads.DELETE("/:id/draft", h.DeleteDraft)

			// Assignments
			threads.GET("/:id/assignments", h.ListAssignments)
			threads.POST("/:id/assignments", h.CreateAssignment)
			threads.PUT("/:id/assignments/:assignmentId", h.UpdateAssignment)
			threads.DELETE("/:id/assignments/:assignmentId", h.DeleteAssignment)

			// Activity log
			threads.GET("/:id/activity", h.GetActivity)

			// Attachments
			threads.GET("/:id/attachments", h.ListAttachments)

			// Follow-ups
			threads.POST("/:id/follow-up", h.CreateFollowUp)

			// Summaries
			threads.GET("/:id/summary", h.GetSummary)
			threads.POST("/:id/summary", h.CreateSummary)

			// Links
			threads.GET("/:id/links", h.GetLinks)
			threads.POST("/:id/links", h.LinkThread)
			threads.DELETE("/:id/links/:linkId", h.DeleteLink)
		}

		// Labels management
		labels := api.Group("/labels")
		{
			labels.POST("", h.CreateLabel)
			labels.GET("", h.ListLabels)
			labels.DELETE("/:labelId", h.DeleteLabel)
		}

		// Templates management
		templates := api.Group("/templates")
		{
			templates.POST("", h.CreateTemplate)
			templates.GET("", h.ListTemplates)
			templates.GET("/:templateId", h.GetTemplate)
			templates.PUT("/:templateId", h.UpdateTemplate)
			templates.DELETE("/:templateId", h.DeleteTemplate)
		}

		// Archive operations
		archives := api.Group("/archives")
		{
			archives.POST("", h.BulkArchive)
			archives.POST("/unarchive", h.BulkUnarchive)
		}

		// User-scoped endpoints
		users := api.Group("/users/:userId")
		{
			users.GET("/threads", h.GetUserThreads)
			users.GET("/bookmarks", h.ListBookmarks)
			users.GET("/drafts", h.ListDrafts)
			users.GET("/mentions", h.ListMentions)
			users.PUT("/mentions/:mentionId/read", h.MarkMentionRead)
			users.POST("/mentions/read-all", h.MarkAllMentionsRead)
			users.GET("/mentions/unread-count", h.CountUnreadMentions)
			users.GET("/assignments", h.ListMyAssignments)
			users.GET("/follow-ups", h.ListFollowUps)
			users.DELETE("/follow-ups/:followUpId", h.DeleteFollowUp)
		}

		// Stats
		api.GET("/stats", h.GetStats)
	}

	return r
}

func requestLogger(logger *logrus.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()
		logger.WithFields(logrus.Fields{
			"method":   c.Request.Method,
			"path":     c.Request.URL.Path,
			"status":   c.Writer.Status(),
			"duration": time.Since(start).Milliseconds(),
		}).Info("request")
	}
}
