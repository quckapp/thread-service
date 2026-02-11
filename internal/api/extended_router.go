package api

import (
	"github.com/gin-gonic/gin"
	"github.com/jmoiron/sqlx"
)

func RegisterExtendedRoutes(r *gin.Engine, db *sqlx.DB) {
	eh := NewExtendedHandler(db)

	api := r.Group("/api/v1")
	{
		threads := api.Group("/threads")
		{
			// Permissions
			threads.POST("/:id/permissions", eh.SetPermission)
			threads.GET("/:id/permissions", eh.ListPermissions)
			threads.GET("/:id/permissions/check", eh.GetPermission)
			threads.DELETE("/:id/permissions/:userId", eh.DeletePermission)

			// Watchers
			threads.POST("/:id/watch", eh.AddWatcher)
			threads.DELETE("/:id/watch", eh.RemoveWatcher)
			threads.GET("/:id/watchers", eh.ListWatchers)
			threads.PUT("/:id/watch", eh.UpdateWatcher)
			threads.GET("/:id/watching", eh.IsWatching)

			// Read Receipts
			threads.POST("/:id/read/:replyId", eh.MarkReplyRead)
			threads.GET("/:id/read-receipts", eh.ListReadReceipts)
			threads.GET("/:id/my-read-receipt", eh.GetMyReadReceipt)
			threads.GET("/:id/unread-count", eh.CountUnread)

			// Scheduled Messages
			threads.POST("/:id/scheduled", eh.CreateScheduledMessage)
			threads.GET("/:id/scheduled", eh.ListScheduledMessages)
			threads.DELETE("/:id/scheduled/:messageId", eh.CancelScheduledMessage)

			// Tags
			threads.POST("/:id/tags", eh.AddTag)
			threads.DELETE("/:id/tags/:tag", eh.RemoveTag)
			threads.GET("/:id/tags", eh.ListThreadTags)

			// Favorites
			threads.POST("/:id/favorite", eh.AddFavorite)
			threads.DELETE("/:id/favorite", eh.RemoveFavorite)
			threads.GET("/:id/favorited", eh.IsFavorited)

			// Pins
			threads.POST("/:id/pin", eh.PinThread)
			threads.DELETE("/:id/pin", eh.UnpinThread)
			threads.GET("/:id/pinned", eh.IsPinned)

			// Notification Preferences
			threads.PUT("/:id/notifications", eh.SetNotificationPref)
			threads.GET("/:id/notifications", eh.GetNotificationPref)

			// Analytics
			threads.GET("/:id/analytics", eh.GetThreadAnalytics)

			// Export
			threads.POST("/:id/export", eh.ExportThread)
			threads.GET("/:id/export/:exportId", eh.GetExportStatus)

			// Merge/Split
			threads.POST("/:id/merge", eh.MergeThreads)
			threads.POST("/:id/split", eh.SplitThread)

			// Typing Indicators
			threads.POST("/:id/typing", eh.SetTyping)
			threads.DELETE("/:id/typing", eh.ClearTyping)
			threads.GET("/:id/typing", eh.GetTypingUsers)

			// Reply Reactions
			threads.POST("/:id/replies/:replyId/reactions", eh.AddReplyReaction)
			threads.DELETE("/:id/replies/:replyId/reactions", eh.RemoveReplyReaction)
			threads.GET("/:id/replies/:replyId/reactions", eh.ListReplyReactions)
		}

		// Tags management
		tags := api.Group("/tags")
		{
			tags.GET("/:tag/threads", eh.SearchByTag)
			tags.GET("/popular", eh.ListPopularTags)
		}

		// Channel-scoped
		channels := api.Group("/channels/:channelId")
		{
			channels.GET("/pins", eh.ListChannelPins)
			channels.GET("/analytics", eh.GetChannelAnalytics)
		}

		// Saved Replies
		savedReplies := api.Group("/saved-replies")
		{
			savedReplies.POST("", eh.CreateSavedReply)
			savedReplies.GET("/search", eh.SearchSavedReplies)
			savedReplies.GET("/:replyId", eh.GetSavedReply)
			savedReplies.PUT("/:replyId", eh.UpdateSavedReply)
			savedReplies.DELETE("/:replyId", eh.DeleteSavedReply)
			savedReplies.POST("/:replyId/use", eh.UseSavedReply)
		}

		// User-scoped extended
		users := api.Group("/users/:userId")
		{
			users.GET("/favorites", eh.ListUserFavorites)
			users.GET("/watched", eh.ListUserWatched)
			users.GET("/scheduled", eh.ListUserScheduled)
			users.GET("/saved-replies", eh.ListSavedReplies)
			users.GET("/muted", eh.ListMutedThreads)
		}

		// Bulk operations
		bulk := api.Group("/bulk")
		{
			bulk.POST("/close", eh.BulkClose)
			bulk.POST("/reopen", eh.BulkReopen)
			bulk.POST("/label", eh.BulkLabel)
			bulk.POST("/assign", eh.BulkAssign)
			bulk.POST("/delete", eh.BulkDelete)
			bulk.POST("/tag", eh.BulkTag)
		}
	}
}
