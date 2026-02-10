package repository

import (
	"database/sql"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/quckapp/thread-service/internal/models"
)

type BookmarkRepository struct {
	db *sqlx.DB
}

func NewBookmarkRepository(db *sqlx.DB) *BookmarkRepository {
	return &BookmarkRepository{db: db}
}

func (r *BookmarkRepository) Create(bm *models.ThreadBookmark) error {
	bm.ID = uuid.New().String()
	bm.CreatedAt = time.Now()
	_, err := r.db.Exec(`INSERT INTO thread_bookmarks (id, thread_id, user_id, note, created_at) VALUES (?, ?, ?, ?, ?)`,
		bm.ID, bm.ThreadID, bm.UserID, bm.Note, bm.CreatedAt)
	return err
}

func (r *BookmarkRepository) Delete(threadID, userID string) error {
	_, err := r.db.Exec(`DELETE FROM thread_bookmarks WHERE thread_id = ? AND user_id = ?`, threadID, userID)
	return err
}

func (r *BookmarkRepository) ListByUser(userID string, limit, offset int) ([]models.ThreadBookmark, error) {
	var bms []models.ThreadBookmark
	err := r.db.Select(&bms, `SELECT * FROM thread_bookmarks WHERE user_id = ? ORDER BY created_at DESC LIMIT ? OFFSET ?`, userID, limit, offset)
	return bms, err
}

func (r *BookmarkRepository) Exists(threadID, userID string) (bool, error) {
	var count int
	err := r.db.Get(&count, `SELECT COUNT(*) FROM thread_bookmarks WHERE thread_id = ? AND user_id = ?`, threadID, userID)
	return count > 0, err
}

func (r *BookmarkRepository) GetByID(id string) (*models.ThreadBookmark, error) {
	var bm models.ThreadBookmark
	err := r.db.Get(&bm, `SELECT * FROM thread_bookmarks WHERE id = ?`, id)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return &bm, err
}

func (r *BookmarkRepository) CountByUser(userID string) (int, error) {
	var count int
	err := r.db.Get(&count, `SELECT COUNT(*) FROM thread_bookmarks WHERE user_id = ?`, userID)
	return count, err
}
