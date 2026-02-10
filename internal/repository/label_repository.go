package repository

import (
	"database/sql"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/quckapp/thread-service/internal/models"
)

type LabelRepository struct {
	db *sqlx.DB
}

func NewLabelRepository(db *sqlx.DB) *LabelRepository {
	return &LabelRepository{db: db}
}

func (r *LabelRepository) Create(label *models.ThreadLabel) error {
	label.ID = uuid.New().String()
	label.CreatedAt = time.Now()
	_, err := r.db.Exec(`INSERT INTO thread_labels (id, name, color, channel_id, created_by, created_at) VALUES (?, ?, ?, ?, ?, ?)`,
		label.ID, label.Name, label.Color, label.ChannelID, label.CreatedBy, label.CreatedAt)
	return err
}

func (r *LabelRepository) GetByID(id string) (*models.ThreadLabel, error) {
	var label models.ThreadLabel
	err := r.db.Get(&label, `SELECT * FROM thread_labels WHERE id = ?`, id)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return &label, err
}

func (r *LabelRepository) ListByChannel(channelID string) ([]models.ThreadLabel, error) {
	var labels []models.ThreadLabel
	err := r.db.Select(&labels, `SELECT * FROM thread_labels WHERE channel_id = ? ORDER BY name ASC`, channelID)
	return labels, err
}

func (r *LabelRepository) Delete(id string) error {
	_, err := r.db.Exec(`DELETE FROM thread_labels WHERE id = ?`, id)
	return err
}

func (r *LabelRepository) Update(id, name, color string) error {
	_, err := r.db.Exec(`UPDATE thread_labels SET name = ?, color = ? WHERE id = ?`, name, color, id)
	return err
}

func (r *LabelRepository) AssignToThread(threadID, labelID, assignedBy string) error {
	id := uuid.New().String()
	_, err := r.db.Exec(`INSERT INTO thread_label_assignments (id, thread_id, label_id, assigned_by, assigned_at) VALUES (?, ?, ?, ?, ?)`,
		id, threadID, labelID, assignedBy, time.Now())
	return err
}

func (r *LabelRepository) RemoveFromThread(threadID, labelID string) error {
	_, err := r.db.Exec(`DELETE FROM thread_label_assignments WHERE thread_id = ? AND label_id = ?`, threadID, labelID)
	return err
}

func (r *LabelRepository) ListByThread(threadID string) ([]models.ThreadLabel, error) {
	var labels []models.ThreadLabel
	err := r.db.Select(&labels, `SELECT tl.* FROM thread_labels tl INNER JOIN thread_label_assignments tla ON tl.id = tla.label_id WHERE tla.thread_id = ?`, threadID)
	return labels, err
}

func (r *LabelRepository) IsAssigned(threadID, labelID string) (bool, error) {
	var count int
	err := r.db.Get(&count, `SELECT COUNT(*) FROM thread_label_assignments WHERE thread_id = ? AND label_id = ?`, threadID, labelID)
	return count > 0, err
}
