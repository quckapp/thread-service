CREATE TABLE IF NOT EXISTS threads (
    id CHAR(36) PRIMARY KEY,
    channel_id CHAR(36) NOT NULL,
    parent_message_id CHAR(36) NOT NULL UNIQUE,
    created_by CHAR(36) NOT NULL,
    reply_count INT DEFAULT 0,
    participant_count INT DEFAULT 1,
    last_reply_at TIMESTAMP NULL,
    last_reply_by CHAR(36),
    is_resolved BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_threads_channel (channel_id),
    INDEX idx_threads_parent (parent_message_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS thread_replies (
    id CHAR(36) PRIMARY KEY,
    thread_id CHAR(36) NOT NULL,
    message_id CHAR(36) NOT NULL,
    user_id CHAR(36) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (thread_id) REFERENCES threads(id) ON DELETE CASCADE,
    INDEX idx_thread_replies_thread (thread_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS thread_participants (
    id CHAR(36) PRIMARY KEY,
    thread_id CHAR(36) NOT NULL,
    user_id CHAR(36) NOT NULL,
    last_read_at TIMESTAMP NULL,
    is_subscribed BOOLEAN DEFAULT TRUE,
    joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (thread_id) REFERENCES threads(id) ON DELETE CASCADE,
    UNIQUE KEY unique_participant (thread_id, user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
