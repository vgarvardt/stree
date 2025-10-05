package models

import "time"

// Bookmark represents a saved S3 connection configuration
type Bookmark struct {
	ID           int64      `json:"id"`
	Title        string     `json:"title"`
	Endpoint     string     `json:"endpoint"`
	Region       string     `json:"region"`
	AccessKeyID  string     `json:"access_key_id"`
	SessionToken string     `json:"session_token,omitempty"`
	CreatedAt    time.Time  `json:"created_at"`
	UpdatedAt    time.Time  `json:"updated_at"`
	LastUsedAt   *time.Time `json:"last_used_at,omitempty"`
}

// Validate checks if the bookmark has required fields
func (b *Bookmark) Validate() error {
	if b.Title == "" {
		return ErrInvalidBookmark("title is required")
	}
	if b.Endpoint == "" {
		return ErrInvalidBookmark("endpoint is required")
	}
	if b.AccessKeyID == "" {
		return ErrInvalidBookmark("access key ID is required")
	}
	return nil
}

// ErrInvalidBookmark represents a bookmark validation error
type ErrInvalidBookmark string

func (e ErrInvalidBookmark) Error() string {
	return "invalid bookmark: " + string(e)
}
