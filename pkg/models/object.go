package models

import "time"

// ObjectVersion represents an S3 object version with detailed metadata
type ObjectVersion struct {
	Key            string    `json:"key"`
	VersionID      string    `json:"version_id"`
	IsLatest       bool      `json:"is_latest"`
	Size           int64     `json:"size"`
	LastModified   time.Time `json:"last_modified"`
	IsDeleteMarker bool      `json:"is_delete_marker"`
	ETag           string    `json:"etag,omitempty"`
	StorageClass   string    `json:"storage_class,omitempty"`
}
