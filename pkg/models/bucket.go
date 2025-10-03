package models

import "time"

// Bucket represents an S3 bucket
type Bucket struct {
	Name         string    `json:"name"`
	CreationDate time.Time `json:"creation_date"`
}

// BucketMetadata represents S3 bucket metadata and configuration
type BucketMetadata struct {
	VersioningEnabled bool   `json:"versioning_enabled"`
	VersioningStatus  string `json:"versioning_status"`
	ObjectLockEnabled bool   `json:"object_lock_enabled"`
	ObjectLockMode    string `json:"object_lock_mode"`
	RetentionEnabled  bool   `json:"retention_enabled"`
	RetentionDays     int32  `json:"retention_days"`
	RetentionYears    int32  `json:"retention_years"`
	RetentionMode     string `json:"retention_mode"`
}

// BucketDetails represents bucket information stored in the database
// It embeds both Bucket and BucketMetadata to avoid field duplication
type BucketDetails struct {
	Bucket         `json:",inline"`
	BucketMetadata `json:",inline"`
}

// Object represents an S3 object
type Object struct {
	Key          string  `json:"key"`
	Size         int64   `json:"size"`
	IsPrefix     bool    `json:"is_prefix"`
	LastModified *string `json:"last_modified,omitempty"`
}

// NewBucketDetails creates a BucketDetails from a Bucket and its metadata
func NewBucketDetails(bucket Bucket, metadata *BucketMetadata) BucketDetails {
	details := BucketDetails{
		Bucket: bucket,
	}

	if metadata != nil {
		details.BucketMetadata = *metadata
	}

	return details
}

// ToMetadata extracts BucketMetadata from BucketDetails
func (bd BucketDetails) ToMetadata() *BucketMetadata {
	return &bd.BucketMetadata
}

// HasMetadata checks if the bucket details contain metadata (not just basic info)
func (bd BucketDetails) HasMetadata() bool {
	return bd.VersioningStatus != "" || bd.ObjectLockEnabled || bd.RetentionEnabled
}
