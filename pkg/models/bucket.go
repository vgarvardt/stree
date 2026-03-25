package models

import (
	"time"

	s3Types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// BucketEncryption is an alias for the S3 server-side encryption configuration
type BucketEncryption = s3Types.ServerSideEncryptionConfiguration

// Bucket represents an S3 bucket
type Bucket struct {
	Name         string            `json:"name"`
	CreationDate time.Time         `json:"creation_date"`
	Encryption   *BucketEncryption `json:"encryption,omitempty"`
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

	ObjectsRefreshedAt *time.Time `json:"objects_refreshed_at,omitempty"`
	ObjectsCount       int64      `json:"objects_count"`
	ObjectsSize        int64      `json:"objects_size"`
	DeleteMarkersCount int64      `json:"delete_markers_count"`

	// Continuation state for resumable objects refresh
	ObjectsContinuation *ObjectsContinuation `json:"objects_continuation,omitempty"`

	// Multipart uploads stats
	MPUsRefreshedAt *time.Time `json:"mpus_refreshed_at,omitempty"`
	MPUsCount       int64      `json:"mpus_count"`
	MPUsTotalParts  int64      `json:"mpus_total_parts"`
	MPUsTotalSize   int64      `json:"mpus_total_size"`
}

// ObjectsContinuation holds state for resuming an interrupted objects refresh.
type ObjectsContinuation struct {
	NextKeyMarker       string `json:"next_key_marker"`
	NextVersionIDMarker string `json:"next_version_id_marker"`
	TotalCount          int64  `json:"total_count"`
	TotalSize           int64  `json:"total_size"`
	DeleteMarkerCount   int64  `json:"delete_marker_count"`
}

// BucketDetails represents bucket information stored in the database
// It embeds both Bucket and BucketMetadata to avoid field duplication
type BucketDetails struct {
	Bucket         `json:",inline"`
	BucketMetadata `json:",inline"`
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
	return bd.VersioningStatus != "" || bd.ObjectLockEnabled || bd.RetentionEnabled || bd.ObjectsRefreshedAt != nil || bd.MPUsRefreshedAt != nil
}
