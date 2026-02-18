package models

import "time"

// MultipartUpload represents an uncompleted multipart upload
type MultipartUpload struct {
	Key          string    `json:"key"`
	UploadID     string    `json:"upload_id"`
	Initiator    string    `json:"initiator,omitempty"`
	Owner        string    `json:"owner,omitempty"`
	StorageClass string    `json:"storage_class,omitempty"`
	Initiated    time.Time `json:"initiated"`
}

// MultipartUploadPart represents a part of a multipart upload
type MultipartUploadPart struct {
	UploadID     string    `json:"upload_id"`
	PartNumber   int32     `json:"part_number"`
	Size         int64     `json:"size"`
	ETag         string    `json:"etag,omitempty"`
	LastModified time.Time `json:"last_modified"`
}

// MultipartUploadStats represents aggregated statistics about multipart uploads
type MultipartUploadStats struct {
	UploadsCount    int64 `json:"uploads_count"`
	TotalPartsCount int64 `json:"total_parts_count"`
	TotalPartsSize  int64 `json:"total_parts_size"`
}

// MultipartUploadWithParts represents a multipart upload with its parts aggregated
type MultipartUploadWithParts struct {
	MultipartUpload
	PartsCount int32 `json:"parts_count"`
	TotalSize  int64 `json:"total_size"`
}
