package models

// Pagination represents pagination state for listing object versions and multipart uploads
type Pagination struct {
	IsTruncated         bool
	NextKeyMarker       string
	NextVersionIDMarker string
	NextUploadIDMarker  string
}
