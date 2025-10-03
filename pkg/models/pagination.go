package models

// Pagination represents pagination state for listing object versions
type Pagination struct {
	IsTruncated         bool
	NextKeyMarker       string
	NextVersionIDMarker string
}
