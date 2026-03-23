package storage

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/vgarvardt/stree/pkg/models"
)

func TestSessionManager(t *testing.T) {
	baseDir := t.TempDir()

	mgr := NewSessionManager(baseDir)
	t.Cleanup(func() { mgr.Close() })

	// Test Open creates database and runs migrations
	bdb, err := mgr.Open(1, 100)
	require.NoError(t, err)
	require.NotNil(t, bdb)

	// Verify database file was created
	_, err = os.Stat(mgr.bucketDBPath(1, 100))
	require.NoError(t, err, "Database file should exist")

	// Test Open returns same instance on second call
	bdb2, err := mgr.Open(1, 100)
	require.NoError(t, err)
	assert.Same(t, bdb, bdb2, "Should return same BucketDB instance")

	// Test DeleteBucket removes file
	err = mgr.DeleteBucket(1, 100)
	require.NoError(t, err)

	_, err = os.Stat(mgr.bucketDBPath(1, 100))
	assert.True(t, os.IsNotExist(err), "Database file should be removed")

	// Test DeleteSession removes entire session directory
	_, err = mgr.Open(2, 200)
	require.NoError(t, err)
	_, err = mgr.Open(2, 201)
	require.NoError(t, err)

	err = mgr.DeleteSession(2)
	require.NoError(t, err)

	_, err = os.Stat(mgr.sessionDir(2))
	assert.True(t, os.IsNotExist(err), "Session directory should be removed")
}

func TestBucketDBObjects(t *testing.T) {
	ctx := t.Context()
	baseDir := t.TempDir()

	mgr := NewSessionManager(baseDir)
	t.Cleanup(func() { mgr.Close() })

	bdb, err := mgr.Open(1, 100)
	require.NoError(t, err)

	now := time.Now()

	// Test InsertObject
	obj := models.ObjectVersion{
		Key:          "test-object.txt",
		VersionID:    "v1",
		IsLatest:     true,
		Size:         1024,
		LastModified: now,
		ETag:         "abc123",
		StorageClass: "STANDARD",
	}
	err = bdb.InsertObject(ctx, obj)
	require.NoError(t, err)

	// Test GetObjects
	objects, err := bdb.GetObjects(ctx)
	require.NoError(t, err)
	assert.Len(t, objects, 1)
	assert.Equal(t, "test-object.txt", objects[0].Key)
	assert.Equal(t, int64(1024), objects[0].Size)
	assert.Equal(t, "v1", objects[0].VersionID)
	assert.True(t, objects[0].IsLatest)

	// Test ListObjects with filter
	f := false
	filtered, err := bdb.ListObjects(ctx, ObjectListOptions{
		FilterDeleteMarker: &f,
		OrderBy:            OrderBySize,
		OrderDesc:          true,
		Limit:              10,
	})
	require.NoError(t, err)
	assert.Len(t, filtered, 1)

	// Test BulkInsertObjectVersions
	bulkVersions := []models.ObjectVersion{
		{Key: "bulk-1.txt", VersionID: "", IsLatest: true, Size: 100, LastModified: now},
		{Key: "bulk-2.txt", VersionID: "", IsLatest: true, Size: 200, LastModified: now},
	}
	err = bdb.BulkInsertObjectVersions(ctx, bulkVersions)
	require.NoError(t, err)

	allObjects, err := bdb.GetObjects(ctx)
	require.NoError(t, err)
	assert.Len(t, allObjects, 3, "Expected 3 objects after bulk insert")

	// Test delete by removing bucket DB file, then re-opening (should be empty)
	err = mgr.DeleteBucket(1, 100)
	require.NoError(t, err)

	bdb, err = mgr.Open(1, 100)
	require.NoError(t, err)

	objects, err = bdb.GetObjects(ctx)
	require.NoError(t, err)
	assert.Empty(t, objects, "Expected no objects after deleting and recreating bucket DB")
}

func TestBucketDBMultipartUploads(t *testing.T) {
	ctx := t.Context()
	baseDir := t.TempDir()

	mgr := NewSessionManager(baseDir)
	t.Cleanup(func() { mgr.Close() })

	bdb, err := mgr.Open(1, 100)
	require.NoError(t, err)

	now := time.Now()

	// Test bulk insert uploads
	uploads := []models.MultipartUpload{
		{Key: "file1.dat", UploadID: "upload-1", Initiator: "user1", StorageClass: "STANDARD", Initiated: now},
		{Key: "file2.dat", UploadID: "upload-2", Initiator: "user1", StorageClass: "STANDARD", Initiated: now.Add(-time.Hour)},
	}
	err = bdb.BulkInsertMultipartUploads(ctx, uploads)
	require.NoError(t, err)

	// Test bulk insert parts
	parts := []models.MultipartUploadPart{
		{UploadID: "upload-1", PartNumber: 1, Size: 5000, ETag: "etag1", LastModified: now},
		{UploadID: "upload-1", PartNumber: 2, Size: 3000, ETag: "etag2", LastModified: now},
	}
	err = bdb.BulkInsertMultipartUploadParts(ctx, "upload-1", parts)
	require.NoError(t, err)

	// Test list with aggregation
	result, err := bdb.ListMultipartUploads(ctx, MPUListOptions{OrderDesc: true})
	require.NoError(t, err)
	assert.Len(t, result, 2)
	// Ordered by initiated DESC, so upload-1 (newer) should be first
	assert.Equal(t, "upload-1", result[0].UploadID)
	assert.Equal(t, int32(2), result[0].PartsCount)
	assert.Equal(t, int64(8000), result[0].TotalSize)
	assert.Equal(t, "upload-2", result[1].UploadID)
	assert.Equal(t, int32(0), result[1].PartsCount)

	// Test stats
	stats, err := bdb.GetMultipartUploadStats(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(2), stats.UploadsCount)
	assert.Equal(t, int64(2), stats.TotalPartsCount)
	assert.Equal(t, int64(8000), stats.TotalPartsSize)

	// Test delete multipart uploads
	err = bdb.DeleteMultipartUploads(ctx)
	require.NoError(t, err)

	result, err = bdb.ListMultipartUploads(ctx, MPUListOptions{})
	require.NoError(t, err)
	assert.Empty(t, result)
}
