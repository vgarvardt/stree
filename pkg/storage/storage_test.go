package storage

import (
	"testing"
	"time"

	"github.com/goccy/go-json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/vgarvardt/stree/pkg/models"
)

func TestStorage(t *testing.T) {
	ctx := t.Context()

	// Create storage with in-memory database for testing
	store, err := New(ctx, Config{
		DSN:   ":memory:",
		Purge: false,
	})
	require.NoError(t, err, "Failed to create storage")
	t.Cleanup(func() {
		err := store.Close()
		require.NoError(t, err)
	})

	// Test session upsert
	configStr := "http://user:pass@localhost:9000?region=us-east-1"
	sessionID, err := store.UpsertSession(ctx, configStr)
	require.NoError(t, err, "Failed to upsert session")
	assert.NotZero(t, sessionID, "Expected non-zero session ID")

	// Test session retrieval
	session, err := store.GetSession(ctx, configStr)
	require.NoError(t, err, "Failed to get session")
	require.NotNil(t, session, "Expected session to exist")
	assert.Equal(t, configStr, session.ConfigStr, "Config string should match")

	// Test session update (should update timestamp)
	time.Sleep(10 * time.Millisecond)
	sessionID2, err := store.UpsertSession(ctx, configStr)
	require.NoError(t, err, "Failed to update session")
	assert.Equal(t, sessionID, sessionID2, "Session ID should remain the same on update")

	// Test bucket upsert
	creationDate := time.Now()
	bucketDetails := models.BucketDetails{
		Bucket: models.Bucket{
			Name:         "test-bucket",
			CreationDate: creationDate,
		},
		BucketMetadata: models.BucketMetadata{
			VersioningEnabled: true,
			ObjectLockEnabled: false,
		},
	}
	err = store.UpsertBucket(ctx, sessionID, "test-bucket", creationDate, bucketDetails, nil)
	require.NoError(t, err, "Failed to upsert bucket")

	// Test bucket retrieval
	bucket, err := store.GetBucket(ctx, sessionID, "test-bucket")
	require.NoError(t, err, "Failed to get bucket")
	require.NotNil(t, bucket, "Expected bucket to exist")
	assert.Equal(t, "test-bucket", bucket.Name, "Bucket name should match")
	assert.False(t, bucket.CreationDate.IsZero(), "Bucket creation date should not be zero")

	// Verify bucket details JSON
	var retrievedDetails models.BucketDetails
	err = json.Unmarshal(bucket.Details, &retrievedDetails)
	require.NoError(t, err, "Failed to unmarshal bucket details")
	assert.Equal(t, true, retrievedDetails.VersioningEnabled, "VersioningEnabled should be true")

	// Test listing buckets
	buckets, err := store.GetBucketsBySession(ctx, sessionID)
	require.NoError(t, err, "Failed to get buckets")
	assert.Len(t, buckets, 1, "Expected 1 bucket")

	// Test object insert
	now := time.Now()
	objectVersion := models.ObjectVersion{
		Key:          "test-object.txt",
		VersionID:    "v1",
		IsLatest:     true,
		Size:         1024,
		LastModified: now,
		ETag:         "abc123",
		StorageClass: "STANDARD",
	}
	err = store.InsertObject(ctx, bucket.ID, objectVersion)
	require.NoError(t, err, "Failed to insert object")

	// Test listing objects
	objects, err := store.GetObjectsByBucket(ctx, bucket.ID)
	require.NoError(t, err, "Failed to get objects")
	assert.Len(t, objects, 1, "Expected 1 object")
	assert.Equal(t, "test-object.txt", objects[0].Key, "Object key should match")
	assert.Equal(t, int64(1024), objects[0].Size, "Object size should match")
	assert.Equal(t, "v1", objects[0].VersionID, "Object version ID should match")
	assert.Equal(t, true, objects[0].IsLatest, "Object should be latest")

	// Test ListObjectsByBucket with filter
	f := false
	filteredObjects, err := store.ListObjectsByBucket(ctx, bucket.ID, ObjectListOptions{
		FilterDeleteMarker: &f,
		OrderBy:            OrderBySize,
		OrderDesc:          true,
		Limit:              10,
	})
	require.NoError(t, err, "Failed to list objects with filter")
	assert.Len(t, filteredObjects, 1, "Expected 1 non-delete-marker object")

	// Test BulkInsertObjectVersions
	bulkVersions := []models.ObjectVersion{
		{Key: "bulk-1.txt", VersionID: "", IsLatest: true, Size: 100, LastModified: now},
		{Key: "bulk-2.txt", VersionID: "", IsLatest: true, Size: 200, LastModified: now},
	}
	err = store.BulkInsertObjectVersions(ctx, bucket.ID, bulkVersions)
	require.NoError(t, err, "Failed to bulk insert objects")

	allObjects, err := store.GetObjectsByBucket(ctx, bucket.ID)
	require.NoError(t, err, "Failed to get all objects")
	assert.Len(t, allObjects, 3, "Expected 3 objects after bulk insert")

	// Test DeleteObjectsByBucketBatch
	deleted, err := store.DeleteObjectsByBucketBatch(ctx, bucket.ID, 2)
	require.NoError(t, err, "Failed to delete objects batch")
	assert.Equal(t, int64(2), deleted, "Expected 2 objects deleted")

	remaining, err := store.GetObjectsByBucket(ctx, bucket.ID)
	require.NoError(t, err, "Failed to get remaining objects")
	assert.Len(t, remaining, 1, "Expected 1 remaining object")

	// Test cascade delete
	err = store.DeleteSession(ctx, sessionID)
	require.NoError(t, err, "Failed to delete session")

	// Verify session is gone
	session, err = store.GetSession(ctx, configStr)
	require.NoError(t, err, "Failed to check deleted session")
	assert.Nil(t, session, "Expected session to be deleted")

	// Verify buckets are gone (cascade)
	buckets, err = store.GetBucketsBySession(ctx, sessionID)
	require.NoError(t, err, "Failed to check deleted buckets")
	assert.Empty(t, buckets, "Expected buckets to be deleted via cascade")
}

func TestMultipartUploads(t *testing.T) {
	ctx := t.Context()

	store, err := New(ctx, Config{DSN: ":memory:"})
	require.NoError(t, err)
	t.Cleanup(func() { store.Close() })

	// Set up session and bucket
	sessionID, err := store.UpsertSession(ctx, "test-config")
	require.NoError(t, err)

	now := time.Now()
	err = store.UpsertBucket(ctx, sessionID, "test-bucket", now, models.BucketDetails{}, nil)
	require.NoError(t, err)

	bucket, err := store.GetBucket(ctx, sessionID, "test-bucket")
	require.NoError(t, err)
	require.NotNil(t, bucket)

	// Test bulk insert uploads
	uploads := []models.MultipartUpload{
		{Key: "file1.dat", UploadID: "upload-1", Initiator: "user1", StorageClass: "STANDARD", Initiated: now},
		{Key: "file2.dat", UploadID: "upload-2", Initiator: "user1", StorageClass: "STANDARD", Initiated: now.Add(-time.Hour)},
	}
	err = store.BulkInsertMultipartUploads(ctx, bucket.ID, uploads)
	require.NoError(t, err)

	// Test bulk insert parts
	parts := []models.MultipartUploadPart{
		{UploadID: "upload-1", PartNumber: 1, Size: 5000, ETag: "etag1", LastModified: now},
		{UploadID: "upload-1", PartNumber: 2, Size: 3000, ETag: "etag2", LastModified: now},
	}
	err = store.BulkInsertMultipartUploadParts(ctx, bucket.ID, "upload-1", parts)
	require.NoError(t, err)

	// Test list with aggregation
	result, err := store.ListMultipartUploadsByBucket(ctx, bucket.ID, MPUListOptions{OrderDesc: true})
	require.NoError(t, err)
	assert.Len(t, result, 2)
	// Ordered by initiated DESC, so upload-1 (newer) should be first
	assert.Equal(t, "upload-1", result[0].UploadID)
	assert.Equal(t, int32(2), result[0].PartsCount)
	assert.Equal(t, int64(8000), result[0].TotalSize)
	assert.Equal(t, "upload-2", result[1].UploadID)
	assert.Equal(t, int32(0), result[1].PartsCount)

	// Test stats
	stats, err := store.GetMultipartUploadStats(ctx, bucket.ID)
	require.NoError(t, err)
	assert.Equal(t, int64(2), stats.UploadsCount)
	assert.Equal(t, int64(2), stats.TotalPartsCount)
	assert.Equal(t, int64(8000), stats.TotalPartsSize)

	// Test delete
	err = store.DeleteMultipartUploadsByBucket(ctx, bucket.ID)
	require.NoError(t, err)

	result, err = store.ListMultipartUploadsByBucket(ctx, bucket.ID, MPUListOptions{})
	require.NoError(t, err)
	assert.Empty(t, result)
}
