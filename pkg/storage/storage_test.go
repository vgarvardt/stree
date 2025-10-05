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
	err = store.UpsertBucket(ctx, sessionID, "test-bucket", creationDate, bucketDetails)
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

	// Test object upsert
	objectProps := map[string]any{
		"Key":  "test-object.txt",
		"Size": 1024,
	}
	objectID, err := store.UpsertObject(ctx, bucket.ID, objectProps)
	require.NoError(t, err, "Failed to upsert object")
	assert.NotZero(t, objectID, "Expected non-zero object ID")

	// Test listing objects
	objects, err := store.GetObjectsByBucket(ctx, bucket.ID)
	require.NoError(t, err, "Failed to get objects")
	assert.Len(t, objects, 1, "Expected 1 object")

	// Verify object properties JSON
	var retrievedProps map[string]any
	err = json.Unmarshal(objects[0].Properties, &retrievedProps)
	require.NoError(t, err, "Failed to unmarshal object properties")
	assert.Equal(t, "test-object.txt", retrievedProps["Key"], "Object key should match")

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
