package s3client

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	awsCredentials "github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3Types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/aws/smithy-go/logging"
	"github.com/cappuccinotm/slogx"

	"github.com/vgarvardt/stree/pkg/models"
)

// ClientLogModeDebug is a combination of all logging modes for debugging purposes
const ClientLogModeDebug = aws.LogSigning |
	aws.LogRetries |
	aws.LogRequest |
	aws.LogRequestWithBody |
	aws.LogResponse |
	aws.LogResponseWithBody |
	aws.LogDeprecatedUsage |
	aws.LogRequestEventMessage |
	aws.LogResponseEventMessage

// Client wraps the AWS S3 client
type Client struct {
	s3Client *s3.Client
}

// Config wraps AWS S3 client options
type Config struct {
	Endpoint     string
	AccessKey    string
	SecretKey    string
	SessionToken string
	Region       string
	RetryMode    aws.RetryMode

	Debug bool
}

// UseSSL returns true if the endpoint is using SSL
func (cfg Config) UseSSL() bool {
	return strings.HasPrefix(cfg.Endpoint, "https://")
}

// EndpointHost returns the host part of the endpoint
func (cfg Config) EndpointHost() string {
	u, err := url.Parse(cfg.Endpoint)
	if err != nil {
		return "invalid-host"
	}
	return u.Host
}

// String returns simple string representation of the config values
func (cfg Config) String() string {
	secretKey := cfg.SecretKey
	if secretKey != "" {
		secretKey = cfg.SecretKey[0:3] + "XXX" + cfg.SecretKey[len(cfg.SecretKey)-3:]
	}

	return fmt.Sprintf(
		"%s%s:%s@%s?region=%s",
		map[bool]string{true: "https://", false: "http://"}[cfg.UseSSL()],
		cfg.AccessKey,
		secretKey,
		cfg.EndpointHost(),
		cfg.Region,
	)
}

var logLevel = map[bool]slog.Level{true: slog.LevelDebug, false: slog.LevelInfo}

// NewClient creates a new S3 client with static credentials
func NewClient(ctx context.Context, cfg Config, version string) (*Client, error) {
	slog.Info("Building new S3 client", slog.String("s3-client-url", cfg.String()))

	var optFns []func(*config.LoadOptions) error

	if cfg.Debug {
		optFns = append(
			optFns,
			config.WithClientLogMode(ClientLogModeDebug),
		)
	}

	configOptions := make([]func(*config.LoadOptions) error, 0, 5+len(optFns))
	configOptions = append(configOptions,
		config.WithRegion(cfg.Region),
		config.WithCredentialsProvider(awsCredentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, cfg.SessionToken)),
		config.WithLogger(logging.LoggerFunc(func(classification logging.Classification, format string, v ...any) {
			switch classification {
			case logging.Warn:
				slog.Warn(fmt.Sprintf("[S3 Client WARN] "+format, v...))
			case logging.Debug:
				slog.Log(ctx, logLevel[cfg.Debug], fmt.Sprintf("[S3 CLient DEBUG] "+format, v...))
			}
		})),
		config.WithAppID("STree/"+version),
	)

	switch cfg.RetryMode {
	case "standard", "adaptive":
		configOptions = append(configOptions, config.WithRetryMode(cfg.RetryMode))
	case "nop", "":
		configOptions = append(configOptions,
			config.WithRetryer(func() aws.Retryer {
				return aws.NopRetryer{}
			}),
		)
	default:
		return nil, fmt.Errorf("unknown RetryMode, %v", cfg.RetryMode)
	}

	awsCfg, err := config.LoadDefaultConfig(ctx, append(configOptions, optFns...)...)
	if err != nil {
		return nil, fmt.Errorf("could not build aws config: %w", err)
	}

	return &Client{
		s3Client: s3.NewFromConfig(awsCfg, func(o *s3.Options) {
			o.BaseEndpoint = &cfg.Endpoint
			o.UsePathStyle = true
		}),
	}, nil
}

// GetBucketMetadata retrieves bucket metadata including versioning, lock, and retention settings
func (c *Client) GetBucketMetadata(ctx context.Context, bucketName string) (*models.BucketMetadata, error) {
	metadata := &models.BucketMetadata{}

	// Get versioning status
	versioningOutput, err := c.s3Client.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		slog.Warn("Could not get bucket versioning", slog.String("bucket", bucketName), slogx.Error(err))
	} else {
		metadata.VersioningStatus = string(versioningOutput.Status)
		metadata.VersioningEnabled = versioningOutput.Status == s3Types.BucketVersioningStatusEnabled
	}

	// Get object lock configuration
	lockOutput, err := c.s3Client.GetObjectLockConfiguration(ctx, &s3.GetObjectLockConfigurationInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		slog.Debug("Failed to get object lock configuration", slog.String("bucket", bucketName), slogx.Error(err))
	} else if lockOutput.ObjectLockConfiguration != nil {
		metadata.ObjectLockEnabled = lockOutput.ObjectLockConfiguration.ObjectLockEnabled == s3Types.ObjectLockEnabledEnabled
		if lockOutput.ObjectLockConfiguration.Rule != nil && lockOutput.ObjectLockConfiguration.Rule.DefaultRetention != nil {
			defaultRetention := lockOutput.ObjectLockConfiguration.Rule.DefaultRetention
			metadata.RetentionEnabled = true
			metadata.RetentionMode = string(defaultRetention.Mode)
			if defaultRetention.Days != nil {
				metadata.RetentionDays = *defaultRetention.Days
			}
			if defaultRetention.Years != nil {
				metadata.RetentionYears = *defaultRetention.Years
			}
		}
	}

	return metadata, nil
}

// GetBucketEncryption retrieves bucket encryption configuration.
// Returns nil if the bucket has no encryption configuration.
func (c *Client) GetBucketEncryption(ctx context.Context, bucketName string) (*models.BucketEncryption, error) {
	output, err := c.s3Client.GetBucketEncryption(ctx, &s3.GetBucketEncryptionInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		apiErr, ok := errors.AsType[smithy.APIError](err)
		if !ok {
			slog.Error("Could not get bucket encryption info", slog.String("bucket", bucketName), slogx.Error(err))
		}

		if apiErr.ErrorCode() == "ServerSideEncryptionConfigurationNotFoundError" {
			return nil, nil
		}

		return nil, err
	}

	return output.ServerSideEncryptionConfiguration, nil
}

// ListBuckets returns all S3 buckets
func (c *Client) ListBuckets(ctx context.Context, limit *int32) ([]models.Bucket, error) {
	output, err := c.s3Client.ListBuckets(ctx, &s3.ListBucketsInput{MaxBuckets: limit})
	if err != nil {
		return nil, err
	}

	buckets := make([]models.Bucket, 0, len(output.Buckets))
	for _, b := range output.Buckets {
		bucket := models.Bucket{
			Name: aws.ToString(b.Name),
		}
		bucket.CreationDate = time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC) // Default to Unix epoch start
		if b.CreationDate != nil {
			bucket.CreationDate = *b.CreationDate
		}
		buckets = append(buckets, bucket)
	}

	return buckets, nil
}

// ListObjectVersions lists object versions in a bucket with pagination support
// If pagination is nil, starts from the beginning. Returns versions and pagination state.
func (c *Client) ListObjectVersions(ctx context.Context, bucketName string, pagination *models.Pagination) ([]models.ObjectVersion, *models.Pagination, error) {
	var keyMarker *string
	var versionIDMarker *string

	// Use pagination state if provided
	if pagination != nil && pagination.IsTruncated {
		if pagination.NextKeyMarker != "" {
			keyMarker = aws.String(pagination.NextKeyMarker)
		}
		if pagination.NextVersionIDMarker != "" {
			versionIDMarker = aws.String(pagination.NextVersionIDMarker)
		}
	}

	input := &s3.ListObjectVersionsInput{
		Bucket:          aws.String(bucketName),
		KeyMarker:       keyMarker,
		VersionIdMarker: versionIDMarker,
		MaxKeys:         aws.Int32(1000),
	}

	output, err := c.s3Client.ListObjectVersions(ctx, input)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to list object versions: %w", err)
	}

	versions := make([]models.ObjectVersion, 0, len(output.Versions)+len(output.DeleteMarkers))

	// Process object versions
	for _, ver := range output.Versions {
		version := models.ObjectVersion{
			Key:            aws.ToString(ver.Key),
			VersionID:      aws.ToString(ver.VersionId),
			IsLatest:       aws.ToBool(ver.IsLatest),
			Size:           aws.ToInt64(ver.Size),
			LastModified:   aws.ToTime(ver.LastModified),
			IsDeleteMarker: false,
			ETag:           aws.ToString(ver.ETag),
			StorageClass:   string(ver.StorageClass),
		}
		versions = append(versions, version)
	}

	// Process delete markers
	for _, dm := range output.DeleteMarkers {
		version := models.ObjectVersion{
			Key:            aws.ToString(dm.Key),
			VersionID:      aws.ToString(dm.VersionId),
			IsLatest:       aws.ToBool(dm.IsLatest),
			Size:           0,
			LastModified:   aws.ToTime(dm.LastModified),
			IsDeleteMarker: true,
		}
		versions = append(versions, version)
	}

	// Build pagination response
	nextPagination := &models.Pagination{
		IsTruncated:         aws.ToBool(output.IsTruncated),
		NextKeyMarker:       aws.ToString(output.NextKeyMarker),
		NextVersionIDMarker: aws.ToString(output.NextVersionIdMarker),
	}

	return versions, nextPagination, nil
}

// DeleteError represents a single object that failed to delete.
type DeleteError struct {
	Key       string
	VersionID string
	Code      string
	Message   string
}

// DeleteResult holds the outcome of a DeleteObjects call.
type DeleteResult struct {
	Deleted int           // number of successfully deleted objects
	Errors  []DeleteError // per-object errors (objects the backend refused to delete)
}

// DeleteObjects deletes up to 1000 objects in a single batch request.
// Returns a DeleteResult and an error. The error is non-nil only for API-level failures
// (network, auth, etc.). Per-object failures are reported in DeleteResult.Errors.
func (c *Client) DeleteObjects(ctx context.Context, bucketName string, objects []models.ObjectVersion) (*DeleteResult, error) {
	if len(objects) == 0 {
		return &DeleteResult{}, nil
	}

	if len(objects) > 1000 {
		return nil, fmt.Errorf("cannot delete more than 1000 objects at once, got %d", len(objects))
	}

	deleteObjects := make([]s3Types.ObjectIdentifier, 0, len(objects))
	for _, obj := range objects {
		id := s3Types.ObjectIdentifier{
			Key: aws.String(obj.Key),
		}
		if obj.VersionID != "" {
			id.VersionId = aws.String(obj.VersionID)
		}
		deleteObjects = append(deleteObjects, id)
	}

	output, err := c.s3Client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket: aws.String(bucketName),
		Delete: &s3Types.Delete{
			Objects: deleteObjects,
			Quiet:   aws.Bool(true),
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to delete objects: %w", err)
	}

	result := &DeleteResult{
		Deleted: len(objects) - len(output.Errors),
	}

	for _, e := range output.Errors {
		result.Errors = append(result.Errors, DeleteError{
			Key:       aws.ToString(e.Key),
			VersionID: aws.ToString(e.VersionId),
			Code:      aws.ToString(e.Code),
			Message:   aws.ToString(e.Message),
		})
	}

	return result, nil
}

// ListMultipartUploads lists all uncompleted multipart uploads in a bucket with pagination support
// If pagination is nil, starts from the beginning. Returns uploads and pagination state.
func (c *Client) ListMultipartUploads(ctx context.Context, bucketName string, pagination *models.Pagination) ([]models.MultipartUpload, *models.Pagination, error) {
	var keyMarker *string
	var uploadIDMarker *string

	// Use pagination state if provided
	if pagination != nil && pagination.IsTruncated {
		if pagination.NextKeyMarker != "" {
			keyMarker = aws.String(pagination.NextKeyMarker)
		}
		if pagination.NextUploadIDMarker != "" {
			uploadIDMarker = aws.String(pagination.NextUploadIDMarker)
		}
	}

	input := &s3.ListMultipartUploadsInput{
		Bucket:         aws.String(bucketName),
		KeyMarker:      keyMarker,
		UploadIdMarker: uploadIDMarker,
		MaxUploads:     aws.Int32(1000),
	}

	output, err := c.s3Client.ListMultipartUploads(ctx, input)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to list multipart uploads: %w", err)
	}

	uploads := make([]models.MultipartUpload, 0, len(output.Uploads))
	for _, u := range output.Uploads {
		upload := models.MultipartUpload{
			Key:          aws.ToString(u.Key),
			UploadID:     aws.ToString(u.UploadId),
			StorageClass: string(u.StorageClass),
			Initiated:    aws.ToTime(u.Initiated),
		}
		if u.Initiator != nil {
			upload.Initiator = aws.ToString(u.Initiator.DisplayName)
		}
		if u.Owner != nil {
			upload.Owner = aws.ToString(u.Owner.DisplayName)
		}
		uploads = append(uploads, upload)
	}

	// Build pagination response
	nextPagination := &models.Pagination{
		IsTruncated:        aws.ToBool(output.IsTruncated),
		NextKeyMarker:      aws.ToString(output.NextKeyMarker),
		NextUploadIDMarker: aws.ToString(output.NextUploadIdMarker),
	}

	return uploads, nextPagination, nil
}

// ListParts lists all parts for a specific multipart upload
func (c *Client) ListParts(ctx context.Context, bucketName, key, uploadID string) ([]models.MultipartUploadPart, error) {
	var parts []models.MultipartUploadPart
	var partNumberMarker *string

	for {
		input := &s3.ListPartsInput{
			Bucket:           aws.String(bucketName),
			Key:              aws.String(key),
			UploadId:         aws.String(uploadID),
			PartNumberMarker: partNumberMarker,
			MaxParts:         aws.Int32(1000),
		}

		output, err := c.s3Client.ListParts(ctx, input)
		if err != nil {
			return nil, fmt.Errorf("failed to list parts for upload %s: %w", uploadID, err)
		}

		for _, p := range output.Parts {
			part := models.MultipartUploadPart{
				UploadID:     uploadID,
				PartNumber:   aws.ToInt32(p.PartNumber),
				Size:         aws.ToInt64(p.Size),
				ETag:         aws.ToString(p.ETag),
				LastModified: aws.ToTime(p.LastModified),
			}
			parts = append(parts, part)
		}

		if !aws.ToBool(output.IsTruncated) {
			break
		}
		partNumberMarker = output.NextPartNumberMarker
	}

	return parts, nil
}
