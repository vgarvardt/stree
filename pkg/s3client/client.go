package s3client

import (
	"context"
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

// ListBuckets returns all S3 buckets
func (c *Client) ListBuckets(ctx context.Context) ([]models.Bucket, error) {
	output, err := c.s3Client.ListBuckets(ctx, &s3.ListBucketsInput{})
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

// ListObjects returns top-level objects in a bucket
func (c *Client) ListObjects(ctx context.Context, bucketName string) ([]models.Object, error) {
	output, err := c.s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:    aws.String(bucketName),
		Delimiter: aws.String("/"),
		MaxKeys:   aws.Int32(1000),
	})
	if err != nil {
		return nil, err
	}

	objects := make([]models.Object, 0)

	// Add common prefixes (folders)
	for _, prefix := range output.CommonPrefixes {
		objects = append(objects, models.Object{
			Key:      aws.ToString(prefix.Prefix),
			IsPrefix: true,
			Size:     0,
		})
	}

	// Add objects (files)
	for _, obj := range output.Contents {
		// Skip the bucket itself if it appears as an object
		key := aws.ToString(obj.Key)
		if key == "" || key == "/" {
			continue
		}

		object := models.Object{
			Key:      key,
			Size:     aws.ToInt64(obj.Size),
			IsPrefix: false,
		}
		if obj.LastModified != nil {
			lastModified := obj.LastModified.String()
			object.LastModified = &lastModified
		}
		objects = append(objects, object)
	}

	return objects, nil
}

// GetObjectType returns a simple categorization of the object
func GetObjectType(obj models.Object) string {
	if obj.IsPrefix {
		return "folder"
	}
	return "file"
}

// FormatSize formats the size in a human-readable format
func FormatSize(size int64) string {
	const unit = 1024
	if size < unit {
		return fmt.Sprintf("%d B", size)
	}
	div, exp := int64(unit), 0
	for n := size / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(size)/float64(div), "KMGTPE"[exp])
}

// ListObjectVersions lists all object versions in a bucket (including delete markers)
func (c *Client) ListObjectVersions(ctx context.Context, bucketName string) ([]models.ObjectVersion, error) {
	var versions []models.ObjectVersion
	var keyMarker *string
	var versionIDMarker *string

	for {
		input := &s3.ListObjectVersionsInput{
			Bucket:          aws.String(bucketName),
			KeyMarker:       keyMarker,
			VersionIdMarker: versionIDMarker,
			MaxKeys:         aws.Int32(1000),
		}

		output, err := c.s3Client.ListObjectVersions(ctx, input)
		if err != nil {
			return nil, fmt.Errorf("failed to list object versions: %w", err)
		}

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

		// Check if there are more results
		if !aws.ToBool(output.IsTruncated) {
			break
		}

		keyMarker = output.NextKeyMarker
		versionIDMarker = output.NextVersionIdMarker
	}

	slog.Info("Listed object versions", slog.String("bucket", bucketName), slog.Int("count", len(versions)))
	return versions, nil
}
