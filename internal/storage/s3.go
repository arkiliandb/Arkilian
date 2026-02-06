package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// S3Storage implements ObjectStorage for AWS S3.
type S3Storage struct {
	client     *s3.Client
	bucket     string
	config     S3Config
	maxRetries int
}

// S3Config holds configuration for S3 storage.
type S3Config struct {
	// Region is the AWS region for the S3 bucket.
	Region string
	// Endpoint is an optional custom endpoint (for MinIO, LocalStack, etc.).
	Endpoint string
	// UsePathStyle enables path-style addressing (required for MinIO).
	UsePathStyle bool
	// MultipartConfig holds multipart upload settings.
	MultipartConfig MultipartUploadConfig
}

// DefaultS3Config returns the default S3 configuration.
func DefaultS3Config() S3Config {
	return S3Config{
		Region:          "us-east-1",
		MultipartConfig: DefaultMultipartConfig(),
	}
}

// NewS3Storage creates a new S3 storage client.
func NewS3Storage(ctx context.Context, bucket string, cfg S3Config) (*S3Storage, error) {
	var opts []func(*config.LoadOptions) error

	if cfg.Region != "" {
		opts = append(opts, config.WithRegion(cfg.Region))
	}

	awsCfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	var s3Opts []func(*s3.Options)
	if cfg.Endpoint != "" {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		})
	}
	if cfg.UsePathStyle {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.UsePathStyle = true
		})
	}

	client := s3.NewFromConfig(awsCfg, s3Opts...)

	return &S3Storage{
		client:     client,
		bucket:     bucket,
		config:     cfg,
		maxRetries: 3,
	}, nil
}

// NewS3StorageWithClient creates a new S3 storage with a pre-configured client.
func NewS3StorageWithClient(client *s3.Client, bucket string, cfg S3Config) *S3Storage {
	return &S3Storage{
		client:     client,
		bucket:     bucket,
		config:     cfg,
		maxRetries: 3,
	}
}

// Upload uploads a file to S3.
func (s *S3Storage) Upload(ctx context.Context, localPath, objectPath string) error {
	file, err := os.Open(localPath)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrUploadFailed, err)
	}
	defer file.Close()

	return s.retryWithBackoff(ctx, func() error {
		// Reset file position for retry
		if _, err := file.Seek(0, io.SeekStart); err != nil {
			return err
		}

		_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(s.bucket),
			Key:    aws.String(objectPath),
			Body:   file,
		})
		return err
	})
}


// UploadMultipart uploads a file using multipart upload with ETag validation.
func (s *S3Storage) UploadMultipart(ctx context.Context, localPath, objectPath string) (string, error) {
	file, err := os.Open(localPath)
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrUploadFailed, err)
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrUploadFailed, err)
	}

	fileSize := stat.Size()
	partSize := s.config.MultipartConfig.PartSize

	// If file is small enough, use simple upload
	if fileSize <= partSize {
		if err := s.Upload(ctx, localPath, objectPath); err != nil {
			return "", err
		}
		// Get the ETag after upload
		return s.getETag(ctx, objectPath)
	}

	var etag string
	err = s.retryWithBackoff(ctx, func() error {
		// Reset file position for retry
		if _, err := file.Seek(0, io.SeekStart); err != nil {
			return err
		}

		var uploadErr error
		etag, uploadErr = s.doMultipartUpload(ctx, file, fileSize, objectPath)
		return uploadErr
	})

	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrUploadFailed, err)
	}

	return etag, nil
}

func (s *S3Storage) doMultipartUpload(ctx context.Context, file *os.File, fileSize int64, objectPath string) (string, error) {
	partSize := s.config.MultipartConfig.PartSize

	// Create multipart upload
	createResp, err := s.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(objectPath),
	})
	if err != nil {
		return "", err
	}

	uploadID := createResp.UploadId
	numParts := int(math.Ceil(float64(fileSize) / float64(partSize)))
	completedParts := make([]types.CompletedPart, 0, numParts)

	// Upload parts
	for partNum := 1; partNum <= numParts; partNum++ {
		offset := int64(partNum-1) * partSize
		size := partSize
		if offset+size > fileSize {
			size = fileSize - offset
		}

		partData := make([]byte, size)
		if _, err := file.ReadAt(partData, offset); err != nil && err != io.EOF {
			s.abortMultipartUpload(ctx, objectPath, uploadID)
			return "", err
		}

		uploadResp, err := s.client.UploadPart(ctx, &s3.UploadPartInput{
			Bucket:        aws.String(s.bucket),
			Key:           aws.String(objectPath),
			UploadId:      uploadID,
			PartNumber:    aws.Int32(int32(partNum)),
			Body:          io.NewSectionReader(file, offset, size),
			ContentLength: aws.Int64(size),
		})
		if err != nil {
			s.abortMultipartUpload(ctx, objectPath, uploadID)
			return "", err
		}

		completedParts = append(completedParts, types.CompletedPart{
			ETag:       uploadResp.ETag,
			PartNumber: aws.Int32(int32(partNum)),
		})
	}

	// Complete multipart upload
	completeResp, err := s.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(s.bucket),
		Key:      aws.String(objectPath),
		UploadId: uploadID,
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: completedParts,
		},
	})
	if err != nil {
		s.abortMultipartUpload(ctx, objectPath, uploadID)
		return "", err
	}

	return aws.ToString(completeResp.ETag), nil
}

func (s *S3Storage) abortMultipartUpload(ctx context.Context, objectPath string, uploadID *string) {
	_, _ = s.client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(s.bucket),
		Key:      aws.String(objectPath),
		UploadId: uploadID,
	})
}

// Download downloads a file from S3.
func (s *S3Storage) Download(ctx context.Context, objectPath, localPath string) error {
	var resp *s3.GetObjectOutput
	err := s.retryWithBackoff(ctx, func() error {
		var getErr error
		resp, getErr = s.client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(s.bucket),
			Key:    aws.String(objectPath),
		})
		return getErr
	})

	if err != nil {
		var noSuchKey *types.NoSuchKey
		if errors.As(err, &noSuchKey) {
			return ErrObjectNotFound
		}
		return fmt.Errorf("%w: %v", ErrDownloadFailed, err)
	}
	defer resp.Body.Close()

	file, err := os.Create(localPath)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrDownloadFailed, err)
	}
	defer file.Close()

	if _, err := io.Copy(file, resp.Body); err != nil {
		return fmt.Errorf("%w: %v", ErrDownloadFailed, err)
	}

	return nil
}

// Delete removes an object from S3.
func (s *S3Storage) Delete(ctx context.Context, objectPath string) error {
	err := s.retryWithBackoff(ctx, func() error {
		_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(s.bucket),
			Key:    aws.String(objectPath),
		})
		return err
	})

	if err != nil {
		return fmt.Errorf("%w: %v", ErrDeleteFailed, err)
	}

	return nil
}

// Exists checks if an object exists in S3.
func (s *S3Storage) Exists(ctx context.Context, objectPath string) (bool, error) {
	var exists bool
	err := s.retryWithBackoff(ctx, func() error {
		_, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(s.bucket),
			Key:    aws.String(objectPath),
		})
		if err != nil {
			var notFound *types.NotFound
			if errors.As(err, &notFound) {
				exists = false
				return nil
			}
			return err
		}
		exists = true
		return nil
	})

	return exists, err
}

// ListObjects returns all object paths under the given prefix.
func (s *S3Storage) ListObjects(ctx context.Context, prefix string) ([]string, error) {
	var objects []string
	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(prefix),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to list objects: %w", err)
		}
		for _, obj := range page.Contents {
			objects = append(objects, aws.ToString(obj.Key))
		}
	}

	return objects, nil
}

// ConditionalPut uploads only if the precondition is met.
func (s *S3Storage) ConditionalPut(ctx context.Context, localPath, objectPath, etag string) error {
	file, err := os.Open(localPath)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrUploadFailed, err)
	}
	defer file.Close()

	return s.retryWithBackoff(ctx, func() error {
		// Reset file position for retry
		if _, err := file.Seek(0, io.SeekStart); err != nil {
			return err
		}

		input := &s3.PutObjectInput{
			Bucket: aws.String(s.bucket),
			Key:    aws.String(objectPath),
			Body:   file,
		}

		// If etag is provided, use If-Match condition
		if etag != "" {
			input.IfMatch = aws.String(etag)
		}

		_, err := s.client.PutObject(ctx, input)
		if err != nil {
			// Check for precondition failed error in the error message
			// AWS SDK v2 doesn't have a specific type for this
			if isS3PreconditionFailed(err) {
				return ErrPreconditionFailed
			}
			return err
		}
		return nil
	})
}

// isS3PreconditionFailed checks if the error is a precondition failed error.
func isS3PreconditionFailed(err error) bool {
	if err == nil {
		return false
	}
	// Check error message for precondition failed indicators
	errStr := err.Error()
	return contains(errStr, "PreconditionFailed") || contains(errStr, "412")
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsImpl(s, substr))
}

func containsImpl(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// getETag retrieves the ETag of an object.
func (s *S3Storage) getETag(ctx context.Context, objectPath string) (string, error) {
	resp, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(objectPath),
	})
	if err != nil {
		return "", err
	}
	return aws.ToString(resp.ETag), nil
}

// retryWithBackoff executes the operation with exponential backoff retry.
func (s *S3Storage) retryWithBackoff(ctx context.Context, operation func() error) error {
	var lastErr error
	for attempt := 0; attempt <= s.maxRetries; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}

		lastErr = operation()
		if lastErr == nil {
			return nil
		}

		// Don't retry on precondition failures or not found errors
		if errors.Is(lastErr, ErrPreconditionFailed) || errors.Is(lastErr, ErrObjectNotFound) {
			return lastErr
		}

		if attempt < s.maxRetries {
			backoff := time.Duration(math.Pow(2, float64(attempt))) * 100 * time.Millisecond
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
		}
	}
	return lastErr
}
