package backup

import (
	"context"
	"expvar"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/rqlite/rqlite/v10/auto/verify"
	"github.com/rqlite/rqlite/v10/db/humanize"
	"github.com/rqlite/rqlite/v10/internal/fsutil"
	"github.com/rqlite/rqlite/v10/internal/progress"
)

// StorageClient is an interface for uploading data to a storage service.
type StorageClient interface {
	// Upload uploads the data from the given reader to the storage service.
	// id is an identifier for the data, and will be stored along with
	// the data in the storage service.
	Upload(ctx context.Context, reader io.Reader, id string) error

	// CurrentID returns the ID of the data in the Storage service.
	// It is always read from the Storage service, and a cached
	// value is never returned.
	CurrentID(ctx context.Context) (string, error)

	fmt.Stringer
}

// DataProvider is an interface for providing data to be uploaded. The Uploader
// service will call Provide() to have the data-for-upload written to the
// given writer.
type DataProvider interface {
	// LastIndex returns the cluster-wide index the data managed by the DataProvider was
	// last modified by.
	LastIndex() (uint64, error)

	// Provide writes the data-for-upload to the writer. A failed call may
	// leave partial data in the writer.
	Provide(w io.Writer) error
}

// stats captures stats for the Uploader service.
var stats *expvar.Map

const (
	numUploadsOK        = "num_uploads_ok"
	numUploadsFail      = "num_uploads_fail"
	numUploadsRetried   = "num_uploads_retried"
	numUploadsSkipped   = "num_uploads_skipped"
	numUploadsSkippedID = "num_uploads_skipped_id"
	numSumGetFail       = "num_sum_get_fail"
	totalUploadBytes    = "total_upload_bytes"
	lastUploadBytes     = "last_upload_bytes"
)

func init() {
	stats = expvar.NewMap("uploader")
	ResetStats()
}

// ResetStats resets the expvar stats for this module. Mostly for test purposes.
func ResetStats() {
	stats.Init()
	stats.Add(numUploadsOK, 0)
	stats.Add(numUploadsFail, 0)
	stats.Add(numUploadsRetried, 0)
	stats.Add(numUploadsSkipped, 0)
	stats.Add(numUploadsSkippedID, 0)
	stats.Add(numSumGetFail, 0)
	stats.Add(totalUploadBytes, 0)
	stats.Add(lastUploadBytes, 0)
}

// Uploader is a service that periodically uploads data to a storage service.
type Uploader struct {
	storageClient StorageClient
	dataProvider  DataProvider
	interval      time.Duration

	nRetries      int           // Number of times to retry a failed Provide.
	retryInterval time.Duration // Time to wait between Provide attempts.

	logger             *log.Logger
	lastUploadTime     time.Time
	lastUploadDuration time.Duration

	lastIndex uint64 // The last index of the data most-recently uploaded.
}

// NewUploader creates a new Uploader service.
func NewUploader(storageClient StorageClient, dataProvider DataProvider, interval time.Duration) *Uploader {
	return &Uploader{
		storageClient: storageClient,
		dataProvider:  dataProvider,
		interval:      interval,
		nRetries:      10,
		retryInterval: 500 * time.Millisecond,
		logger:        log.New(os.Stderr, "[uploader] ", log.LstdFlags),
	}
}

// Start starts the Uploader service.
func (u *Uploader) Start(ctx context.Context, isUploadEnabled func() bool) chan struct{} {
	doneCh := make(chan struct{})
	if isUploadEnabled == nil {
		isUploadEnabled = func() bool { return true }
	}

	u.logger.Printf("starting upload to %s every %s", u.storageClient, u.interval)
	ticker := time.NewTicker(u.interval)
	go func() {
		defer ticker.Stop()
		defer close(doneCh)
		for {
			select {
			case <-ctx.Done():
				u.logger.Println("upload service shutting down")
				return
			case <-ticker.C:
				if !isUploadEnabled() {
					continue
				}
				if err := u.upload(ctx); err != nil {
					u.logger.Printf("failed to upload to %s: %v", u.storageClient, err)
				}
			}
		}
	}()
	return doneCh
}

// Stats returns the stats for the Uploader service.
func (u *Uploader) Stats() (map[string]any, error) {
	status := map[string]any{
		"upload_destination":   u.storageClient.String(),
		"upload_interval":      u.interval.String(),
		"last_upload_time":     u.lastUploadTime.Format(time.RFC3339),
		"last_upload_duration": u.lastUploadDuration.String(),
		"last_index":           strconv.FormatUint(u.lastIndex, 10),
	}
	return status, nil
}

func (u *Uploader) upload(ctx context.Context) error {
	li, err := u.dataProvider.LastIndex()
	if err != nil {
		return err
	}
	if li <= u.lastIndex {
		stats.Add(numUploadsSkipped, 1)
		return nil
	}
	id := strconv.FormatUint(li, 10)

	if u.lastIndex == 0 {
		// No last index, so this must be the first upload since this
		// uploader started. Double-check that we really need to upload.
		currID, err := u.storageClient.CurrentID(ctx)
		if err != nil {
			stats.Add(numSumGetFail, 1)
			u.logger.Printf("failed to get current ID from %s: %v", u.storageClient, err)
		} else if currID == id {
			stats.Add(numUploadsSkippedID, 1)
			return nil
		}
	}

	// Build and upload the verifiable snapshot inside a single retry loop.
	// Every attempt works on a fresh temporary file that is removed on
	// failure, so an interrupted attempt can never leave a half-written
	// object that a later, smaller upload or a restore might mistake for a
	// valid snapshot. The object only becomes "the current backup" once the
	// storage client reports a successful, complete upload.
	var lastErr error
	for attempt := 0; attempt <= u.nRetries; attempt++ {
		if attempt > 0 {
			stats.Add(numUploadsRetried, 1)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(u.retryInterval):
			}
		}
		lastErr = u.attemptUpload(ctx, id)
		if lastErr == nil {
			break
		}
		u.logger.Printf("upload attempt %d/%d to %s failed: %v",
			attempt+1, u.nRetries+1, u.storageClient, lastErr)
	}
	if lastErr != nil {
		stats.Add(numUploadsFail, 1)
		return lastErr
	}

	// Successful upload!
	u.lastIndex = li
	stats.Add(numUploadsOK, 1)
	return nil
}

// attemptUpload performs one complete prepare-and-upload cycle. It builds the
// payload in a temporary file, appends the self-describing verification
// trailer, and uploads the resulting object. Any error leaves only the local
// temporary file behind, which is always removed; nothing in the storage
// service is promoted or renamed by this method.
func (u *Uploader) attemptUpload(ctx context.Context, id string) (retErr error) {
	fd, err := tempFD()
	if err != nil {
		return err
	}
	defer fsutil.Remove(fd.Name())
	defer fd.Close()

	// Compute the checksum while the provider streams the payload, then
	// persist the algorithm and checksum in the trailer so receivers verify
	// against metadata carried by the object itself.
	vw, err := verify.NewWriter(fd, verify.AlgorithmCRC32Castagnoli, id)
	if err != nil {
		return err
	}
	if err := u.dataProvider.Provide(vw); err != nil {
		return fmt.Errorf("preparing snapshot data: %w", err)
	}
	if err := vw.Close(); err != nil {
		return err
	}
	if err := fd.Sync(); err != nil {
		return fmt.Errorf("syncing staged snapshot: %w", err)
	}

	if _, err := fd.Seek(0, io.SeekStart); err != nil {
		return err
	}
	cr := progress.NewCountingReader(fd)
	startTime := time.Now()
	if err := u.storageClient.Upload(ctx, cr, id); err != nil {
		return fmt.Errorf("uploading snapshot: %w", err)
	}

	stats.Add(totalUploadBytes, cr.Count())
	stats.Get(lastUploadBytes).(*expvar.Int).Set(cr.Count())
	u.lastUploadTime = time.Now()
	u.lastUploadDuration = time.Since(startTime)
	u.logger.Printf("completed auto upload of %s to %s in %s",
		humanize.Bytes(uint64(cr.Count())), u.storageClient, u.lastUploadDuration)
	return nil
}

func tempFD() (*os.File, error) {
	return os.CreateTemp("", "rqlite-upload")
}
