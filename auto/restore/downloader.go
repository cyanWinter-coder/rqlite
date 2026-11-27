package restore

import (
	"bytes"
	"compress/gzip"
	"context"
	"expvar"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/rqlite/rqlite/v10/auto/verify"
	"github.com/rqlite/rqlite/v10/internal/fsutil"
)

// stats captures stats for the Downloader service.
var stats *expvar.Map

var (
	gzipMagic = []byte{0x1f, 0x8b, 0x08}
)

const (
	numDownloadsOK        = "num_downloads_ok"
	numDownloadsFail      = "num_downloads_fail"
	numDownloadsRetried   = "num_downloads_retried"
	numVerificationFail   = "num_verification_fail"
	numQuarantined        = "num_quarantined"
	numDownloadBytes      = "download_bytes"
	lastDownloadBytes     = "last_download_bytes"
	lastDownloadState     = "last_download_state"
	downloadStateNone     = "none"
	downloadStateVerified = "verified"
	downloadStateFailed   = "failed"
)

func init() {
	stats = expvar.NewMap("downloader")
	ResetStats()
}

// ResetStats resets the expvar stats for this module. Mostly for test purposes.
func ResetStats() {
	stats.Init()
	stats.Add(numDownloadsOK, 0)
	stats.Add(numDownloadsFail, 0)
	stats.Add(numDownloadsRetried, 0)
	stats.Add(numVerificationFail, 0)
	stats.Add(numQuarantined, 0)
	stats.Add(numDownloadBytes, 0)
	stats.Add(lastDownloadBytes, 0)
	stats.Add(lastDownloadState, 0)
}

// Progress is a point-in-time view of a download, observable while the
// download is in progress.
type Progress struct {
	// State is "downloading", "verifying", "verified" or "failed".
	State string `json:"state"`

	// Attempt is the 1-based attempt number currently in progress.
	Attempt int `json:"attempt"`

	// BytesDownloaded is the number of bytes fetched on the current attempt.
	BytesDownloaded int64 `json:"bytes_downloaded"`

	// LastError describes the most recent failed attempt, if any.
	LastError string `json:"last_error,omitempty"`
}

// ProgressListener is called whenever the observable download state changes.
// Implementations must not block.
type ProgressListener func(Progress)

// DownloadFile downloads the auto-restore file from the given URL, verifies
// its self-describing checksum, and returns the path to the verified payload
// (decompressed when the object is gzip-compressed).
//
// A checksum or size mismatch rejects the object: the raw object and its
// original verification metadata are moved into the quarantine directory for
// manual inspection and are never deleted automatically. The returned error
// is non-nil in that case regardless of ContinueOnFailure, so a corrupt
// snapshot can never be installed as a node's starting state.
func DownloadFile(ctx context.Context, cfgPath string) (path string, errOK bool, err error) {
	return DownloadFileToQuarantine(ctx, cfgPath, "")
}

// DownloadFileToQuarantine behaves like DownloadFile but lets the caller pin
// the quarantine directory (for example inside the node's data directory), so
// rejected snapshots survive operating-system temp cleanup. An empty
// quarantineDir falls back to the config value, then to an OS temp default.
func DownloadFileToQuarantine(ctx context.Context, cfgPath, quarantineDir string) (path string, errOK bool, err error) {
	var f *os.File
	defer func() {
		if err != nil {
			if f != nil {
				f.Close()
				fsutil.Remove(f.Name())
			}
		}
	}()

	b, err := ReadConfigFile(cfgPath)
	if err != nil {
		return "", false, fmt.Errorf("failed to read auto-restore file: %s", err.Error())
	}

	var sc StorageClient
	var dCfg *Config
	dCfg, sc, err = NewStorageClient(b)
	if err != nil {
		return "", false, fmt.Errorf("failed to create storage client: %s", err.Error())
	}
	d := NewDownloader(sc)
	switch {
	case quarantineDir != "":
		if qerr := d.SetQuarantineDir(quarantineDir); qerr != nil {
			return "", false, qerr
		}
	case dCfg.QuarantinePath != "":
		if qerr := d.SetQuarantineDir(dCfg.QuarantinePath); qerr != nil {
			return "", false, qerr
		}
	}

	// The verified payload is delivered to a file the caller installs.
	f, err = os.CreateTemp("", "rqlite-auto-restore")
	if err != nil {
		return "", false, fmt.Errorf("failed to create temporary file: %s", err.Error())
	}

	if err = d.DownloadVerified(ctx, f, time.Duration(dCfg.Timeout)); err != nil {
		// A corrupt/unverifiable snapshot is never safe to start from, so
		// continue-on-failure cannot apply to it: startup is always rejected
		// and the evidence stays in the quarantine directory.
		if IsVerificationError(err) {
			return "", false, err
		}
		return "", dCfg.ContinueOnFailure, fmt.Errorf("failed to download auto-restore file: %s", err.Error())
	}
	return f.Name(), false, nil
}

// StorageClient is an interface for downloading data from a storage service.
type StorageClient interface {
	Download(ctx context.Context, writer io.WriterAt) error
	fmt.Stringer
}

// Downloader is a struct that handles downloading data from a storage service.
type Downloader struct {
	storageClient StorageClient
	logger        *log.Logger

	nRetries      int
	retryInterval time.Duration

	quarantine *verify.Quarantine
	listener   ProgressListener
}

// NewDownloader creates a new Downloader instance with the given StorageClient.
func NewDownloader(storageClient StorageClient) *Downloader {
	return &Downloader{
		storageClient: storageClient,
		logger:        log.New(os.Stderr, "[downloader] ", log.LstdFlags),
		nRetries:      3,
		retryInterval: time.Second,
	}
}

// SetRetries configures how many times a transient download failure is
// retried (the initial attempt is always made) and the wait between attempts.
func (d *Downloader) SetRetries(n int, interval time.Duration) {
	d.nRetries = n
	d.retryInterval = interval
}

// SetQuarantineDir sets the directory used to preserve snapshots that fail
// verification. It must be called before DownloadVerified.
func (d *Downloader) SetQuarantineDir(dir string) error {
	q, err := verify.NewQuarantine(dir)
	if err != nil {
		return err
	}
	d.quarantine = q
	return nil
}

// SetProgressListener registers a callback invoked as the download progresses.
func (d *Downloader) SetProgressListener(l ProgressListener) {
	d.listener = l
}

func (d *Downloader) report(p Progress) {
	stats.Get(lastDownloadState).(*expvar.Int).Set(progressStateCode(p.State))
	if d.listener != nil {
		d.listener(p)
	}
}

// DownloadVerified downloads data from the storage service into a fresh
// staging file, verifies the self-describing checksum before any payload is
// handed to w, and then decompresses it into w if it is gzip-compressed.
//
// Transient download errors are retried from scratch with a new staging file.
// A verification failure is not retried: retrying cannot repair a corrupt
// object, and the object plus its original metadata are quarantined. The
// function returns a non-nil error in that case and never writes the corrupt
// payload to w.
func (d *Downloader) DownloadVerified(ctx context.Context, w io.Writer, timeout time.Duration) (err error) {
	defer func() {
		if err == nil {
			stats.Add(numDownloadsOK, 1)
		} else {
			stats.Add(numDownloadsFail, 1)
		}
	}()

	var lastErr error
	for attempt := 1; attempt <= d.nRetries+1; attempt++ {
		if attempt > 1 {
			stats.Add(numDownloadsRetried, 1)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(d.retryInterval):
			}
		}
		d.report(Progress{State: "downloading", Attempt: attempt})

		// A fresh staging file per attempt means a truncated download can
		// never be mistaken for a complete object.
		staging, perr := os.CreateTemp("", "rqlite-downloader")
		if perr != nil {
			return perr
		}
		stagingPath := staging.Name()

		attemptCtx, cancel := context.WithTimeout(ctx, timeout)
		cw := &countingWriterAt{writerAt: staging}
		dlErr := d.storageClient.Download(attemptCtx, &reportingWriterAt{
			writerAt: cw,
			attempt:  attempt,
			report:   d.report,
		})
		cancel()
		if dlErr != nil {
			staging.Close()
			fsutil.Remove(stagingPath)
			lastErr = fmt.Errorf("download attempt %d: %w", attempt, dlErr)
			d.report(Progress{State: "failed", Attempt: attempt, LastError: lastErr.Error()})
			d.logger.Printf("%s", lastErr)
			continue
		}
		if cerr := staging.Close(); cerr != nil {
			fsutil.Remove(stagingPath)
			lastErr = cerr
			continue
		}

		// Verify against metadata stored inside the object itself; no local
		// cache of checksums is consulted. This streams the file rather than
		// loading it, so memory use does not scale with snapshot size.
		d.report(Progress{State: "verifying", Attempt: attempt, BytesDownloaded: cw.count})
		envelope, _, verifyErr := verify.VerifyFile(stagingPath)
		if verifyErr != nil {
			// A corrupt object is evidence. Move the file (with its original
			// metadata) into quarantine, then stop: it must never be installed.
			stats.Add(numVerificationFail, 1)
			qDir := d.quarantineFile(stagingPath, verifyErr, envelope)
			// quarantineFile moves the file on success; only remove it if it
			// could not be preserved.
			if qDir == "" {
				fsutil.Remove(stagingPath)
			}
			d.report(Progress{State: "failed", Attempt: attempt, LastError: verifyErr.Error()})
			return &verificationError{err: verifyErr, quarantineDir: qDir}
		}

		// Verification passed. Stream the payload region to the caller's
		// writer, decompressing transparently when it is gzip data.
		payloadReader, _, perr := verify.PayloadReader(stagingPath)
		if perr != nil {
			fsutil.Remove(stagingPath)
			lastErr = perr
			continue
		}
		if derr := d.deliverPayload(payloadReader, w); derr != nil {
			payloadReader.Close()
			fsutil.Remove(stagingPath)
			lastErr = derr
			continue
		}
		payloadReader.Close()

		objectSize := cw.count
		fsutil.Remove(stagingPath)
		stats.Add(numDownloadBytes, objectSize)
		stats.Get(lastDownloadBytes).(*expvar.Int).Set(objectSize)
		d.report(Progress{State: "verified", Attempt: attempt, BytesDownloaded: objectSize})
		return nil
	}
	return lastErr
}

// Do downloads and verifies data, writing the resulting payload to w. It is
// retained for callers that do not need retry/progress configuration.
func (d *Downloader) Do(ctx context.Context, w io.Writer, timeout time.Duration) error {
	return d.DownloadVerified(ctx, w, timeout)
}

// deliverPayload copies payload to w, transparently gunzipping it when it
// carries the gzip magic number.
func (d *Downloader) deliverPayload(payload io.Reader, w io.Writer) error {
	// Peek at the first bytes to detect gzip without buffering the whole
	// payload again.
	head := make([]byte, len(gzipMagic))
	n, err := io.ReadFull(payload, head)
	if err != nil && err != io.ErrUnexpectedEOF && err != io.EOF {
		return err
	}
	head = head[:n]
	r := io.MultiReader(bytes.NewReader(head), payload)
	if bytes.Equal(head, gzipMagic) {
		gzr, gerr := gzip.NewReader(r)
		if gerr != nil {
			return gerr
		}
		defer gzr.Close()
		if _, err := io.Copy(w, gzr); err != nil {
			return fmt.Errorf("failed to decompress data: %s", err.Error())
		}
		return nil
	}
	if _, err := io.Copy(w, r); err != nil {
		return fmt.Errorf("failed to write data: %s", err.Error())
	}
	return nil
}

// quarantineFile preserves a failed object file and its original metadata. If
// no quarantine directory has been configured, a default under the OS temp
// directory is created. Quarantine failures are logged but never mask the
// verification error that triggered them. It returns the quarantine entry
// directory, or "" if the object could not be preserved.
func (d *Downloader) quarantineFile(srcPath string, reason error, envelope *verify.Envelope) string {
	var err error
	if d.quarantine == nil {
		if err = d.SetQuarantineDir(filepath.Join(os.TempDir(), "rqlite-restore-quarantine")); err != nil {
			d.logger.Printf("failed to initialize quarantine directory: %s", err)
			return ""
		}
	}
	entry, err := d.quarantine.StoreFile(srcPath, reason, envelope, d.storageClient.String())
	if err != nil {
		d.logger.Printf("failed to quarantine corrupt snapshot: %s", err)
		return ""
	}
	stats.Add(numQuarantined, 1)
	d.logger.Printf("corrupt snapshot preserved for manual investigation at %s: %s", entry, reason)
	return entry
}

type countingWriterAt struct {
	writerAt io.WriterAt
	count    int64
}

func (c *countingWriterAt) WriteAt(p []byte, off int64) (n int, err error) {
	n, err = c.writerAt.WriteAt(p, off)
	c.count += int64(n)
	return
}

// reportingWriterAt forwards writes and emits progress updates bounded by the
// current attempt.
type reportingWriterAt struct {
	writerAt io.WriterAt
	attempt  int
	report   ProgressListener
}

func (r *reportingWriterAt) WriteAt(p []byte, off int64) (int, error) {
	n, err := r.writerAt.WriteAt(p, off)
	if r.report != nil {
		r.report(Progress{State: "downloading", Attempt: r.attempt, BytesDownloaded: off + int64(n)})
	}
	return n, err
}

func progressStateCode(state string) int64 {
	switch state {
	case "downloading":
		return 1
	case "verifying":
		return 2
	case "verified":
		return 3
	case "failed":
		return 4
	default:
		return 0
	}
}
