package verify

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

// quarantineRecordFile is the metadata written next to every quarantined
// object. It preserves the original verification envelope and the reason the
// object was rejected, so an operator can investigate without reconstructing
// anything from logs.
const quarantineRecordFile = "record.json"

// QuarantineRecord is the on-disk record describing one rejected snapshot.
type QuarantineRecord struct {
	// ObjectFile is the quarantined snapshot object, relative to the
	// quarantine directory.
	ObjectFile string `json:"object_file"`

	// Reason is the verification error that caused the quarantine.
	Reason string `json:"reason"`

	// Envelope is the original verification envelope parsed from the
	// rejected object. It is nil when the object carried no envelope.
	Envelope *Envelope `json:"envelope,omitempty"`

	// Source identifies where the object came from, e.g. the storage
	// client's String().
	Source string `json:"source,omitempty"`

	// QuarantinedAt records when the rejection happened (RFC3339).
	QuarantinedAt string `json:"quarantined_at"`
}

// Quarantine stores snapshots that failed verification in a dedicated
// directory. Nothing in this directory is ever deleted automatically: a
// corrupt snapshot is evidence and must survive until a human inspects it.
type Quarantine struct {
	dir string
}

// NewQuarantine creates a Quarantine rooted at dir, creating the directory if
// necessary.
func NewQuarantine(dir string) (*Quarantine, error) {
	if dir == "" {
		return nil, fmt.Errorf("quarantine directory must be non-empty")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("creating quarantine directory %s: %w", dir, err)
	}
	return &Quarantine{dir: dir}, nil
}

// Dir returns the quarantine directory path.
func (q *Quarantine) Dir() string {
	return q.dir
}

// Store copies the rejected object into a unique subdirectory of the
// quarantine root together with a record describing the failure. The original
// envelope is preserved verbatim inside the record. It never overwrites an
// existing entry and never deletes anything.
func (q *Quarantine) Store(object []byte, reason error, envelope *Envelope, source string) (string, error) {
	if reason == nil {
		return "", fmt.Errorf("refusing to quarantine an object without a failure reason")
	}
	entryDir, err := q.uniqueEntryDir()
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(entryDir, 0o755); err != nil {
		return "", fmt.Errorf("creating quarantine entry: %w", err)
	}

	objPath := filepath.Join(entryDir, "snapshot.bin")
	if err := os.WriteFile(objPath, object, 0o644); err != nil {
		return "", fmt.Errorf("writing quarantined object: %w", err)
	}
	return q.writeRecord(entryDir, reason, envelope, source)
}

// StoreFile moves (or, across filesystems, copies) a rejected object file
// into a unique quarantine entry, preserving its original verification
// metadata. It is the streaming counterpart to Store and avoids reading large
// objects into memory. On success the source file is removed; on failure the
// source is left untouched so evidence is never lost.
func (q *Quarantine) StoreFile(srcPath string, reason error, envelope *Envelope, source string) (string, error) {
	if reason == nil {
		return "", fmt.Errorf("refusing to quarantine an object without a failure reason")
	}
	entryDir, err := q.uniqueEntryDir()
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(entryDir, 0o755); err != nil {
		return "", fmt.Errorf("creating quarantine entry: %w", err)
	}

	objPath := filepath.Join(entryDir, "snapshot.bin")
	if err := moveFile(srcPath, objPath); err != nil {
		return "", fmt.Errorf("preserving quarantined object: %w", err)
	}
	return q.writeRecord(entryDir, reason, envelope, source)
}

// writeRecord writes the JSON record describing a rejected object and returns
// its entry directory.
func (q *Quarantine) writeRecord(entryDir string, reason error, envelope *Envelope, source string) (string, error) {
	rec := &QuarantineRecord{
		ObjectFile:    "snapshot.bin",
		Reason:        reason.Error(),
		Envelope:      envelope,
		Source:        source,
		QuarantinedAt: time.Now().UTC().Format(time.RFC3339Nano),
	}
	recBytes, err := json.MarshalIndent(rec, "", "  ")
	if err != nil {
		return "", fmt.Errorf("marshaling quarantine record: %w", err)
	}
	recPath := filepath.Join(entryDir, quarantineRecordFile)
	if err := os.WriteFile(recPath, recBytes, 0o644); err != nil {
		return "", fmt.Errorf("writing quarantine record: %w", err)
	}
	return entryDir, nil
}

// moveFile renames src to dst, falling back to a copy-and-remove when the two
// paths live on different filesystems (where rename is not possible).
func moveFile(src, dst string) error {
	if err := os.Rename(src, dst); err == nil {
		return nil
	}
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	if err := out.Sync(); err != nil {
		out.Close()
		return err
	}
	if err := out.Close(); err != nil {
		return err
	}
	return os.Remove(src)
}

// Entries returns the paths of quarantine entry directories, sorted by name.
func (q *Quarantine) Entries() ([]string, error) {
	entries, err := os.ReadDir(q.dir)
	if err != nil {
		return nil, fmt.Errorf("reading quarantine directory: %w", err)
	}
	var out []string
	for _, e := range entries {
		if e.IsDir() {
			out = append(out, filepath.Join(q.dir, e.Name()))
		}
	}
	return out, nil
}

// uniqueEntryDir returns a not-yet-existing entry path. The nanosecond
// timestamp is virtually always unique; a numeric suffix handles the rare
// collision so an entry can never be overwritten.
func (q *Quarantine) uniqueEntryDir() (string, error) {
	base := fmt.Sprintf("snapshot-%s", time.Now().UTC().Format("20060102T150405.000000000"))
	for i := 0; ; i++ {
		name := base
		if i > 0 {
			name = fmt.Sprintf("%s-%d", base, i)
		}
		dir := filepath.Join(q.dir, name)
		if _, err := os.Stat(dir); os.IsNotExist(err) {
			return dir, nil
		} else if err != nil {
			return "", fmt.Errorf("allocating quarantine entry: %w", err)
		}
	}
}
