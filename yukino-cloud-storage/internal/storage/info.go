package storage

import (
	"fmt"
	"os"
	"path/filepath"
)

// StorageInfo is the metadata record for one stored file. It is what gets
// persisted in the storage info table and rendered in the web UI.
type StorageInfo struct {
	// ModTime is the last modification time as a Unix timestamp.
	ModTime int64 `json:"mtime"`
	// AccessTime is the last access time as a Unix timestamp. Go does not
	// expose atime portably, so it mirrors ModTime; the field is retained for
	// schema compatibility with the original record layout.
	AccessTime int64 `json:"atime"`
	// Size is the stored object size in bytes. For deep storage this is the
	// size of the compressed bundle.
	Size int64 `json:"fsize"`
	// StoragePath is the on-disk location of the stored object.
	StoragePath string `json:"storage_path"`
	// URL is the resource path clients use to download the file.
	URL string `json:"url"`
	// Name is the file name presented to clients on download.
	Name string `json:"name"`
	// Deep records whether the object is stored compressed.
	Deep bool `json:"deep"`
}

// NewStorageInfo builds a record by stat-ing storagePath. downloadPrefix is
// prepended to the file name to form the public URL.
//
// The original implementation assigned the access time into the modification
// field and vice versa; that swap is corrected here.
func NewStorageInfo(storagePath, downloadPrefix string, deep bool) (*StorageInfo, error) {
	st, err := os.Stat(storagePath)
	if err != nil {
		return nil, err
	}
	name := filepath.Base(storagePath)
	mod := st.ModTime().Unix()
	return &StorageInfo{
		ModTime:     mod,
		AccessTime:  mod,
		Size:        st.Size(),
		StoragePath: storagePath,
		URL:         downloadPrefix + name,
		Name:        name,
		Deep:        deep,
	}, nil
}

// ETag returns a validator for conditional and range requests, quoted as
// required by RFC 7232. The original emitted an unquoted value.
func (i *StorageInfo) ETag() string {
	return fmt.Sprintf(`"%s-%d-%d"`, i.Name, i.Size, i.ModTime)
}
