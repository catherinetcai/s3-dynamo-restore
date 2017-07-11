package restore

import "path/filepath"

const (
	gzip = ".gz"
)

func filterFiles(list []string) (filtered []string) {
	for _, entry := range list {
		ext := filepath.Ext(entry)
		if ext == gzip {
			filtered = append(filtered, entry)
		}
	}
	return filtered
}
