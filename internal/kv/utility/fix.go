package utility

import "path"

// GetPrefixByDirectory checks if the directory is empty or not end with /, and returns the prefix of the directory
func GetPrefixByDirectory(rootPath string, directory string) string {
	prefix := path.Join(rootPath, directory) + "/"
	return prefix
}
