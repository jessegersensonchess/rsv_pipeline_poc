package csv

import "strings"

//const utf8BOM = "\uFEFF"

// StripHeaderBOM removes a UTF-8 BOM from the first header cell if present.
func StripHeaderBOM(headers []string) []string {
	if len(headers) == 0 {
		return headers
	}
	if strings.HasPrefix(headers[0], utf8BOM) {
		headers[0] = strings.TrimPrefix(headers[0], utf8BOM)
	}
	return headers
}
