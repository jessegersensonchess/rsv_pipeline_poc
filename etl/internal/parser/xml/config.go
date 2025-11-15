// Package xmlparser provides a configurable, streaming XML parser
// designed to fit the project's existing parser interfaces and style.
// It uses JSON configuration and the Go standard library only.
package xmlparser

import (
	"encoding/json"
	"fmt"
	"strings"
)

// Config describes how to extract values from each <record_tag> element.
// Paths are relative to each record and support a simple predicate on the
// last segment, e.g. "ArticleIdList/ArticleId[@IdType='doi']".
type Config struct {
	RecordTag string            `json:"record_tag"`
	Fields    map[string]string `json:"fields,omitempty"` // single-valued
	Lists     map[string]string `json:"lists,omitempty"`  // multi-valued
}

// ParseConfigJSON parses JSON bytes into a Config and validates required fields.
func ParseConfigJSON(b []byte) (Config, error) {
	var c Config
	if err := json.Unmarshal(b, &c); err != nil {
		return c, err
	}
	if strings.TrimSpace(c.RecordTag) == "" {
		return c, fmt.Errorf("config.record_tag is required")
	}
	if c.Fields == nil {
		c.Fields = map[string]string{}
	}
	if c.Lists == nil {
		c.Lists = map[string]string{}
	}
	return c, nil
}

// MarshalConfigJSON returns JSON for a Config. When pretty is true, output is indented.
// (We intentionally keep the standard map shape so ParseConfigJSON can read it back.)
func MarshalConfigJSON(c Config, pretty bool) ([]byte, error) {
	if pretty {
		return json.MarshalIndent(c, "", "  ")
	}
	return json.Marshal(c)
}
