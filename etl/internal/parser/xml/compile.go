// Package xmlparser contains config compilation helpers used by the XML stream
// parser. This file provides the minimal symbols required by stream/shard code
// (Compiled, namedMatcher, tailMatches) and keeps them package-internal.
package xmlparser

import (
	"fmt"
	"strings"
)

// seg is one path segment with an optional predicate on the last segment.
type seg struct{ name, attrName, attrVal string }

// pathSpec is a compiled path like "A/B/C[@k='v']".
type pathSpec struct{ segs []seg }

// parsePathSpec parses a relative path with an optional predicate on the last segment.
func parsePathSpec(raw string) (pathSpec, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return pathSpec{}, fmt.Errorf("empty path")
	}
	parts := strings.Split(raw, "/")
	segs := make([]seg, 0, len(parts))
	for i, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			return pathSpec{}, fmt.Errorf("bad empty segment in %q", raw)
		}
		s := seg{name: p}
		if i == len(parts)-1 {
			// Allow Name[@Attr='Value'] on last segment only.
			if j := strings.Index(p, "["); j != -1 && strings.HasSuffix(p, "]") {
				base := p[:j]
				pred := strings.TrimSpace(p[j+1 : len(p)-1])
				s.name = base
				if strings.HasPrefix(pred, "@") {
					pred = pred[1:]
					if eq := strings.Index(pred, "="); eq > 0 {
						s.attrName = pred[:eq]
						val := strings.TrimSpace(pred[eq+1:])
						s.attrVal = strings.Trim(val, `"'`)
					}
				}
			}
		}
		segs = append(segs, s)
	}
	return pathSpec{segs: segs}, nil
}

// namedMatcher binds an output key to a compiled pathSpec.
type namedMatcher struct {
	outKey string
	spec   pathSpec
	isList bool
}

// Compiled is the compiled configuration used in the hot path.
type Compiled struct {
	recordTag string
	byLast    map[string][]namedMatcher // last element name → matchers
}

// Example getter in Compiled struct
func (c *Compiled) RecordTag() string {
	return c.recordTag
}

// Compile compiles a Config into a hot-path Compiled structure.
func Compile(c Config) (Compiled, error) {
	cc := Compiled{
		recordTag: c.RecordTag,
		byLast:    map[string][]namedMatcher{},
	}
	for k, p := range c.Fields {
		ps, err := parsePathSpec(p)
		if err != nil {
			return cc, fmt.Errorf("fields.%s: %w", k, err)
		}
		last := ps.segs[len(ps.segs)-1].name
		cc.byLast[last] = append(cc.byLast[last], namedMatcher{outKey: k, spec: ps, isList: false})
	}
	for k, p := range c.Lists {
		ps, err := parsePathSpec(p)
		if err != nil {
			return cc, fmt.Errorf("lists.%s: %w", k, err)
		}
		last := ps.segs[len(ps.segs)-1].name
		cc.byLast[last] = append(cc.byLast[last], namedMatcher{outKey: k, spec: ps, isList: true})
	}
	return cc, nil
}

// tailMatches reports whether rel (a stack of element names relative to the record)
// ends with spec's segments.
func tailMatches(rel []string, spec pathSpec) bool {
	if len(rel) < len(spec.segs) {
		return false
	}
	off := len(rel) - len(spec.segs)
	for i := range spec.segs {
		if rel[off+i] != spec.segs[i].name {
			return false
		}
	}
	return true
}
