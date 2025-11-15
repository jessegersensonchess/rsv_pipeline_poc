package xmlparser

import "bytes"

// UltraFastExtractPubMed is an optional, schema-specific fast path that
// extracts a small set of fields using raw byte scans. It assumes the input
// bytes contain an entire <record_tag> subtree. It is safe to ignore if the
// dataset is different; callers should gate its usage with a flag or config.
func UltraFastExtractPubMed(b []byte) (Record, error) {
	out := make(Record, 8)
	// Minimal helpers
	find := func(tag string) []byte {
		open := []byte("<" + tag + ">")
		close := []byte("</" + tag + ">")
		i := bytes.Index(b, open)
		if i < 0 {
			return nil
		}
		i += len(open)
		j := bytes.Index(b[i:], close)
		if j < 0 {
			return nil
		}
		return bytes.TrimSpace(b[i : i+j])
	}
	findAttr := func(tag, name, val string) []byte {
		open := []byte("<" + tag)
		gt := []byte(">")
		close := []byte("</" + tag + ">")
		pat := []byte(name + "=\"" + val + "\"")
		i := 0
		for {
			k := bytes.Index(b[i:], open)
			if k < 0 {
				return nil
			}
			k += i
			e := bytes.Index(b[k:], gt)
			if e < 0 {
				return nil
			}
			e += k
			if bytes.Contains(b[k:e], pat) {
				j := bytes.Index(b[e+1:], close)
				if j < 0 {
					return nil
				}
				return bytes.TrimSpace(b[e+1 : e+1+j])
			}
			i = e + 1
		}
	}
	if v := find("PMID"); len(v) > 0 {
		out["pmid"] = string(v)
	}
	if v := find("ArticleTitle"); len(v) > 0 {
		out["article_title"] = string(v)
	}
	if v := find("Title"); len(v) > 0 {
		out["journal_title"] = string(v)
	}
	if v := find("Year"); len(v) > 0 {
		out["publication_year"] = string(v)
	}
	if v := find("Country"); len(v) > 0 {
		out["country"] = string(v)
	}
	if v := findAttr("ArticleId", "IdType", "doi"); len(v) > 0 {
		out["doi"] = string(v)
	}
	// Authors/LastName (collect a few)
	open, close := []byte("<LastName>"), []byte("</LastName>")
	var authors []string
	i := 0
	for len(authors) < 16 {
		k := bytes.Index(b[i:], open)
		if k < 0 {
			break
		}
		k += i + len(open)
		j := bytes.Index(b[k:], close)
		if j < 0 {
			break
		}
		name := string(bytes.TrimSpace(b[k : k+j]))
		if name != "" {
			authors = append(authors, name)
		}
		i = k + j + len(close)
	}
	if len(authors) > 0 {
		anyAuthors := make([]any, len(authors))
		for i, s := range authors {
			anyAuthors[i] = s
		}
		out["authors_lastname"] = anyAuthors
	}
	return out, nil
}
