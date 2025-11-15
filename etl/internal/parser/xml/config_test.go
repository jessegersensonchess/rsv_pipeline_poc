package xmlparser

import "testing"

func TestParseConfigJSON_OK(t *testing.T) {
	js := []byte(`{"record_tag":"PubmedArticle","fields":{"pmid":"MedlineCitation/PMID"}}`)
	c, err := ParseConfigJSON(js)
	if err != nil {
		t.Fatal(err)
	}
	if c.RecordTag != "PubmedArticle" {
		t.Fatalf("record_tag %q", c.RecordTag)
	}
	if _, ok := c.Fields["pmid"]; !ok {
		t.Fatalf("missing field pmid")
	}
}

func TestParsePathSpec_Predicate(t *testing.T) {
	ps, err := parsePathSpec(`A/B[@id="x"]`)
	if err != nil {
		t.Fatal(err)
	}
	if got := ps.segs[len(ps.segs)-1]; got.attrName != "id" || got.attrVal != "x" {
		t.Fatalf("predicate parse: %+v", got)
	}
}

func TestCompile_IndexByLast(t *testing.T) {
	c := Config{
		RecordTag: "R",
		Fields:    map[string]string{"k": "A/B/C"},
	}
	cc, err := Compile(c)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := cc.byLast["C"]; !ok {
		t.Fatalf("expected index on last seg C")
	}
}

func TestMarshalConfigJSON_Pretty(t *testing.T) {
	b, err := MarshalConfigJSON(Config{
		RecordTag: "X",
		Fields:    map[string]string{"b": "B", "a": "A"},
	}, true)
	if err != nil {
		t.Fatal(err)
	}
	if string(b) == "" || b[0] != '{' {
		t.Fatalf("unexpected marshal output")
	}
}
