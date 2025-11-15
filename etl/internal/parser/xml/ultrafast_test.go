package xmlparser

import "testing"

func TestUltraFastExtractPubMed(t *testing.T) {
	xml := `<PubmedArticle><MedlineCitation><PMID>1</PMID></MedlineCitation><ArticleIdList><ArticleId IdType="doi">x</ArticleId></ArticleIdList><Article><ArticleTitle>T</ArticleTitle><Journal><Title>J</Title><JournalIssue><PubDate><Year>2020</Year></PubDate></JournalIssue></Journal></Article><MedlineJournalInfo><Country>CZ</Country></MedlineJournalInfo><AuthorList><LastName>A</LastName><LastName>B</LastName></AuthorList></PubmedArticle>`
	r, err := UltraFastExtractPubMed([]byte(xml))
	if err != nil {
		t.Fatal(err)
	}
	if r["pmid"] != "1" || r["doi"] != "x" || r["article_title"] != "T" {
		t.Fatalf("bad extract: %#v", r)
	}
}
