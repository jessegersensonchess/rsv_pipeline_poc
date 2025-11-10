package schema

type Field struct {
	Name     string   `json:"name"`
	Type     string   `json:"type"` // "int" | "text" | "bool" | "date"
	Required bool     `json:"required,omitempty"`
	Nullable bool     `json:"nullable,omitempty"`
	Enum     []string `json:"enum,omitempty"`
	Layout   string   `json:"layout,omitempty"` // date layout
	Truthy   []string `json:"truthy,omitempty"` // bool parsing
	Falsy    []string `json:"falsy,omitempty"`
}

type Contract struct {
	Name      string            `json:"name"`
	Fields    []Field           `json:"fields"`
	HeaderMap map[string]string `json:"header_map,omitempty"`
}
