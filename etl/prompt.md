throughout the project, produce Google-grade code. All new golang code should also include new unit tests + benchmarks. Unit tests should use table-driven checks, godoc-style comments, and no third-party deps.

the project requirements are: 
1. give url 
2. csvprobe fetches url and figures out field-types 
3. user reviews, then edits, field types. (beside each field include a dropdown box to select the type (detected type is pre-selected). Beside each field include a checkbox for "required" if the field is required.) 

4. user picks primary key 
5. JSON is regenerated with the revized information 
6. etl consumes the json: creates db table, processes and inserts data
7. in UI, user click button to view table schema 
7a. for each field: let user click button which outputs results of: SELECT 'field' from 'table' limit 10; 
7b. provide a search box users can type into, and results from fuzzy matching are instantly displayed 
8. use simple, single file sqlite db implement this. 

below are selected source files and a filestructure. If the contents of other files are required, list the filenames.

// cmd/csvprobe/main.go
// cmd/csvprobe-ui/main.go
// cmd/etl/main.go
// cmd/etl/container.go
// internal/storage/postgres/*go but not test