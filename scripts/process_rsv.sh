#!/bin/bash
export DATA_DIR="${HOME}/projects/etl_pipeline/etl/testdata"
export old_file="${DATA_DIR}/RSV_vozidla_dovoz_20251001.csv"
export new_file="${DATA_DIR}/RSV_vozidla_dovoz_20251101.csv"
export PCV_FILE="${DATA_DIR}/pcv_with_ico-2025-11-17"
export DIFF="${DATA_DIR}/diff-$(date +%F)"

wc -l "$old_file"

linediff -file1 "$old_file" -file2 "$new_file" > "$DIFF"

wc -l "$DIFF"

get_data_with_pcv -pcvfile "$PCV_FILE" -data "$DIFF"

