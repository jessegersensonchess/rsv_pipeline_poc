#!/bin/bash

set -e
BYTES=10000000
#export URL='https://download.dataovozidlech.cz/vypiszregistru/vozidladovoz'
REMOVE="$2"
URL="$1"
if [ -z $URL ]; then
	echo "ERROR: URL is empty"
	exit
fi
NAME=$(echo "$URL" | sed "s/.*\///g")
CSV="/home/zippy/projects/auto/etl/testdata/${NAME}.csv"
JSON="/home/zippy/projects/auto/etl/configs/pipelines/${NAME}.json"

echo "name=${NAME}, csv=$CSV, json=$JSON, url=$URL"
if [ -f $CSV ]; then rm $CSV; fi
if [ -f $NAME.csv ]; then rm $NAME.csv; fi
if [ -f $JSON ]; then rm $JSON; fi

if [ ! -z $REMOVE ]; then
	echo "INFO: remove done. exiting"
	exit
fi


csv_header_fetcher -url "$URL" -bytes "$BYTES" -json -name "$NAME" -save > "$JSON"
mv "${NAME}.csv" "$CSV" 

# patch json
sed -i 's/key_columns": \[\]/key_columns": \["pcv"\]/g' "$JSON"
sed -i 's/has_header": true,/has_header": true, "stream_scrub_likvidaci": true,/g' "$JSON"
#cat $JSON
#ls -al "$CSV"

cd /home/zippy/projects/auto/etl
go run ./cmd/etl -v -config "$JSON"

