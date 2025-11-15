#!/bin/bash
#for location in ceske_budejovice plzen usti_nad_labem brno ostrava hradec_kralove; do cat urls | while read typ;do $0 "$typ" "$location" 2025;done;done

set -e
BYTES=100000000
#sleep 0.2
TYP="$1"
location="$2"
YEAR="$3"


NAME="${TYP}-full-${location}-${YEAR}"
CONFIG="${HOME}/projects/etl_pipeline/etl/configs/pipelines/${TYP}-full-praha.json"
#CONFIG="${HOME}/projects/etl_pipeline/etl/configs/pipelines/${NAME}.json"
DIR="${HOME}/projects/etl_pipeline/etl/testdata/${location}"

make_dir() {
	if [ ! -d $DIR ]
	then
			mkdir "$DIR"
	fi
}

make_dir "$DIR"
if [ "$3" == 'csv' ]
then
	FILE_EXT='.csv'
	DATA="${HOME}/projects/etl_pipeline/etl/testdata/${location}/${NAME}${FILE_EXT}"
else
	FILE_EXT='.xml'
	DATA="${HOME}/projects/etl_pipeline/etl/testdata/${location}/${NAME}${FILE_EXT}"
fi
URL="https://dataor.justice.cz/api/file/${NAME}${FILE_EXT}"
#echo "$URL"

#echo "============= ${URL} ================="
# GET data
echo "fetch_url_gzip -bytes -url '$URL' > ${DATA}"
fetch_url_gzip -url "$URL" > "$DATA"


# Generate config
echo "xmlprobe -i $DATA -generate-config -config xx -pretty > ${CONFIG}"
#xmlprobe -i "$DATA" -generate-config -config xx -pretty > "${CONFIG}"
echo

# Process data
echo "xmlprobe -i '${DATA}' -config '${CONFIG}' -pretty"
#xmlprobe -i "$DATA" -config "$CONFIG" -pretty >> ico-${location}-${YEAR}
#rm "$DATA"
########################
exit
########################


