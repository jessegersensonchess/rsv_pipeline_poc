#!/usr/bin/env sh
#set -euxo pipefail

#### DESCRIPTION: verifies 
# etl and probe compile
# probe generates a usable config
# etl can run the config and output valid metrics
# it is looking for this metric to have a value of 2 in the pushgateway/metrics API:
# etl_records_total{instance="",job="sample",kind="inserted"} 2

# Run from e2e/ directory
cd "$(dirname "$0")"

# 1 & 2. Build etl & probe via docker compose build
docker compose build

# 3. Use probe to generate config from CSV into /configs/pipeline.json
docker compose run --rm etl-probe -c '
#  set -eux
NAME="sample"
  # Probe: generate JSON config from local CSV.
  # Adjust flags to match your probe CLI.
  probe \
    -url="file:///data/sample.csv" \
    -name="$NAME" \
    -bytes=8192 \
    -backend=sqlite \
    -pretty | sed -e "s/sample.csv/\/data\/sample.csv/g" > /configs/pipeline.json
  ls /data

  cat /configs/pipeline.json

  # install curl 
  apk add curl > /dev/null

# 4. delete old pushgateway metrics
for i in $(curl -s pushgateway:9091/metrics | grep -o job=.* | cut -f2 -d\" | sort | uniq); do
		echo $i
		curl -X DELETE http://pushgateway:9091/metrics/job/$i
done


# 5. Run ETL with generated config; SQLite DB will be created in /data/db/etl.db
  etl -config /configs/pipeline.json

# 6. Verify metrics in Pushgateway.

if [[ $(curl -s -o - http://pushgateway:9091/metrics | grep "$NAME" | grep inserted | cut -f2 -d" ") -eq 2 ]]
then
	# echo "E2E test passed."
	exit 0
else
	# echo "E2E test failed."
	exit 1
fi

' 
result="$?"

docker compose down

echo ""
if [ $result -eq 0 ]
then
	echo "E2E test passed."
else
	echo "E2E test failed."
fi
echo ""
exit "$result"
