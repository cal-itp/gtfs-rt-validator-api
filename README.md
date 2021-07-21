## Fetching example data

```
# puts data from a single feed (Commerce) into data/commerce/rt
python script/pull_example_agency.py


# puts bucket data into data/bucket/<timestamp>/<itp_id>/<url_number>/<filename>.pb
python script/pull_bucket_data.py
```

## Running validator

```
docker-compose run gtfs-rt-validator java \
    -jar /gtfs-realtime-validator-lib-1.0.0-SNAPSHOT.jar \
    -gtfs /app/data/commerce/gtfs.zip \
    -gtfsRealtimePath /app/data/commerce/rt
```
