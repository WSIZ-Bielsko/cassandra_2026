

- how to copy data from table to local csv ??
```
COPY sensor_ks.readings_by_value (value_bucket, city, value, id, created)
TO '/tmp/readings_by_value.csv';
```
- 