use sensor_ks;

-- time range queries: bucket prevents unbounded partitions
CREATE TABLE readings_by_time (
    day     date,
    created timestamp,
    id      uuid,
    city    text,
    value   float,
    PRIMARY KEY (day, created, id)
) WITH CLUSTERING ORDER BY (created ASC);

-- city queries (exact match) + optional created range clustering
CREATE TABLE readings_by_city (
    city    text,
    created timestamp,
    id      uuid,
    value   float,
    PRIMARY KEY (city, created, id)
) WITH CLUSTERING ORDER BY (created ASC);

-- value range: bucket into 10 bands (0.0–0.1 → 0, etc.)
CREATE TABLE readings_by_value (
    value_bucket int,   -- floor(value * 10)
    value        float,
    id           uuid,
    city         text,
    created      timestamp,
    PRIMARY KEY (value_bucket, value, id)
);

