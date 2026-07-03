CREATE KEYSPACE IF NOT EXISTS benchstore
WITH replication = {
  'class': 'NetworkTopologyStrategy',
  'datacenter1': 1
}
AND durable_writes = true;

use  benchstore;

CREATE TABLE version(
    version text PRIMARY KEY,
    level int
);

insert into version(version, level) VALUES ('START', 1);

