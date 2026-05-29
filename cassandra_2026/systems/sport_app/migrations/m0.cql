-- Use NetworkTopologyStrategy for production environments
CREATE KEYSPACE IF NOT EXISTS running_stats
WITH REPLICATION = {
    'class': 'NetworkTopologyStrategy',
    'datacenter1': 3
};

USE running_stats;

CREATE TABLE IF NOT EXISTS user_records (
    username text PRIMARY KEY,
    best_5km float,
    best_10km float
);