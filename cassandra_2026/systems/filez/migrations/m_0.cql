CREATE KEYSPACE IF NOT EXISTS filex
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}
    AND durable_writes = true;