# how to install cassandra cluster with docker swarm


## prep
- have cluster (docker swarm)
- `docker node ls` - see node IDs
- add labels, e.g. (using IDs)
```
docker node update --label-add cassandra=node1 j4l
docker node update --label-add cassandra=node2 ycyy
docker node update --label-add cassandra=node3 ti
```


## brief ideas
- comment out node-2 and node-3
- start node-1
- wait for cassandra to be up (will take 2 minutes)
- start node-2
- wait ...
- start node-3
- wait ...

## diagnostics

```
docker exec -it {container_id} nodetool status
docker service logs -f cassandra_stack_cassandra-1 
```

proper status of cluster:
```
Datacenter: dc1
===============
Status=Up/Down
|/ State=Normal/Leaving/Joining/Moving
--  Address    Load        Tokens  Owns  Host ID                               Rack 
UN  10.0.3.13  323.98 KiB  16      ?     6a8f1d4d-d479-44ef-9f79-133b69f79dc7  rack1
UN  10.0.3.14  289.97 KiB  16      ?     66a00e01-9390-48b6-9181-7c79cc21507c  rack1
UN  10.0.3.15  322.65 KiB  16      ?     bec3387c-d0ad-4b09-9916-a1bf4e588052  rack1

Note: Non-system keyspaces don't have the same replication settings, effective ownership information is meaningless
```