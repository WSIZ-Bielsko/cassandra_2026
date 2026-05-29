


from cassandra.cluster import Cluster
from os import getenv
from dotenv import load_dotenv
from pydantic import BaseModel

load_dotenv()

# Connect to cluster

# endpoint = '10.10.1.225:9050'

# cluster = Cluster([endpoint])
cluster = Cluster(['10.10.1.225'], port=9050)

session = cluster.connect()

# Execute CQL
# session.execute("""
#     CREATE KEYSPACE IF NOT EXISTS filex
#     WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
#     AND durable_writes = true;
# """)


session.execute("""
   DROP KEYSPACE IF EXISTS filex;
""")



class Migration(BaseModel):
    name: str
    up_cql: str
    down_cql: str
    id: str
    prev_id: str
    nexst_id: str

