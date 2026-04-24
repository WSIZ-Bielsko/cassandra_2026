import os
from asyncio import Future, get_event_loop
from typing import Any

from cassandra.cluster import Cluster
from dotenv import load_dotenv


def get_cluster_session():
    load_dotenv()
    contact_points = os.getenv("CASSANDRA_CONTACT_POINTS", "").split(",")
    port = int(os.getenv("CASSANDRA_PORT", 9042))
    keyspace = os.getenv("CASSANDRA_KEYSPACE")

    cluster = Cluster(contact_points=contact_points, port=port)
    session = cluster.connect(keyspace)
    return cluster, session

# Helper function to convert Cassandra's ResponseFuture to an asyncio Future
def execute_async_awaitable(session, query, parameters=None) -> Future[Any]:
    loop = get_event_loop()
    future = loop.create_future()

    cassandra_future = session.execute_async(query, parameters)

    def on_success(result):
        loop.call_soon_threadsafe(future.set_result, result)

    def on_error(exception):
        loop.call_soon_threadsafe(future.set_exception, exception)

    cassandra_future.add_callbacks(on_success, on_error)
    return future
