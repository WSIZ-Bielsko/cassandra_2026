import asyncio
import uuid
from datetime import datetime, timedelta, UTC
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement


# Helper function to convert Cassandra's ResponseFuture to an asyncio Future
def execute_async_awaitable(session, query, parameters=None):
    loop = asyncio.get_event_loop()
    future = loop.create_future()

    cassandra_future = session.execute_async(query, parameters)

    def on_success(result):
        loop.call_soon_threadsafe(future.set_result, result)

    def on_error(exception):
        loop.call_soon_threadsafe(future.set_exception, exception)

    cassandra_future.add_callbacks(on_success, on_error)
    return future


async def main():
    # 1. Connect to Cassandra
    # 1.1 Connect to the 3-node Cassandra cluster on port 9044
    contact_points = ['10.10.1.231', '10.10.1.232', '10.10.1.233']


    # Initialize the cluster with the nodes and the custom port
    # cluster = Cluster(['127.0.0.1'])
    cluster = Cluster(
        contact_points=contact_points,
        port=9044,
        # Optional but recommended: ensures your queries distribute properly
        # load_balancing_policy=DCAwareRoundRobinPolicy(local_dc='dc1')
    )

    KS = 'filez' # hide in .env
    session = cluster.connect(f'{KS}')

    # Prepare statements for better performance
    insert_stmt = session.prepare(
        "INSERT INTO files (file_id, author_id, filename, created_at, content) VALUES (?, ?, ?, ?, ?)"
    )
    get_by_id_stmt = session.prepare("SELECT * FROM files WHERE file_id = ?")
    get_by_author_stmt = session.prepare("SELECT * FROM files WHERE author_id = ?")
    # Note: range queries with SAI can be prepared too
    get_by_time_range_stmt = session.prepare(
        "SELECT * FROM files WHERE created_at >= ? AND created_at <= ?"
    )

    # 2. Example Data
    file_id = uuid.uuid4()
    author_id = uuid.uuid4()
    now = datetime.now(UTC)
    file_content = b"Hello, Cassandra 5.0 with SAI!!"

    # 3. Store a File (Async Insert)
    print("Storing file...")
    await execute_async_awaitable(
        session,
        insert_stmt,
        (file_id, author_id, "example.txt", now, file_content)
    )

    # 4. Query 1: Retrieve by file_id (Primary Key)
    print("\nRetrieving by file_id:")
    result_id = await execute_async_awaitable(session, get_by_id_stmt, (file_id,))
    for row in result_id:
        print(f"Found: {row.filename} by {row.author_id}")

    # 5. Query 2: Retrieve by author_id (Using SAI)
    print("\nRetrieving by author_id:")
    result_author = await execute_async_awaitable(session, get_by_author_stmt, (author_id,))
    for row in result_author:
        print(f"Found: {row.filename} created at {row.created_at}")

    # 6. Query 3: Retrieve by created_at range (Using SAI)
    print("\nRetrieving by time range:")
    start_time = now - timedelta(hours=10)
    end_time = now + timedelta(hours=1)

    result_range = await execute_async_awaitable(
        session,
        get_by_time_range_stmt,
        (start_time, end_time)
    )
    for row in result_range:
        print(f"Found in range: {row.filename} (ID: {row.file_id})")

    cluster.shutdown()


if __name__ == "__main__":
    asyncio.run(main())