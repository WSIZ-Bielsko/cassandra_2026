import asyncio
import os
from uuid import UUID

from cassandra.cluster import Cluster, Session
from dotenv import load_dotenv

from cassandra_2026.systems.filez.model import StoredFile

load_dotenv()


def execute_async_awaitable(session: Session, query, parameters=None):
    loop = asyncio.get_event_loop()
    future = loop.create_future()

    cassandra_future = session.execute_async(query, parameters)

    def on_success(result):
        loop.call_soon_threadsafe(future.set_result, result)

    def on_error(exception):
        loop.call_soon_threadsafe(future.set_exception, exception)

    cassandra_future.add_callbacks(on_success, on_error)
    return future


def get_cluster_session():
    contact_points = [
        h.strip() for h in os.getenv("CASSANDRA_CONTACT_POINTS", "").split(",")
    ]
    port = int(os.getenv("CASSANDRA_PORT", "9044"))
    keyspace = os.getenv("CASSANDRA_KEYSPACE", "filez")

    cluster = Cluster(contact_points=contact_points, port=port)
    session = cluster.connect(keyspace)
    return cluster, session


class FileRepository:
    def __init__(self, session: Session):
        self.session = session
        self.insert_stmt = session.prepare(
            "INSERT INTO files (file_id, author_id, filename, created_at, content) VALUES (?, ?, ?, ?, ?)"
        )
        self.get_by_id_stmt = session.prepare("SELECT * FROM files WHERE file_id = ?")
        self.get_meta_by_id_stmt = session.prepare(
            "SELECT file_id, author_id, filename, created_at FROM files WHERE file_id = ?"
        )
        self.get_by_author_stmt = session.prepare(
            "SELECT file_id, author_id, filename, created_at FROM files WHERE author_id = ?"
        )

    @staticmethod
    def _to_stored_file(row) -> StoredFile:
        return StoredFile(**row._asdict())

    async def insert_file(self, file: StoredFile) -> None:
        await execute_async_awaitable(
            self.session,
            self.insert_stmt,
            (
                file.file_id,
                file.author_id,
                file.filename,
                file.created_at,
                file.content,
            ),
        )

    async def get_file_by_id(self, file_id: UUID) -> StoredFile | None:
        result = await execute_async_awaitable(
            self.session, self.get_by_id_stmt, (file_id,)
        )
        rows = list(result)
        return self._to_stored_file(rows[0]) if rows else None

    async def get_file_meta_by_id(self, file_id: UUID) -> StoredFile | None:
        result = await execute_async_awaitable(
            self.session, self.get_meta_by_id_stmt, (file_id,)
        )
        rows = list(result)
        return self._to_file_meta(rows[0]) if rows else None

    @staticmethod
    def _to_file_meta(row) -> StoredFile:
        d = row._asdict()
        d["content"] = None
        return StoredFile(**d)

    async def get_files_by_author(self, author_id: UUID) -> list[StoredFile]:
        result = await execute_async_awaitable(
            self.session, self.get_by_author_stmt, (author_id,)
        )
        return [self._to_file_meta(row) for row in result]
