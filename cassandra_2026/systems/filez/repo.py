from uuid import UUID

from loguru import logger

from cassandra_2026.systems.filez.common import execute_async_awaitable
from cassandra_2026.systems.filez.model import StoredFile
from cassandra_2026.systems.filez.statements import *


class FileRepository:
    def __init__(self, session):
        self.session = session

        # prepare statements
        self.insert_stmt = insert_st(session)
        self.get_by_id_stmt = get_by_id_st(session)
        self.get_by_author_stmt = get_by_author_st(session)
        self.get_by_time_range_stmt = get_by_time_range_st(session)



    async def insert_file(self, file: StoredFile):
        await execute_async_awaitable(
            self.session,
            self.insert_stmt,
            (file.file_id, file.author_id, file.filename, file.created_at, file.content)
        )

    async def get_file_by_id(self, file_id: UUID) -> StoredFile | None:
        logger.info(f'Getting file by id: {file_id}')
        file = await execute_async_awaitable(self.session, self.get_by_id_stmt, (file_id,))
        gg = file[0]
        print(gg.filename)
        print(type(gg))
        return StoredFile(**(gg._asdict())) if file else None

    async def get_files_by_author(self, author_id: UUID) -> list[StoredFile]:
        """
        :param author_id:
        :return: "lite" version of StoredFile (no content)
        """
        logger.info(f'Getting files by author: {author_id}')
        files = await execute_async_awaitable(self.session, self.get_by_author_stmt, (author_id,))
        return [StoredFile(**(file._asdict())) for file in files] if files else []