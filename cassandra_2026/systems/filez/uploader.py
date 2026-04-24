import os
from asyncio import run
from datetime import datetime
from uuid import uuid4, UUID

from loguru import logger

from cassandra_2026.systems.filez.common import get_cluster_session
from cassandra_2026.systems.filez.model import StoredFile
from cassandra_2026.systems.filez.repo import FileRepository


def save_file(file: StoredFile, dir='drive'):
    path = os.path.join(dir, file.filename)
    with open(path, 'wb') as f:
        f.write(file.content)


def load_file(filename, dir='drive'):
    path = os.path.join(dir, filename)
    with open(path, 'rb') as f:
        return StoredFile(file_id=uuid4(), author_id=uuid4(), filename=filename, content=f.read(),
                          created_at=datetime.now())




async def main():
    cluster, session = get_cluster_session()

    repo = FileRepository(session)


    # file = StoredFile(file_id=uuid4(), author_id=uuid4(),
    #                   filename='test.txt', content=b'Hello, world!', created_at=datetime.now())


    all_files_by_author = await repo.get_files_by_author(UUID('74a4a9d0-18e1-4b61-a761-6fe44e3f9d15'))
    # for f in all_files_by_author:
    #     print(f)
    # return


    file = load_file('harry.txt')
    await repo.insert_file(file)
    logger.info(f'File saved: {file.filename}')

    zz = await repo.get_file_by_id(file.file_id)
    save_file(zz, dir='drive')

    # save_file(file)
    # ff = load_file('test.txt')
    # print(ff)

    cluster.shutdown()

if __name__ == '__main__':
    run(main())



