from asyncio import run
from os import environ

from dotenv import load_dotenv

from cassandra_2026.systems.migrator.service import CassandraExecutor, MigratorService


async def main():
    load_dotenv()

    contact_points = environ["CONTACT_POINTS"].split(",")
    port = int(environ["PORT"])
    keyspace = environ["KEYSPACE"]
    dir = environ["MIGRATION_DIR"]

    cass_executor = CassandraExecutor(contact_points=contact_points,
                                      port=port,
                                      keyspace=keyspace)

    cass_executor.initialize()

    service = MigratorService(executor=cass_executor, migration_dir=dir)
    # await service.rollback_last()

    await service.upgrade_head()
    cass_executor.shutdown()



if __name__ == '__main__':
    run(main())
