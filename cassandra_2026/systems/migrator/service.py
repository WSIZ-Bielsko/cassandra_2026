from abc import ABC
from asyncio import run
from os import environ
from time import sleep
from typing import Any

# import asyncpg
# from asyncpg import UndefinedTableError

from cassandra.cluster import Cluster, Session


from dotenv import load_dotenv
from loguru import logger

from cassandra_2026.systems.migrator.model import Version, Migration, plan_migrations, MigrationError, get_migration_by_id, \
    get_migration_list


class QueryExecutor(ABC):

    def initialize(self):
        """establish appropriate connections"""

    def shutdown(self):
        """close connections"""

    async def execute_script(self, script: str) -> Any:
        """execute a script; return value if the script returns a value"""


    async def get_version(self) -> Version:
        """get the current version of the database"""


    async def update_version(self, version: Version):
        """update the current version of the database"""



class CassandraExecutor(QueryExecutor):

    def __init__(self, contact_points: list[str], port: int, keyspace: str):
        self.contact_points = contact_points
        self.port = port
        self.keyspace = keyspace


        self.cluster: Cluster = None
        self.session: Session = None


    def initialize(self):
        logger.info(f'connecting to {self.contact_points}:{self.port}/{self.keyspace}')
        self.cluster = Cluster(contact_points=self.contact_points, port=self.port)
        self.session = self.cluster.connect(self.keyspace)
        logger.info(f'connected to {self.contact_points}:{self.port}/{self.keyspace}')

    def shutdown(self):
        logger.info(f'disconnecting from {self.contact_points}:{self.port}/{self.keyspace}')
        self.session.shutdown()
        self.cluster.shutdown()
        logger.info(f'disconnected from {self.contact_points}:{self.port}/{self.keyspace}')

    async def execute_script(self, script: str) -> Any:
        logger.info(f'executing script on keyspace {self.keyspace}')
        return self.session.execute(script)

    async def get_version(self) -> Version:
        """get the current version of the database"""

        res = await self.execute_script('select version, level from version limit 1')
        return Version(version=res[0][0], level=res[0][1])

    async def update_version(self, version: Version):
        """update the current version of the database"""
        await self.execute_script('TRUNCATE version')
        await self.execute_script(f"INSERT INTO version (version, level) VALUES ('{version.version}', {version.level})")
        logger.info(f'version updated to {version.version}')


class MigratorService:

    def __init__(self, migration_dir: str, executor: QueryExecutor):
        self.migration_dir = migration_dir
        self.executor = executor


    async def __execute_script(self, script: str) -> Any:
        return await self.executor.execute_script(script)

    async def current_db_version(self) -> Version:
        return await self.executor.get_version()

    async def migrate(self, migration: Migration, direction: str):
        m = migration # alias
        current_version = await self.current_db_version()
        logger.info(f'running migration: {m.name}, id: {m.id}, direction: {direction}')

        if direction == 'UP':
            if current_version.version != m.prev_id:
                raise MigrationError(f"Migration {m.id} is not valid. "
                                     f"Current version is {current_version.version}, migration requires {m.prev_id}")
            await self.__execute_script(m.up_script)
            logger.info(f"Migration {m.id} applied ({direction})")

            await self.executor.update_version(Version(version=m.id, level=m.level))

            logger.debug(f"Version updated to {m.id}")
        elif direction == 'DOWN':
            if current_version.version != m.id:
                raise MigrationError(f"Migration {m.id} is not valid. "
                                     f"Current version is {current_version.version}, migration requires {m.id}")
            await self.__execute_script(m.down_script)
            logger.info(f"Migration {m.id} applied ({direction})")
            await self.executor.update_version(Version(version=m.prev_id, level=m.level-1))
            logger.debug(f"Version updated to {m.prev_id}")
        else:
            raise MigrationError(f"Invalid direction: {direction}")

    async def rollback_last(self):
        current_version = await self.current_db_version()
        if current_version.version == 'START':
            logger.warning("Cannot rollback from START version")
            return
        last_migration = get_migration_by_id(path=self.migration_dir, id=current_version.version)
        if not last_migration:
            raise MigrationError(f"Migration {current_version.version} not found")
        await self.migrate(last_migration, direction='DOWN')
        logger.info(f"Rolled back last migration: {current_version.version}")

    async def upgrade_head(self):
        """
        Executes all necessary migrations to bring DB to the highest level (as defined by files in self.migration_dir).
        :return:
        """


        current_version = await self.current_db_version()
        last_migration = get_migration_list(self.migration_dir)[-1]

        plan = plan_migrations(path=self.migration_dir,
                               last_executed_migration_id=current_version.version,
                               target_migration=last_migration.id)
        logger.info(f'executing all migrations up to head: {last_migration.id} / {last_migration.level} /{last_migration.name}')
        for m in plan.migrations:
            await self.migrate(m, direction=plan.direction)




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
    # logger.debug(f'version: {await cass_executor.get_version()}')
    # await cass_executor.update_version(Version(version='START', level=0))
    # logger.debug(f'version: {await cass_executor.get_version()}')
    # sleep(1)


    service = MigratorService(executor=cass_executor, migration_dir=dir)
    # await service.connect()
    # ver = await service.current_db_version()
    # logger.info(f"Current database version: {ver}")

    # MIGRATE SELECTIVCE
    # plan = plan_migrations(path=migration_dir, last_executed_migration_id='START', target_migration='M3')
    #
    # for m in plan.migrations:
    #     await service.migrate(m, direction=plan.direction)

    # await service.rollback_last()
    # await service.rollback_last()
    # await service.rollback_last()

    await service.upgrade_head()
    cass_executor.shutdown()



if __name__ == '__main__':
    run(main())
