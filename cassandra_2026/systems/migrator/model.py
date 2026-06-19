import os

from pydantic import BaseModel
from loguru import logger


class Migration(BaseModel):
    name: str
    up_script: str
    down_script: str
    level: int
    # id's below are ~6 letter strings
    id: str
    prev_id: str
    next_id: str



class MigrationError(Exception):
    pass


class Version(BaseModel):
    version: str
    level: int

def save_migration(path: str, filename: str, migration: Migration):
    full_path = os.path.join(path, filename)
    logger.info(f"Saving migration {migration.id} to {full_path}")
    with open(full_path, 'w') as f:
        f.write(migration.model_dump_json(indent=2))
    logger.info(f"Migration {migration.id} saved")


def load_migration(path: str, filename: str) -> Migration:
    full_path = os.path.join(path, filename)
    with open(full_path, 'r') as f:
        return Migration.model_validate_json(f.read())


def get_migration_list(path: str) -> list[Migration]:
    """
    Return a list of migr_test sorted by level
    :param path:
    :return:
    """
    return sorted(
        [load_migration(path, f) for f in os.listdir(path) if f.startswith('m') and f.endswith(".json")],
        key=lambda m: m.level
    )


def get_migration_by_id(path: str, id: str) -> Migration | None:
    for m in get_migration_list(path):
        if m.id == id:
            return m
    return None

def check_consistency(migrations: list[Migration]):
    """
    Raises MigrationError if migration[n+1].prev_id != migration[n].id or migration[n].next_id != migration[n+1].id
    :param migrations:
    :return:
    """
    for x in range(len(migrations) - 1):
        if migrations[x + 1].prev_id != migrations[x].id:
            raise MigrationError(f"Migrations {migrations[x].id} and {migrations[x + 1].id} are not consistent")
        if migrations[x].next_id != migrations[x + 1].id:
            raise MigrationError(f"Migrations {migrations[x].id} and {migrations[x + 1].id} are not consistent")


class Plan(BaseModel):
    migrations: list[Migration]
    direction: str


def plan_migrations(path: str, last_executed_migration_id: str, target_migration: str) -> Plan:
    """
    For "UP" migr_test (M1,M3): ["M2", "M3"] will be returned.
    For "DOWN" migr_test: (M3, M1): ["M3", "M2"] will be returned.

    :param path:
    :param last_executed_migration_id:
    :param target_migration:
    :param target_level:
    :return:
    """
    m_src = get_migration_by_id(path, last_executed_migration_id)
    m_tgt = get_migration_by_id(path, target_migration)
    if m_src is None:
        raise MigrationError(f"Migration {last_executed_migration_id} not found")
    if m_tgt is None:
        raise MigrationError(f"Migration {target_migration} not found")
    if m_src.level == m_tgt.level:
        raise MigrationError(f"Migration {last_executed_migration_id} and {target_migration} are at the same level")

    mm = get_migration_list(path)

    check_consistency(mm)

    src_idx = mm.index(m_src)
    tgt_idx = mm.index(m_tgt)

    logger.info(f"src_idx={src_idx}, tgt_idx={tgt_idx}")

    if m_tgt.level > m_src.level:
        # start isn't included, target included; execute all returned migr_test.up_script
        return Plan(migrations=mm[src_idx + 1: tgt_idx + 1], direction='UP')
    else:
        # target isn't included, start included; execute all returned migr_test.down_script
        return Plan(migrations=mm[tgt_idx+1: src_idx + 1][::-1], direction='DOWN')



def test_zero():
    PATH = "db_2026/migrator/db"
    mm = get_migration_list(PATH)
    for m in mm:
        print(m.model_dump())

def test_by_id():
    PATH = "db_2026/migrator/db"
    m = get_migration_by_id(PATH, "M1")
    assert m is not None
    print(m.model_dump())

def test_plan():
    PATH = "db_2026/migrator/db"
    print('----' * 5)
    for m in plan_migrations(PATH, "M1", "M3").migrations:
        print(m.model_dump())

    print('----' * 5)
    for m in plan_migrations(PATH, "M3", "M1").migrations:
        print(m.model_dump())



if __name__ == '__main__':
    pass
