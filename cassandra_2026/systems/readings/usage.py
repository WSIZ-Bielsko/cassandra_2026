import random
from uuid import uuid4

from cassandra.cluster import Cluster
from loguru import logger

from repository import ReadingRepository
from models import Reading
from datetime import datetime, timezone, timedelta

POLISH_CITIES = ["Warsaw", "Kraków", "Łódź", "Wrocław", "Poznań"]

def random_reading() -> Reading:
    return Reading(
        id=uuid4(),
        city=random.choice(POLISH_CITIES),
        created=datetime.now(timezone.utc) - timedelta(days=random.uniform(0, 365)),
        value=random.uniform(0.0, 1.0),
    )

def add_readings(n_readings: int, repo) -> list[Reading]:
    readings = [random_reading() for _ in range(n_readings)]
    for r in readings:
        repo.insert(r)
    return readings


if __name__ == '__main__':
    URL = "10.10.1.225"
    PORT = 9042
    cluster = Cluster([URL], port=PORT,)
    session = cluster.connect()

    repo = ReadingRepository(session, keyspace="sensor_ks")

    # single "insert"
    # r = Reading(city="Warsaw", created=datetime.now(timezone.utc), value=0.42)
    # repo.insert(r)

    logger.info(f"Readings in DB: {repo.count()}")
    logger.info("querying for time range")
    repo.find_by_time_range(datetime(2025, 1, 1, tzinfo=timezone.utc), datetime(2025, 2, 1, tzinfo=timezone.utc))
    logger.info("querying for city")
    res = repo.find_by_city("Warsaw")
    logger.info(f"readings from warsaw: {res}")
    logger.info("querying for value range")
    repo.find_by_value_range(0.3, 0.6)
    logger.info('queries done')

    logger.info('adding readings')
    add_readings(100, repo)
    logger.info('adding readings complete')

    # updated = repo.update(r, value=0.75)
    # repo.delete(updated)
