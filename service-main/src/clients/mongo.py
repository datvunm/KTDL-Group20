from functools import lru_cache
from typing import Any
from pymongo import MongoClient
from pymongo.database import Database

from src.core.config import settings


@lru_cache(maxsize=1)
def get_client() -> MongoClient:
    uri = (
        f"mongodb://{settings.mongo_user}:{settings.mongo_password}"
        f"@{settings.mongo_host}:{settings.mongo_port}/"
    )
    return MongoClient(uri)


def get_db() -> Database[Any]:
    client = get_client()
    return client[settings.mongo_db]
