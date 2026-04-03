"""app/db/mongo.py — Singleton MongoDB client"""
from functools import lru_cache

from pymongo import MongoClient
from pymongo.database import Database

from app.core.config import settings


@lru_cache
def get_mongo() -> MongoClient:
    return MongoClient(settings.MONGO_URI)


def get_db() -> Database:
    return get_mongo()[settings.MONGO_DATABASE]
