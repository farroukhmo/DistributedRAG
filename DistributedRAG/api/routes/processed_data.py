"""
Processed Data Endpoints - Simplified
Query data without RAG integration
"""

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import Optional
from pymongo import MongoClient
import logging

logger = logging.getLogger(__name__)

# Create router
router = APIRouter()

# MongoDB connection
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "scraped_data"
COLLECTION_NAME = "processed_items"


def get_db():
    """Get MongoDB database connection"""
    try:
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        return db
    except Exception as e:
        logger.error(f"MongoDB connection error: {e}")
        raise HTTPException(status_code=500, detail="Database connection failed")


# Request models
class QueryRequest(BaseModel):
    query: str
    n_results: Optional[int] = 5


@router.post("/query")
async def query_system(request: QueryRequest):
    """
    Query the system with natural language (Simple keyword search)
    """
    try:
        db = get_db()
        collection = db[COLLECTION_NAME]

        # Simple text search
        query = {"quote": {"$regex": request.query, "$options": "i"}}
        cursor = collection.find(query).limit(request.n_results)

        results = []
        for doc in cursor:
            doc["_id"] = str(doc["_id"])
            results.append(doc)

        return {
            "success": True,
            "query": request.query,
            "n_results": request.n_results,
            "results": results
        }

    except Exception as e:
        logger.error(f"Error querying system: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/author/{author_name}")
async def get_author_data(
    author_name: str,
    n_results: int = Query(5, ge=1, le=20)
):
    """
    Get data from specific author
    """
    try:
        db = get_db()
        collection = db[COLLECTION_NAME]

        query = {"author": {"$regex": author_name, "$options": "i"}}
        cursor = collection.find(query).limit(n_results)

        results = []
        for doc in cursor:
            doc["_id"] = str(doc["_id"])
            results.append(doc)

        return {
            "success": True,
            "author": author_name,
            "n_results": n_results,
            "data": results
        }

    except Exception as e:
        logger.error(f"Error getting author data: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/topic/{topic}")
async def get_topic_data(
    topic: str,
    n_quotes: int = Query(5, ge=1, le=20)
):
    """
    Get data about a topic
    """
    try:
        db = get_db()
        collection = db[COLLECTION_NAME]

        # Search in tags and quote
        query = {
            "$or": [
                {"tags": {"$regex": topic, "$options": "i"}},
                {"quote": {"$regex": topic, "$options": "i"}}
            ]
        }
        cursor = collection.find(query).limit(n_quotes)

        results = []
        for doc in cursor:
            doc["_id"] = str(doc["_id"])
            results.append(doc)

        return {
            "success": True,
            "topic": topic,
            "n_quotes": n_quotes,
            "data": results
        }

    except Exception as e:
        logger.error(f"Error getting topic data: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/health")
async def health_check():
    """
    Check health of system
    """
    try:
        db = get_db()
        collection = db[COLLECTION_NAME]

        mongo_count = collection.count_documents({})

        return {
            "status": "healthy",
            "components": {
                "mongodb": {
                    "status": "connected",
                    "documents": mongo_count
                }
            }
        }

    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return {
            "status": "unhealthy",
            "error": str(e)
        }