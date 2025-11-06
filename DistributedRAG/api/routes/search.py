"""
Search Endpoints - Simplified
Search functionality without RAG
"""

from fastapi import APIRouter, HTTPException, Query
from pymongo import MongoClient
from typing import Optional
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


@router.get("/keyword")
async def keyword_search(
    q: str = Query(..., min_length=1, description="Search query"),
    limit: int = Query(10, ge=1, le=50),
    skip: int = Query(0, ge=0)
):
    """
    Keyword-based search in MongoDB
    """
    try:
        db = get_db()
        collection = db[COLLECTION_NAME]

        # Text search query
        query = {
            "$or": [
                {"quote": {"$regex": q, "$options": "i"}},
                {"author": {"$regex": q, "$options": "i"}},
                {"tags": {"$regex": q, "$options": "i"}}
            ]
        }

        # Count total matches
        total_count = collection.count_documents(query)

        # Fetch results
        cursor = collection.find(query).skip(skip).limit(limit)
        results = []

        for doc in cursor:
            doc["_id"] = str(doc["_id"])
            results.append(doc)

        return {
            "search_type": "keyword",
            "query": q,
            "total_matches": total_count,
            "returned": len(results),
            "limit": limit,
            "skip": skip,
            "results": results
        }

    except Exception as e:
        logger.error(f"Keyword search error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/advanced")
async def advanced_search(
    q: str = Query(..., min_length=1, description="Search query"),
    author: Optional[str] = Query(None, description="Filter by author"),
    tags: Optional[str] = Query(None, description="Filter by tags"),
    limit: int = Query(10, ge=1, le=50)
):
    """
    Advanced search with filters
    """
    try:
        db = get_db()
        collection = db[COLLECTION_NAME]

        # Build query
        query = {
            "$or": [
                {"quote": {"$regex": q, "$options": "i"}},
                {"author": {"$regex": q, "$options": "i"}}
            ]
        }

        # Add filters
        if author:
            query["author"] = {"$regex": author, "$options": "i"}

        if tags:
            tag_list = [t.strip() for t in tags.split(",")]
            query["tags"] = {"$in": tag_list}

        # Fetch results
        cursor = collection.find(query).limit(limit)
        results = []

        for doc in cursor:
            doc["_id"] = str(doc["_id"])
            results.append(doc)

        return {
            "search_type": "keyword_advanced",
            "query": q,
            "filters": {
                "author": author,
                "tags": tags
            },
            "results": results
        }

    except Exception as e:
        logger.error(f"Advanced search error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tags")
async def search_by_tags(
    tags: str = Query(..., description="Comma-separated tags"),
    limit: int = Query(10, ge=1, le=50)
):
    """
    Search by specific tags
    """
    try:
        db = get_db()
        collection = db[COLLECTION_NAME]

        # Parse tags
        tag_list = [t.strip() for t in tags.split(",")]

        # Query
        query = {"tags": {"$in": tag_list}}

        # Fetch results
        cursor = collection.find(query).limit(limit)
        results = []

        for doc in cursor:
            doc["_id"] = str(doc["_id"])
            results.append(doc)

        return {
            "search_type": "tags",
            "tags": tag_list,
            "count": len(results),
            "results": results
        }

    except Exception as e:
        logger.error(f"Tag search error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stats")
async def search_stats():
    """Get search statistics"""
    try:
        db = get_db()
        collection = db[COLLECTION_NAME]

        total_docs = collection.count_documents({})
        authors = collection.distinct("author")
        all_tags = collection.distinct("tags")

        return {
            "total_documents": total_docs,
            "unique_authors": len(authors),
            "total_tags": len(all_tags),
            "available_authors": sorted(authors)[:20],
            "available_tags": sorted(all_tags)[:50]
        }

    except Exception as e:
        logger.error(f"Stats error: {e}")
        raise HTTPException(status_code=500, detail=str(e))