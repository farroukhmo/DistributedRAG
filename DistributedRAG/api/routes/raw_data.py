"""
Raw Data Endpoints
Fetch raw scraped data from MongoDB
"""

from fastapi import APIRouter, HTTPException, Query
from pymongo import MongoClient
from typing import List, Optional
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


@router.get("/")
async def get_all_raw_data(
    limit: int = Query(10, ge=1, le=100, description="Number of items to return"),
    skip: int = Query(0, ge=0, description="Number of items to skip"),
    author: Optional[str] = Query(None, description="Filter by author")
):
    """
    Get all raw scraped data with pagination

    - **limit**: Maximum number of items (1-100)
    - **skip**: Number of items to skip
    - **author**: Filter by author name (optional)
    """
    try:
        db = get_db()
        collection = db[COLLECTION_NAME]

        # Build query
        query = {}
        if author:
            query["author"] = {"$regex": author, "$options": "i"}

        # Get total count
        total_count = collection.count_documents(query)

        # Fetch data
        cursor = collection.find(query).skip(skip).limit(limit)
        items = []

        for doc in cursor:
            # Convert ObjectId to string
            doc["_id"] = str(doc["_id"])
            items.append(doc)

        return {
            "total": total_count,
            "limit": limit,
            "skip": skip,
            "count": len(items),
            "data": items
        }

    except Exception as e:
        logger.error(f"Error fetching raw data: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{item_id}")
async def get_raw_data_by_id(item_id: str):
    """
    Get specific raw data item by ID

    - **item_id**: MongoDB ObjectId of the item
    """
    try:
        from bson import ObjectId

        db = get_db()
        collection = db[COLLECTION_NAME]

        # Find item
        item = collection.find_one({"_id": ObjectId(item_id)})

        if not item:
            raise HTTPException(status_code=404, detail="Item not found")

        # Convert ObjectId to string
        item["_id"] = str(item["_id"])

        return {
            "success": True,
            "data": item
        }

    except Exception as e:
        logger.error(f"Error fetching item {item_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stats/summary")
async def get_raw_data_stats():
    """
    Get statistics about raw scraped data
    """
    try:
        db = get_db()
        collection = db[COLLECTION_NAME]

        # Total documents
        total_docs = collection.count_documents({})

        # Count by author
        authors_pipeline = [
            {"$group": {"_id": "$author", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 10}
        ]
        top_authors = list(collection.aggregate(authors_pipeline))

        # Count by tags
        tags_pipeline = [
            {"$unwind": "$tags"},
            {"$group": {"_id": "$tags", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 10}
        ]
        top_tags = list(collection.aggregate(tags_pipeline))

        return {
            "total_documents": total_docs,
            "top_authors": top_authors,
            "top_tags": top_tags,
            "collection_name": COLLECTION_NAME,
            "database_name": DB_NAME
        }

    except Exception as e:
        logger.error(f"Error fetching stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/author/{author_name}")
async def get_by_author(
    author_name: str,
    limit: int = Query(10, ge=1, le=100)
):
    """
    Get all quotes by specific author

    - **author_name**: Author name to search for
    - **limit**: Maximum number of items
    """
    try:
        db = get_db()
        collection = db[COLLECTION_NAME]

        # Case-insensitive search
        query = {"author": {"$regex": author_name, "$options": "i"}}

        cursor = collection.find(query).limit(limit)
        items = []

        for doc in cursor:
            doc["_id"] = str(doc["_id"])
            items.append(doc)

        if not items:
            raise HTTPException(
                status_code=404,
                detail=f"No quotes found for author: {author_name}"
            )

        return {
            "author": author_name,
            "count": len(items),
            "data": items
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching by author: {e}")
        raise HTTPException(status_code=500, detail=str(e))