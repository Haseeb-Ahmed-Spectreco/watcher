#!/usr/bin/env python3
"""
Enhanced MongoDB Polling Watcher with Badge Evaluation Collection
Works with standalone MongoDB instances - no replica set required
Automatically creates badge evaluation entries when changes are detected
"""

import asyncio
import logging
import os
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional, Callable
from motor.motor_asyncio import AsyncIOMotorClient
from bson import ObjectId
from dotenv import load_dotenv

load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class MongoDBPollingWatcher:
    """MongoDB polling watcher with badge evaluation collection support"""
    
    def __init__(self, mongo_uri: str = None, db_name: str = None):
        self.mongo_uri = mongo_uri or os.getenv("MONGO_URI", "mongodb://localhost:27017")
        self.db_name = db_name or os.getenv("MONGO_DB_NAME", "ensogove")
        
        self.client = None
        self.db = None
        self.running = False
        
        # Collections to watch
        self.watched_collections = {}  # collection_name -> config
        
        # Change handlers
        self.change_handlers = []
        
        # Polling configuration
        self.poll_interval = 30  # seconds
        self.batch_size = 1000   # documents per batch
        
        # Track last seen timestamps/ids for each collection
        self.last_seen = {}
        
        # Badge evaluation collection settings
        self.badge_evaluation_enabled = True
        self.badge_collection_name = "badge_evaluation_queue"
        
        # Statistics
        self.stats = {
            'total_changes': 0,
            'changes_by_collection': {},
            'changes_by_operation': {},
            'successful_extractions': 0,
            'failed_extractions': 0,
            'badge_entries_created': 0,
            'badge_entries_updated': 0,
            'polling_cycles': 0,
            'start_time': None
        }
    
    def enable_badge_evaluation(self, collection_name: str = "badge_evaluation_queue"):
        """Enable badge evaluation collection creation"""
        self.badge_evaluation_enabled = True
        self.badge_collection_name = collection_name
        logger.info(f"✅ Badge evaluation enabled - collection: {collection_name}")
    
    def disable_badge_evaluation(self):
        """Disable badge evaluation collection creation"""
        self.badge_evaluation_enabled = False
        logger.info("❌ Badge evaluation disabled")
    
    async def connect(self):
        """Connect to MongoDB"""
        try:
            self.client = AsyncIOMotorClient(self.mongo_uri)
            self.db = self.client[self.db_name]
            await self.client.admin.command('ping')
            logger.info(f"✅ Connected to MongoDB: {self.db_name} (Polling Mode)")
            
            # Create badge evaluation collection index if enabled
            if self.badge_evaluation_enabled:
                await self._setup_badge_collection()
                
        except Exception as e:
            logger.error(f"❌ Error connecting to MongoDB: {e}")
            raise
    
    async def _setup_badge_collection(self):
        """Setup badge evaluation collection with proper indexes"""
        try:
            badge_collection = self.db[self.badge_collection_name]
            
            # Check if unique index already exists
            existing_indexes = await badge_collection.list_indexes().to_list(None)
            unique_index_exists = any(
                idx.get('key') == {'company_id': 1, 'site_code': 1} and idx.get('unique') 
                for idx in existing_indexes
            )
            
            if not unique_index_exists:
                # Clean up any existing duplicates before creating unique index
                await self._cleanup_duplicates(badge_collection)
                
                # Create compound index on company_id and site_code for efficient upserts
                try:
                    await badge_collection.create_index([
                        ("company_id", 1),
                        ("site_code", 1)
                    ], unique=True, background=True)
                    logger.info("✅ Created unique compound index on (company_id, site_code)")
                except Exception as idx_error:
                    if "duplicate key error" in str(idx_error).lower():
                        logger.warning("⚠️ Duplicate entries found, cleaning up and retrying...")
                        await self._cleanup_duplicates(badge_collection)
                        # Try again after cleanup
                        await badge_collection.create_index([
                            ("company_id", 1),
                            ("site_code", 1)
                        ], unique=True, background=True)
                        logger.info("✅ Created unique compound index after cleanup")
                    else:
                        raise idx_error
            else:
                logger.info("✅ Unique compound index already exists")
            
            # Create other indexes (these can be created multiple times safely)
            await badge_collection.create_index([("status", 1)], background=True)
            await badge_collection.create_index([("created_at", 1)], background=True)
            
            logger.info(f"✅ Badge evaluation collection '{self.badge_collection_name}' setup complete")
            
        except Exception as e:
            logger.error(f"❌ Error setting up badge collection: {e}")
            # Don't raise the error - badge evaluation will still work, just less efficiently
            logger.warning("⚠️ Continuing without unique index - duplicates may occur")
    
    async def _cleanup_duplicates(self, badge_collection):
        """Remove duplicate entries keeping the most recent one"""
        try:
            logger.info("🧹 Cleaning up duplicate badge evaluation entries...")
            
            # Find duplicates and keep only the most recent one
            pipeline = [
                {
                    "$group": {
                        "_id": {
                            "company_id": "$company_id",
                            "site_code": "$site_code"
                        },
                        "docs": {"$push": "$ROOT"},
                        "count": {"$sum": 1}
                    }
                },
                {
                    "$match": {"count": {"$gt": 1}}
                }
            ]
            
            duplicates_removed = 0
            async for group in badge_collection.aggregate(pipeline):
                # Sort by updated_at descending and keep the first (most recent)
                docs = sorted(group["docs"], key=lambda x: x.get("updated_at", x.get("created_at", datetime.min.replace(tzinfo=timezone.utc))), reverse=True)
                
                # Remove all but the most recent
                ids_to_remove = [doc["_id"] for doc in docs[1:]]
                if ids_to_remove:
                    result = await badge_collection.delete_many({"_id": {"$in": ids_to_remove}})
                    duplicates_removed += result.deleted_count
            
            if duplicates_removed > 0:
                logger.info(f"🧹 Removed {duplicates_removed} duplicate badge evaluation entries")
            else:
                logger.info("✅ No duplicate entries found")
                
        except Exception as e:
            logger.error(f"❌ Error cleaning up duplicates: {e}")
            # Continue anyway
    
    async def _add_badge_evaluation_entry(self, company_id: str, site_code: str, source_info: Dict[str, Any]):
        """Add or update badge evaluation entry with affected collections list"""
        if not self.badge_evaluation_enabled or not company_id:
            return
        
        try:
            # Normalize site_code - convert None to empty string
            normalized_site_code = site_code if site_code is not None else ""
            
            badge_collection = self.db[self.badge_collection_name]
            
            now = datetime.now(timezone.utc)
            collection_name = source_info.get('collection_name', '')
            
            filter_query = {
                "company_id": company_id,
                "site_code": normalized_site_code
            }
            
            # ALWAYS use nuclear approach due to widespread document corruption
            logger.debug(f"🧨 Using nuclear approach for {company_id}:{normalized_site_code} (collection has structural issues)")
            success = await self._nuclear_approach_for_badge_document(
                badge_collection, filter_query, company_id, normalized_site_code, source_info, now
            )
            
            if success:
                self.stats['badge_entries_created'] += 1
                logger.debug(f"✅ Nuclear approach succeeded for {company_id}:{normalized_site_code}")
            else:
                logger.warning(f"⚠️  Nuclear approach failed for {company_id}:{normalized_site_code} - skipping badge evaluation")
                
        except Exception as e:
            logger.error(f"❌ Error adding badge evaluation entry for {company_id}:{site_code}: {e}")
            logger.debug(f"   Source info: {source_info}")
    
    async def _nuclear_approach_for_badge_document(self, badge_collection, filter_query, company_id, site_code, source_info, now):
        """Nuclear approach: delete any existing documents and create fresh - guaranteed to work"""
        try:
            collection_name = source_info.get('collection_name', '')
            
            # Step 1: Check if entry already exists
            existing_doc = await badge_collection.find_one(filter_query)
            
            if existing_doc:
                # Entry exists - update affected_collections list and metadata
                logger.debug(f"📋 Updating existing badge entry for {company_id}:{site_code}")
                
                # Get current affected collections list
                current_collections = existing_doc.get('affected_collections', [])
                if not isinstance(current_collections, list):
                    current_collections = [str(current_collections)] if current_collections else []
                
                # Add new collection if not already in the list
                if collection_name and collection_name not in current_collections:
                    current_collections.append(collection_name)
                    collections_updated = True
                    logger.debug(f"📋 Added {collection_name} to affected collections")
                else:
                    collections_updated = False
                    logger.debug(f"📋 Collection {collection_name} already in affected collections")
                
                # Prepare update document
                update_doc = {
                    "$set": {
                        "affected_collections": current_collections,
                        "updated_at": now,
                        "source": "mongodb",
                        "last_collection_change": collection_name,
                        "last_document_id": source_info.get('document_id'),
                        "metadata.last_detection_timestamp": now,
                        "metadata.last_source_operation": source_info.get('operation_type', 'update'),
                        "metadata.total_collections_affected": len(current_collections),
                        "metadata.collections_updated": collections_updated
                    }
                }
                
                # If status is completed, reset it to pending since we have new changes
                if existing_doc.get('status') == 'completed':
                    update_doc["$set"]["status"] = "pending"
                    update_doc["$set"]["metadata.status_reset_reason"] = "new_changes_detected"
                
                try:
                    # Delete the old document first (nuclear approach)
                    await badge_collection.delete_one(filter_query)
                    logger.debug(f"🗑️ Deleted existing document for {company_id}:{site_code}")
                    
                    # Create fresh document with updated data
                    fresh_doc = {
                        "company_id": company_id,
                        "site_code": site_code,
                        "status": existing_doc.get("status", "pending"),
                        "priority": existing_doc.get("priority", "normal"),
                        "source": "mongodb",
                        "affected_collections": current_collections,
                        "last_collection_change": collection_name,
                        "last_document_id": source_info.get('document_id'),
                        "created_at": existing_doc.get("created_at", now),
                        "updated_at": now,
                        "retry_count": existing_doc.get("retry_count", 0),
                        "metadata": {
                            "detected_from": "mongodb_watcher",
                            "first_detection_timestamp": existing_doc.get("metadata", {}).get("first_detection_timestamp", now),
                            "last_detection_timestamp": now,
                            "last_source_operation": source_info.get('operation_type', 'update'),
                            "total_collections_affected": len(current_collections),
                            "collections_updated": collections_updated,
                            "update_count": existing_doc.get("metadata", {}).get("update_count", 0) + 1
                        }
                    }
                    
                    # Reset status to pending if it was completed
                    if existing_doc.get('status') == 'completed':
                        fresh_doc["status"] = "pending"
                        fresh_doc["metadata"]["status_reset_reason"] = "new_changes_detected"
                    
                    await badge_collection.insert_one(fresh_doc)
                    self.stats['badge_entries_updated'] += 1
                    logger.debug(f"🔄 Updated badge entry for {company_id}:{site_code} - collections: {current_collections}")
                    return True
                    
                except Exception as update_error:
                    logger.error(f"❌ Failed to update existing badge entry: {update_error}")
                    return False
            else:
                # No existing entry - create new one
                logger.debug(f"🏗️ Creating new badge entry for {company_id}:{site_code}")
                
                # Create fresh document with affected_collections as list
                fresh_doc = {
                    "company_id": company_id,
                    "site_code": site_code,
                    "status": "pending",
                    "priority": "normal",
                    "source": "mongodb",
                    "affected_collections": [collection_name] if collection_name else [],
                    "last_collection_change": collection_name,
                    "last_document_id": source_info.get('document_id'),
                    "created_at": now,
                    "updated_at": now,
                    "retry_count": 0,
                    "metadata": {
                        "detected_from": "mongodb_watcher",
                        "first_detection_timestamp": now,
                        "last_detection_timestamp": now,
                        "last_source_operation": source_info.get('operation_type', 'update'),
                        "total_collections_affected": 1 if collection_name else 0,
                        "collections_updated": True,
                        "update_count": 1
                    }
                }
                
                try:
                    await badge_collection.insert_one(fresh_doc)
                    self.stats['badge_entries_created'] += 1
                    logger.debug(f"🏆 Created new badge entry for {company_id}:{site_code} - collection: {collection_name}")
                    return True
                except Exception as insert_error:
                    # Handle race condition - another process might have created it
                    if "duplicate key error" in str(insert_error).lower():
                        logger.debug(f"🔄 Race condition for {company_id}:{site_code}, retrying...")
                        await asyncio.sleep(0.1)
                        return await self._nuclear_approach_for_badge_document(
                            badge_collection, filter_query, company_id, site_code, source_info, now
                        )
                    else:
                        logger.error(f"❌ Failed to create new badge entry: {insert_error}")
                        return False
                
        except Exception as nuclear_error:
            logger.error(f"❌ Nuclear approach completely failed for {company_id}:{site_code}: {nuclear_error}")
            return False
    
    async def _handle_badge_document_safely(self, badge_collection, filter_query, company_id, site_code, source_info, now):
        """Safely handle badge document creation/update with fallback strategies"""
        
        # Strategy 1: Try to check if document exists and replace it
        try:
            existing_doc = await badge_collection.find_one(filter_query)
            
            if existing_doc:
                logger.debug(f"Found existing document for {company_id}:{site_code}")
                
                # Create clean replacement document
                replacement_doc = {
                    "company_id": company_id,
                    "site_code": site_code,
                    "status": existing_doc.get("status", "pending"),
                    "priority": existing_doc.get("priority", "normal"),
                    "source": "mongodb",
                    "source_collection": source_info.get('collection_name'),
                    "source_document_id": source_info.get('document_id'),
                    "created_at": existing_doc.get("created_at", now),
                    "updated_at": now,
                    "retry_count": existing_doc.get("retry_count", 0),
                    "metadata": {
                        "detected_from": "mongodb_watcher",
                        "source_operation": source_info.get('operation_type', 'update'),
                        "detection_timestamp": now
                    }
                }
                
                # Try replace_one first
                try:
                    result = await badge_collection.replace_one(filter_query, replacement_doc)
                    if result.modified_count > 0:
                        self.stats['badge_entries_updated'] += 1
                        return True
                except Exception as replace_error:
                    logger.warning(f"⚠️ replace_one failed for {company_id}:{site_code}: {replace_error}")
                    
                    # Fallback: Delete and recreate (nuclear option)
                    logger.info(f"🧨 Using nuclear option for corrupted document {company_id}:{site_code}")
                    
                    try:
                        # Delete the corrupted document
                        delete_result = await badge_collection.delete_one(filter_query)
                        if delete_result.deleted_count > 0:
                            logger.info(f"🗑️ Deleted corrupted document for {company_id}:{site_code}")
                            
                            # Insert clean document
                            await badge_collection.insert_one(replacement_doc)
                            self.stats['badge_entries_created'] += 1
                            logger.info(f"🏆 Recreated clean document for {company_id}:{site_code}")
                            return True
                        else:
                            logger.warning(f"⚠️ Could not delete document for {company_id}:{site_code}")
                            
                    except Exception as nuclear_error:
                        logger.error(f"❌ Nuclear option failed for {company_id}:{site_code}: {nuclear_error}")
                        return False
            else:
                # Document doesn't exist - create new one
                new_doc = {
                    "company_id": company_id,
                    "site_code": site_code,
                    "status": "pending",
                    "priority": "normal",
                    "source": "mongodb",
                    "source_collection": source_info.get('collection_name'),
                    "source_document_id": source_info.get('document_id'),
                    "created_at": now,
                    "updated_at": now,
                    "retry_count": 0,
                    "metadata": {
                        "detected_from": "mongodb_watcher",
                        "source_operation": source_info.get('operation_type', 'update'),
                        "detection_timestamp": now
                    }
                }
                
                try:
                    await badge_collection.insert_one(new_doc)
                    self.stats['badge_entries_created'] += 1
                    return True
                except Exception as insert_error:
                    if "duplicate key error" in str(insert_error).lower():
                        # Race condition - another process created it
                        logger.info(f"🔄 Race condition for {company_id}:{site_code}, retrying...")
                        await asyncio.sleep(0.1)
                        return await self._handle_badge_document_safely(
                            badge_collection, filter_query, company_id, site_code, source_info, now
                        )
                    else:
                        logger.error(f"❌ Insert failed for {company_id}:{site_code}: {insert_error}")
                        return False
                        
        except Exception as e:
            logger.error(f"❌ Strategy 1 failed for {company_id}:{site_code}: {e}")
            
            # Strategy 2: Nuclear approach - try to delete any existing document and create fresh
            logger.info(f"💣 Trying nuclear approach for {company_id}:{site_code}")
            
            try:
                # Delete any existing document (ignore result)
                await badge_collection.delete_many(filter_query)
                
                # Create fresh document
                fresh_doc = {
                    "company_id": company_id,
                    "site_code": site_code,
                    "status": "pending",
                    "priority": "normal",
                    "source": "mongodb",
                    "source_collection": source_info.get('collection_name'),
                    "source_document_id": source_info.get('document_id'),
                    "created_at": now,
                    "updated_at": now,
                    "retry_count": 0,
                    "metadata": {
                        "detected_from": "mongodb_watcher",
                        "source_operation": source_info.get('operation_type', 'update'),
                        "detection_timestamp": now
                    }
                }
                
                await badge_collection.insert_one(fresh_doc)
                self.stats['badge_entries_created'] += 1
                logger.info(f"💣 Nuclear approach succeeded for {company_id}:{site_code}")
                return True
                
            except Exception as nuclear_error:
                logger.error(f"❌ Nuclear approach failed for {company_id}:{site_code}: {nuclear_error}")
                return False
        
        return False
    
    async def disconnect(self):
        """Disconnect from MongoDB"""
        if self.client:
            self.client.close()
            logger.info("🔌 Disconnected from MongoDB")
    
    def watch_collection(self, collection_name: str, timestamp_field: str = None, 
                        id_field: str = '_id', company_id_field: str = 'company_id',
                        site_code_field: str = 'site_code'):
        """
        Add a collection to watch for changes
        
        Args:
            collection_name: Name of the collection to watch
            timestamp_field: Field to use for detecting changes (e.g. 'updated_at', 'modified_time')
            id_field: ID field (e.g. '_id', 'id') - used if no timestamp field
            company_id_field: Field containing company identifier
            site_code_field: Field containing site code
        """
        config = {
            'timestamp_field': timestamp_field,
            'id_field': id_field or '_id',
            'company_id_field': company_id_field,
            'site_code_field': site_code_field
        }
        
        self.watched_collections[collection_name] = config
        logger.info(f"📋 Watching collection: {collection_name} (timestamp: {timestamp_field}, id: {id_field})")
    
    def watch_collections(self, collection_configs: List[Dict[str, str]]):
        """
        Add multiple collections to watch
        
        Args:
            collection_configs: List of collection configurations
            Example: [
                {
                    'collection_name': 'kpi_data',
                    'timestamp_field': 'updated_at',
                    'company_id_field': 'company_id',
                    'site_code_field': 'site_code'
                }
            ]
        """
        for config in collection_configs:
            self.watch_collection(
                collection_name=config['collection_name'],
                timestamp_field=config.get('timestamp_field'),
                id_field=config.get('id_field'),
                company_id_field=config.get('company_id_field', 'company_id'),
                site_code_field=config.get('site_code_field', 'site_code')
            )
    
    def add_change_handler(self, handler: Callable):
        """Add a function to handle changes"""
        self.change_handlers.append(handler)
        logger.info(f"🔧 Added change handler: {handler.__name__}")
    
    def set_poll_interval(self, seconds: int):
        """Set polling interval in seconds"""
        self.poll_interval = seconds
        logger.info(f"⏰ Set polling interval to {seconds} seconds")
    
    async def start_watching(self):
        """Start watching all specified collections"""
        if not self.watched_collections:
            logger.error("❌ No collections specified to watch")
            return
        
        await self.connect()
        
        self.running = True
        self.stats['start_time'] = datetime.now(timezone.utc)
        
        logger.info(f"🚀 Starting to watch {len(self.watched_collections)} MongoDB collections (polling mode)...")
        logger.info(f"⏰ Polling every {self.poll_interval} seconds")
        logger.info(f"🏆 Badge evaluation: {'enabled' if self.badge_evaluation_enabled else 'disabled'}")
        
        # Initialize last seen values for each collection
        await self._initialize_last_seen()
        
        try:
            # Main polling loop
            while self.running:
                await self._poll_collections()
                
                # Show countdown for next poll
                logger.info(f"⏰ Next poll in {self.poll_interval} seconds...")
                await asyncio.sleep(self.poll_interval)
        except KeyboardInterrupt:
            logger.info("🛑 Received shutdown signal")
        finally:
            await self.stop_watching()
    
    async def stop_watching(self):
        """Stop watching all collections"""
        self.running = False
        await self.disconnect()
        logger.info("🛑 Stopped watching MongoDB collections")
    
    async def _initialize_last_seen(self):
        """Initialize last seen timestamps/ids for each collection"""
        for collection_name, config in self.watched_collections.items():
            try:
                collection = self.db[collection_name]
                
                # Get the latest timestamp or ID to start from
                if config['timestamp_field']:
                    # Find document with latest timestamp
                    cursor = collection.find().sort(config['timestamp_field'], -1).limit(1)
                    doc = await cursor.to_list(1)
                    
                    if doc:
                        self.last_seen[collection_name] = doc[0][config['timestamp_field']]
                        logger.info(f"📍 Initialized {collection_name} last seen: {self.last_seen[collection_name]}")
                    else:
                        # Collection is empty
                        self.last_seen[collection_name] = datetime.now(timezone.utc) - timedelta(minutes=1)
                        logger.info(f"📍 Initialized {collection_name} with default timestamp")
                else:
                    # Find document with latest ObjectId
                    cursor = collection.find().sort('_id', -1).limit(1)
                    doc = await cursor.to_list(1)
                    
                    if doc:
                        self.last_seen[collection_name] = doc[0]['_id']
                        logger.info(f"📍 Initialized {collection_name} last seen: {self.last_seen[collection_name]}")
                    else:
                        # Collection is empty
                        self.last_seen[collection_name] = ObjectId.from_datetime(datetime.now(timezone.utc) - timedelta(minutes=1))
                        logger.info(f"📍 Initialized {collection_name} with default ObjectId")
                        
            except Exception as e:
                logger.error(f"❌ Error initializing {collection_name}: {e}")
                # Set a default value
                if config['timestamp_field']:
                    self.last_seen[collection_name] = datetime.now(timezone.utc)
                else:
                    self.last_seen[collection_name] = ObjectId()
    
    async def _poll_collections(self):
        """Poll all watched collections for changes"""
        self.stats['polling_cycles'] += 1
        logger.info(f"📊 Polling cycle #{self.stats['polling_cycles']} - Checking {len(self.watched_collections)} collections...")
        
        for collection_name, config in self.watched_collections.items():
            try:
                await self._poll_collection(collection_name, config)
            except Exception as e:
                logger.error(f"❌ Error polling collection {collection_name}: {e}")
    
    async def _poll_collection(self, collection_name: str, config: Dict[str, str]):
        """Poll a single collection for changes"""
        try:
            collection = self.db[collection_name]
            last_seen = self.last_seen.get(collection_name)
            
            logger.info(f"🔍 Checking collection {collection_name} for changes since: {last_seen}")
            
            # Build query based on whether we're using timestamp or ID
            if config['timestamp_field']:
                # Use timestamp-based detection
                query = {config['timestamp_field']: {"$gt": last_seen}}
                sort_field = config['timestamp_field']
            else:
                # Use ObjectId-based detection
                query = {"_id": {"$gt": last_seen}}
                sort_field = "_id"
            
            # Get new documents
            cursor = collection.find(query).sort(sort_field, 1).limit(self.batch_size)
            documents = await cursor.to_list(self.batch_size)
            
            if documents:
                logger.info(f"🔄 Found {len(documents)} changes in collection {collection_name}")
                
                for doc in documents:
                    await self._process_change(collection_name, config, doc)
                
                # Update last seen value
                if config['timestamp_field']:
                    self.last_seen[collection_name] = documents[-1][config['timestamp_field']]
                else:
                    self.last_seen[collection_name] = documents[-1]['_id']
                
                logger.info(f"📍 Updated {collection_name} last seen: {self.last_seen[collection_name]}")
            else:
                logger.info(f"📊 No new changes in {collection_name} (last seen: {last_seen})")
                
        except Exception as e:
            logger.error(f"❌ Error polling collection {collection_name}: {e}")
    
    async def _process_change(self, collection_name: str, config: Dict[str, str], document: Dict[str, Any]):
        """Process a single changed document"""
        try:
            # Update statistics
            self.stats['total_changes'] += 1
            
            if collection_name not in self.stats['changes_by_collection']:
                self.stats['changes_by_collection'][collection_name] = 0
            self.stats['changes_by_collection'][collection_name] += 1
            
            # For MongoDB polling, we detect "changes" but can't distinguish operation type
            operation_type = 'update'  # Assume update since we're polling
            if operation_type not in self.stats['changes_by_operation']:
                self.stats['changes_by_operation'][operation_type] = 0
            self.stats['changes_by_operation'][operation_type] += 1
            
            # Extract company_id and site_code
            company_id, site_code = self._extract_identifiers(document, config)
            
            # Normalize site_code - convert None to empty string for consistency
            if site_code is None:
                site_code = ""
            
            if company_id:  # Only process if we have a company_id
                self.stats['successful_extractions'] += 1
                
                # Create source info for badge evaluation
                source_info = {
                    'collection_name': collection_name,
                    'document_id': document.get('_id'),
                    'operation_type': operation_type
                }
                
                # Add badge evaluation entry
                await self._add_badge_evaluation_entry(company_id, site_code, source_info)
                
                # Create change info (similar to change stream format)
                change_info = {
                    'collection_name': collection_name,
                    'operation_type': operation_type,
                    'company_id': company_id,
                    'site_code': site_code,
                    'document_key': {'_id': document.get('_id')},
                    'full_document': document,
                    'timestamp': datetime.now(timezone.utc),
                    'change_id': document.get('_id')
                }
                
                logger.info(f"✅ Extracted: {company_id}:{site_code} from {collection_name}")
                
                # Call all registered handlers
                for handler in self.change_handlers:
                    try:
                        await handler(change_info)
                    except Exception as e:
                        logger.error(f"❌ Error in change handler {handler.__name__}: {e}")
            else:
                self.stats['failed_extractions'] += 1
                logger.warning(f"⚠️  Could not extract company_id from {collection_name}")
                logger.debug(f"Document fields: {list(document.keys())}")
                
        except Exception as e:
            logger.error(f"❌ Error processing change: {e}")
            self.stats['failed_extractions'] += 1
    
    def _extract_identifiers(self, document: Dict[str, Any], config: Dict[str, str]) -> tuple[Optional[str], Optional[str]]:
        """Extract company_id and site_code from document"""
        company_id = None
        site_code = None
        
        # Try primary field names
        company_id = self._extract_field(document, [config['company_id_field']])
        site_code = self._extract_field(document, [config['site_code_field']])
        
        # Try alternative field names if primary didn't work
        if not company_id:
            company_id = self._extract_field(document, ['companyId', 'company', 'company_code'])
        
        # Convert to string and clean up
        if company_id is not None:
            company_id = str(company_id).strip()
        if site_code is not None:
            print("Site code before strip:", site_code)
            site_code = str(site_code).strip()
        
        return (company_id if company_id else None, 
                site_code if site_code else None)
    
    def _extract_field(self, document: Dict[str, Any], field_names: List[str]) -> Optional[str]:
        """Extract a field from document trying multiple possible field names"""
        
        for field_name in field_names:
            if field_name in document:
                value = document[field_name]
                if value is not None and str(value).strip():
                    return str(value)
        return None

    
    def _get_nested_value(self, document: Dict[str, Any], path: str) -> Optional[str]:
        """Get value from nested document using dot notation"""
        try:
            keys = path.split('.')
            value = document
            
            for key in keys:
                if isinstance(value, dict) and key in value:
                    value = value[key]
                else:
                    return None
            
            if value is not None and str(value).strip():
                return str(value).strip()
        except Exception:
            pass
        
        return None
    
    async def get_badge_evaluation_stats(self) -> Dict[str, Any]:
        """Get statistics about badge evaluation queue"""
        if not self.badge_evaluation_enabled:
            return {"enabled": False}
        
        try:
            badge_collection = self.db[self.badge_collection_name]
            
            # Get counts by status
            pipeline = [
                {"$group": {
                    "_id": "$status",
                    "count": {"$sum": 1}
                }}
            ]
            
            status_counts = {}
            async for doc in badge_collection.aggregate(pipeline):
                status_counts[doc['_id']] = doc['count']
            
            # Get total count
            total_count = await badge_collection.count_documents({})
            
            # Get recent entries count (last hour)
            one_hour_ago = datetime.now(timezone.utc) - timedelta(hours=1)
            recent_count = await badge_collection.count_documents({
                "created_at": {"$gte": one_hour_ago}
            })
            
            return {
                "enabled": True,
                "collection_name": self.badge_collection_name,
                "total_entries": total_count,
                "recent_entries_1h": recent_count,
                "status_counts": status_counts,
                "entries_created_this_session": self.stats['badge_entries_created'],
                "entries_updated_this_session": self.stats['badge_entries_updated']
            }
            
        except Exception as e:
            logger.error(f"❌ Error getting badge evaluation stats: {e}")
            return {"enabled": True, "error": str(e)}
    
    def get_stats(self) -> Dict[str, Any]:
        """Get watching statistics"""
        uptime = None
        if self.stats['start_time']:
            uptime = (datetime.now(timezone.utc) - self.stats['start_time']).total_seconds()
        
        return {
            **self.stats,
            'uptime_seconds': uptime,
            'running': self.running,
            'watched_collections': list(self.watched_collections.keys()),
            'poll_interval': self.poll_interval,
            'last_seen_values': self.last_seen,
            'badge_evaluation_enabled': self.badge_evaluation_enabled,
            'badge_collection_name': self.badge_collection_name,
            'extraction_success_rate': (
                self.stats['successful_extractions'] / max(1, self.stats['total_changes']) * 100
                if self.stats['total_changes'] > 0 else 0
            )
        }

# Example usage and testing
async def example_mongo_polling_handler(change_info: Dict[str, Any]):
    """Example handler function that processes MongoDB changes"""
    print(f"🔄 MongoDB Polling Change Handler Called:")
    print(f"   Collection: {change_info['collection_name']}")
    print(f"   Operation: {change_info['operation_type']}")
    print(f"   Company ID: {change_info['company_id']}")
    print(f"   Site Code: {change_info['site_code']}")
    print(f"   Document ID: {change_info['change_id']}")
    print(f"   Timestamp: {change_info['timestamp']}")
    if change_info['full_document']:
        print(f"   Document fields: {list(change_info['full_document'].keys())}")
    print("-" * 50)

async def main():
    """Main function to test MongoDB polling watcher with badge evaluation"""
    print("🚀 Starting Enhanced MongoDB Polling Watcher Test")
    print("=" * 60)
    
    # Create MongoDB polling watcher
    watcher = MongoDBPollingWatcher()
    
    # Enable badge evaluation (optional - enabled by default)
    watcher.enable_badge_evaluation("badge_evaluation_queue")
    
    # Example collection configurations - MODIFY THESE FOR YOUR COLLECTIONS
    collection_configs = [
        {
            'collection_name': 'cdata',           # Your collection name
            'timestamp_field': 'updatedAt',      # Your timestamp field (or None)
            'company_id_field': 'company_id',     # Your company field
            'site_code_field': 'site_code'        # Your site field
        },
       
    ]
    
    try:
        # Test connection first
        await watcher.connect()
        print("✅ Database connection successful!")
        
        # Show badge evaluation stats
        badge_stats = await watcher.get_badge_evaluation_stats()
        print(f"🏆 Badge evaluation: {badge_stats}")
        
        await watcher.disconnect()
        
        # Configure watcher
        print(f"\n🔧 Configuring watcher for {len(collection_configs)} collections...")
        watcher.watch_collections(collection_configs)
        watcher.set_poll_interval(15)  # Poll every 15 seconds for testing
        watcher.add_change_handler(example_mongo_polling_handler)
        
        print("\n📊 Starting monitoring...")
        print("💡 Make changes to your MongoDB collections to see them detected!")
        print("💡 Badge evaluation entries will be created automatically!")
        print("💡 This works WITHOUT replica sets!")
        print("💡 Press Ctrl+C to stop")
        print("-" * 60)
        
        # Start watching (this will run until interrupted)
        await watcher.start_watching()
        
    except KeyboardInterrupt:
        print("\n🛑 Stopping MongoDB polling watcher...")
        await watcher.stop_watching()
        
        # Show final statistics
        stats = watcher.get_stats()
        print("\n📊 Final Statistics:")
        print(f"Total changes processed: {stats['total_changes']}")
        print(f"Successful extractions: {stats['successful_extractions']}")
        print(f"Failed extractions: {stats['failed_extractions']}")
        print(f"Success rate: {stats['extraction_success_rate']:.1f}%")
        print(f"Changes by collection: {stats['changes_by_collection']}")
        print(f"Badge entries created: {stats['badge_entries_created']}")
        print(f"Badge entries updated: {stats['badge_entries_updated']}")
        print(f"Polling cycles: {stats['polling_cycles']}")
        
        # Show final badge evaluation stats
        if watcher.badge_evaluation_enabled:
            try:
                await watcher.connect()
                badge_stats = await watcher.get_badge_evaluation_stats()
                print(f"\n🏆 Final Badge Evaluation Stats:")
                print(f"Total entries: {badge_stats.get('total_entries', 0)}")
                print(f"Status counts: {badge_stats.get('status_counts', {})}")
                await watcher.disconnect()
            except Exception as e:
                print(f"❌ Error getting final badge stats: {e}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main())