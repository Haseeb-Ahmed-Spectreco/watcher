
"""
Enhanced SQL Database Change Watcher with Badge Evaluation Collection
Watches specified SQL tables for changes and creates badge evaluation entries in MongoDB
"""

import asyncio
import logging
import os
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional, Callable
import aiomysql
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv

load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SQLDatabaseWatcher:
    """SQL Database change watcher with MongoDB badge evaluation collection support"""
    
    def __init__(self, host: str = None, user: str = None, password: str = None, 
                 database: str = None, port: int = None):
        # SQL Database connection settings
        self.host = host or os.getenv("SQL_HOST", "127.0.0.1")
        self.user = user or os.getenv("SQL_USER", "root")
        self.password = password or os.getenv("SQL_PASSWORD", "")
        self.database = database or os.getenv("SQL_DATABASE", "ensogove")
        self.port = port or int(os.getenv("SQL_PORT", "3306"))
        
        # MongoDB connection settings for badge evaluation
        self.mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017")
        self.mongo_db_name = os.getenv("MONGO_DB_NAME", "ensogove")
        
        self.pool = None
        self.mongo_client = None
        self.mongo_db = None
        self.running = False
        
        # Tables to watch
        self.watched_tables = {}  # table_name -> config
        
        # Change handlers
        self.change_handlers = []
        
        # Polling configuration
        self.poll_interval = 30  # seconds
        self.batch_size = 1000   # records per batch
        
        # Track last seen timestamps/ids for each table
        self.last_seen = {}
        
        # Badge evaluation collection settings
        self.badge_evaluation_enabled = True
        self.badge_collection_name = "badge_evaluation_queue"
        
        # Statistics
        self.stats = {
            'total_changes': 0,
            'changes_by_table': {},
            'changes_by_operation': {},
            'successful_extractions': 0,
            'failed_extractions': 0,
            'badge_entries_created': 0,
            'badge_entries_updated': 0,
            'polling_cycles': 0,
            'start_time': None
        }
    
    def enable_badge_evaluation(self, collection_name: str = "badge_evaluation_queue", 
                               mongo_uri: str = None, mongo_db_name: str = None):
        """Enable badge evaluation collection creation in MongoDB"""
        self.badge_evaluation_enabled = True
        self.badge_collection_name = collection_name
        
        if mongo_uri:
            self.mongo_uri = mongo_uri
        if mongo_db_name:
            self.mongo_db_name = mongo_db_name
            
        logger.info(f"✅ Badge evaluation enabled - MongoDB collection: {collection_name}")
    
    def disable_badge_evaluation(self):
        """Disable badge evaluation collection creation"""
        self.badge_evaluation_enabled = False
        logger.info("❌ Badge evaluation disabled")
    
    async def connect(self):
        """Create connection pool to MySQL database and connect to MongoDB for badge evaluation"""
        try:
            # Connect to SQL database
            self.pool = await aiomysql.create_pool(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                db=self.database,
                minsize=1,
                maxsize=10,
                autocommit=True
            )
            
            # Test SQL connection
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute("SELECT 1")
                    
            logger.info(f"✅ Connected to MySQL database: {self.database}")
            
            # Connect to MongoDB for badge evaluation if enabled
            if self.badge_evaluation_enabled:
                await self._connect_mongodb()
            
        except Exception as e:
            logger.error(f"❌ Error connecting to databases: {e}")
            raise
    
    async def _connect_mongodb(self):
        """Connect to MongoDB for badge evaluation"""
        try:
            self.mongo_client = AsyncIOMotorClient(self.mongo_uri)
            self.mongo_db = self.mongo_client[self.mongo_db_name]
            await self.mongo_client.admin.command('ping')
            logger.info(f"✅ Connected to MongoDB for badge evaluation: {self.mongo_db_name}")
            
            # Setup badge evaluation collection
            await self._setup_badge_collection()
            
        except Exception as e:
            logger.error(f"❌ Error connecting to MongoDB: {e}")
            raise
    
    async def _setup_badge_collection(self):
        """Setup badge evaluation collection with proper indexes"""
        try:
            badge_collection = self.mongo_db[self.badge_collection_name]
            
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
        """Add or update badge evaluation entry in MongoDB with affected tables list"""
        if not self.badge_evaluation_enabled or not company_id or self.mongo_db is None:
            return
        
        try:
            # Normalize site_code - convert None to empty string
            normalized_site_code = site_code if site_code is not None else ""
            
            badge_collection = self.mongo_db[self.badge_collection_name]
            
            now = datetime.now(timezone.utc)
            table_name = source_info.get('table_name', '')
            
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
            table_name = source_info.get('table_name', '')
            
            # Step 1: Check if entry already exists
            existing_doc = await badge_collection.find_one(filter_query)
            
            if existing_doc:
                # Entry exists - update affected_collections/tables list and metadata
                logger.debug(f"📋 Updating existing badge entry for {company_id}:{site_code}")
                
                # Get current affected collections/tables list
                current_collections = existing_doc.get('affected_collections', [])
                current_tables = existing_doc.get('affected_tables', [])
                
                # Ensure they are lists
                if not isinstance(current_collections, list):
                    current_collections = [str(current_collections)] if current_collections else []
                if not isinstance(current_tables, list):
                    current_tables = [str(current_tables)] if current_tables else []
                
                # Add new table to affected_tables if not already in the list
                tables_updated = False
                if table_name and table_name not in current_tables:
                    current_tables.append(table_name)
                    tables_updated = True
                    logger.debug(f"📋 Added {table_name} to affected tables")
                
                # Prepare fresh document (nuclear approach)
                fresh_doc = {
                    "company_id": company_id,
                    "site_code": site_code,
                    "status": existing_doc.get("status", "pending"),
                    "priority": existing_doc.get("priority", "normal"),
                    "source": "sql",
                    "affected_collections": current_collections,  # Preserve MongoDB collections
                    "affected_tables": current_tables,           # Update SQL tables
                    "last_table_change": table_name,
                    "last_primary_key": source_info.get('primary_key'),
                    "created_at": existing_doc.get("created_at", now),
                    "updated_at": now,
                    "retry_count": existing_doc.get("retry_count", 0),
                    "metadata": {
                        "detected_from": "sql_watcher",
                        "first_detection_timestamp": existing_doc.get("metadata", {}).get("first_detection_timestamp", now),
                        "last_detection_timestamp": now,
                        "last_source_operation": source_info.get('operation_type', 'update'),
                        "total_collections_affected": len(current_collections),
                        "total_tables_affected": len(current_tables),
                        "tables_updated": tables_updated,
                        "update_count": existing_doc.get("metadata", {}).get("update_count", 0) + 1,
                        "sql_database": self.database
                    }
                }
                
                # If status is completed, reset it to pending since we have new changes
                if existing_doc.get('status') == 'completed':
                    fresh_doc["status"] = "pending"
                    fresh_doc["metadata"]["status_reset_reason"] = "new_changes_detected"
                
                try:
                    # Delete the old document first (nuclear approach)
                    await badge_collection.delete_one(filter_query)
                    logger.debug(f"🗑️ Deleted existing document for {company_id}:{site_code}")
                    
                    # Insert fresh document
                    await badge_collection.insert_one(fresh_doc)
                    self.stats['badge_entries_updated'] += 1
                    logger.debug(f"🔄 Updated badge entry for {company_id}:{site_code} - tables: {current_tables}")
                    return True
                    
                except Exception as update_error:
                    logger.error(f"❌ Failed to update existing badge entry: {update_error}")
                    return False
            else:
                # No existing entry - create new one
                logger.debug(f"🏗️ Creating new badge entry for {company_id}:{site_code}")
                
                # Create fresh document with affected_tables as list
                fresh_doc = {
                    "company_id": company_id,
                    "site_code": site_code,
                    "status": "pending",
                    "priority": "normal",
                    "source": "sql",
                    "affected_collections": [],  # Empty for SQL-only entries
                    "affected_tables": [table_name] if table_name else [],
                    "last_table_change": table_name,
                    "last_primary_key": source_info.get('primary_key'),
                    "created_at": now,
                    "updated_at": now,
                    "retry_count": 0,
                    "metadata": {
                        "detected_from": "sql_watcher",
                        "first_detection_timestamp": now,
                        "last_detection_timestamp": now,
                        "last_source_operation": source_info.get('operation_type', 'update'),
                        "total_collections_affected": 0,
                        "total_tables_affected": 1 if table_name else 0,
                        "tables_updated": True,
                        "update_count": 1,
                        "sql_database": self.database
                    }
                }
                
                try:
                    await badge_collection.insert_one(fresh_doc)
                    self.stats['badge_entries_created'] += 1
                    logger.debug(f"🏆 Created new badge entry for {company_id}:{site_code} - table: {table_name}")
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
    
    async def disconnect(self):
        """Close database connections"""
        if self.pool:
            self.pool.close()
            await self.pool.wait_closed()
            logger.info("🔌 Disconnected from MySQL database")
        
        if self.mongo_client:
            self.mongo_client.close()
            logger.info("🔌 Disconnected from MongoDB")
    
    def watch_table(self, table_name: str, timestamp_field: str = None, 
                   id_field: str = None, company_id_field: str = 'company_id', 
                   site_code_field: str = 'site_code'):
        """
        Add a table to watch for changes
        
        Args:
            table_name: Name of the table to watch
            timestamp_field: Field to use for detecting changes (e.g. 'updated_at', 'modified_time')
            id_field: Primary key field (e.g. 'id', 'record_id') - used if no timestamp field
            company_id_field: Field containing company identifier
            site_code_field: Field containing site code
        """
        config = {
            'timestamp_field': timestamp_field,
            'id_field': id_field if id_field is not None else 'id',
            'company_id_field': company_id_field,
            'site_code_field': site_code_field
        }
        
        self.watched_tables[table_name] = config
        logger.info(f"📋 Watching table: {table_name} (timestamp: {timestamp_field}, id: {id_field})")
    
    def watch_tables(self, table_configs: List[Dict[str, str]]):
        """
        Add multiple tables to watch
        
        Args:
            table_configs: List of table configurations
            Example: [
                {
                    'table_name': 'kpi_data',
                    'timestamp_field': 'updated_at',
                    'company_id_field': 'company_id',
                    'site_code_field': 'site_code'
                }
            ]
        """
        for config in table_configs:
            self.watch_table(
                table_name=config['table_name'],
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
        """Start watching all specified tables"""
        if not self.watched_tables:
            logger.error("❌ No tables specified to watch")
            return
        
        await self.connect()
        
        self.running = True
        self.stats['start_time'] = datetime.now(timezone.utc)
        
        logger.info(f"🚀 Starting to watch {len(self.watched_tables)} SQL tables...")
        logger.info(f"⏰ Polling every {self.poll_interval} seconds")
        logger.info(f"🏆 Badge evaluation: {'enabled' if self.badge_evaluation_enabled else 'disabled'}")
        
        # Initialize last seen values for each table
        await self._initialize_last_seen()
        
        try:
            # Main polling loop
            while self.running:
                await self._poll_tables()
                
                # Show countdown for next poll
                logger.info(f"⏰ Next poll in {self.poll_interval} seconds...")
                await asyncio.sleep(self.poll_interval)
        except KeyboardInterrupt:
            logger.info("🛑 Received shutdown signal")
        finally:
            await self.stop_watching()
    
    async def stop_watching(self):
        """Stop watching all tables"""
        self.running = False
        await self.disconnect()
        logger.info("🛑 Stopped watching SQL tables")
    
    async def _initialize_last_seen(self):
        """Initialize last seen timestamps/ids for each table"""
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                for table_name, config in self.watched_tables.items():
                    try:
                        # Get the latest timestamp or ID to start from
                        if config['timestamp_field']:
                            query = f"SELECT MAX({config['timestamp_field']}) FROM {table_name}"
                        else:
                            query = f"SELECT MAX({config['id_field']}) FROM {table_name}"
                        
                        await cursor.execute(query)
                        result = await cursor.fetchone()
                        
                        if result and result[0] is not None:
                            self.last_seen[table_name] = result[0]
                            logger.info(f"📍 Initialized {table_name} last seen: {result[0]}")
                        else:
                            # Table is empty or field doesn't exist
                            if config['timestamp_field']:
                                self.last_seen[table_name] = datetime.now(timezone.utc) - timedelta(minutes=1)
                            else:
                                self.last_seen[table_name] = 0
                            logger.info(f"📍 Initialized {table_name} with default value")
                            
                    except Exception as e:
                        logger.error(f"❌ Error initializing {table_name}: {e}")
                        # Set a default value
                        if config['timestamp_field']:
                            self.last_seen[table_name] = datetime.now(timezone.utc)
                        else:
                            self.last_seen[table_name] = 0
    
    async def _poll_tables(self):
        """Poll all watched tables for changes"""
        self.stats['polling_cycles'] += 1
        logger.info(f"📊 Polling cycle #{self.stats['polling_cycles']} - Checking for changes...")
        
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                for table_name, config in self.watched_tables.items():
                    try:
                        await self._poll_table(cursor, table_name, config)
                    except Exception as e:
                        logger.error(f"❌ Error polling table {table_name}: {e}")
    
    async def _poll_table(self, cursor, table_name: str, config: Dict[str, str]):
        """Poll a single table for changes"""
        try:
            last_seen = self.last_seen.get(table_name)
            logger.info(f"🔍 Checking table {table_name} for changes since: {last_seen}")
            
            # Build query based on whether we're using timestamp or ID
            if config['timestamp_field']:
                # Use timestamp-based detection
                query = f"""
                    SELECT * FROM {table_name} 
                    WHERE {config['timestamp_field']} > %s 
                    ORDER BY {config['timestamp_field']} ASC 
                    LIMIT %s
                """
                logger.debug(f"📝 Query: {query.strip()} with params: ({last_seen}, {self.batch_size})")
                await cursor.execute(query, (last_seen, self.batch_size))
            else:
                # Use ID-based detection
                query = f"""
                    SELECT * FROM {table_name} 
                    WHERE {config['id_field']} > %s 
                    ORDER BY {config['id_field']} ASC 
                    LIMIT %s
                """
                logger.debug(f"📝 Query: {query.strip()} with params: ({last_seen}, {self.batch_size})")
                await cursor.execute(query, (last_seen, self.batch_size))
            
            rows = await cursor.fetchall()
            
            if rows:
                logger.info(f"🔄 Found {len(rows)} changes in table {table_name}")
                
                for row in rows:
                    await self._process_change(table_name, config, row)
                
                # Update last seen value
                if config['timestamp_field']:
                    self.last_seen[table_name] = rows[-1][config['timestamp_field']]
                else:
                    self.last_seen[table_name] = rows[-1][config['id_field']]
                
                logger.info(f"📍 Updated {table_name} last seen: {self.last_seen[table_name]}")
            else:
                logger.info(f"📊 No new changes in {table_name} (last seen: {last_seen})")
                
        except Exception as e:
            logger.error(f"❌ Error polling table {table_name}: {e}")
    
    async def _process_change(self, table_name: str, config: Dict[str, str], row: Dict[str, Any]):
        """Process a single changed row"""
        try:
            # Update statistics
            self.stats['total_changes'] += 1
            
            if table_name not in self.stats['changes_by_table']:
                self.stats['changes_by_table'][table_name] = 0
            self.stats['changes_by_table'][table_name] += 1
            
            # For SQL, we detect "changes" but can't distinguish operation type
            # We assume everything is an "update" since we're polling
            operation_type = 'update'
            if operation_type not in self.stats['changes_by_operation']:
                self.stats['changes_by_operation'][operation_type] = 0
            self.stats['changes_by_operation'][operation_type] += 1
            
            # Extract company_id and site_code
            company_id, site_code = self._extract_identifiers(row, config)
            
            # Normalize site_code - convert None to empty string for consistency
            if site_code is None:
                site_code = ""
            
            if company_id:  # Only process if we have a company_id
                self.stats['successful_extractions'] += 1
                
                # Create source info for badge evaluation
                source_info = {
                    'table_name': table_name,
                    'primary_key': row.get(config['id_field']),
                    'operation_type': operation_type
                }
                
                # Add badge evaluation entry
                await self._add_badge_evaluation_entry(company_id, site_code, source_info)
                
                # Create change info
                change_info = {
                    'table_name': table_name,
                    'operation_type': operation_type,
                    'company_id': company_id,
                    'site_code': site_code,
                    'row_data': row,
                    'timestamp': datetime.now(timezone.utc),
                    'primary_key': row.get(config['id_field'])
                }
                
                logger.info(f"✅ Extracted: {company_id}:{site_code} from {table_name}")
                
                # Call all registered handlers
                for handler in self.change_handlers:
                    try:
                        await handler(change_info)
                    except Exception as e:
                        logger.error(f"❌ Error in change handler {handler.__name__}: {e}")
            else:
                self.stats['failed_extractions'] += 1
                logger.warning(f"⚠️  Could not extract company_id from {table_name}")
                logger.debug(f"Row data: {list(row.keys())}")
                
        except Exception as e:
            logger.error(f"❌ Error processing change: {e}")
            self.stats['failed_extractions'] += 1
    
    def _extract_identifiers(self, row: Dict[str, Any], config: Dict[str, str]) -> tuple[Optional[str], Optional[str]]:
        """Extract company_id and site_code from row data"""
        company_id = None
        site_code = None
        
        # Try primary field names
        if config['company_id_field'] is not None:
            company_id = self._extract_field(row, [config['company_id_field']])
        if config['site_code_field'] is not None:
            site_code = self._extract_field(row, [config['site_code_field']])
        
        # Try alternative field names if primary didn't work
        if not company_id:
            company_id = self._extract_field(row, ['id', 'company_name', 'user_id', 'company_code'])
        
        if not site_code and config['site_code_field'] is not None:
            site_code = self._extract_field(row, ['site_code'])
        
        # Convert to string and clean up
        if company_id is not None:
            company_id = str(company_id).strip()
        if site_code is not None:
            site_code = str(site_code).strip()
        
        return (company_id if company_id else None, 
                site_code if site_code else None)
    
    def _extract_field(self, row: Dict[str, Any], field_names: List[str]) -> Optional[str]:
        """Extract a field from row trying multiple possible field names"""
        for field_name in field_names:
            if field_name in row:
                value = row[field_name]
                if value is not None and str(value).strip():
                    return str(value).strip()
        return None
    
    async def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """Get information about a table structure"""
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    # Get table columns
                    await cursor.execute(f"DESCRIBE {table_name}")
                    columns = await cursor.fetchall()
                    
                    # Get row count
                    await cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                    row_count = (await cursor.fetchone())[0]
                    
                    # Get sample data
                    await cursor.execute(f"SELECT * FROM {table_name} LIMIT 3")
                    sample_rows = await cursor.fetchall()
                    
                    return {
                        'table_name': table_name,
                        'columns': [col[0] for col in columns],
                        'column_details': columns,
                        'row_count': row_count,
                        'sample_rows': sample_rows
                    }
        except Exception as e:
            logger.error(f"❌ Error getting table info for {table_name}: {e}")
            return {'error': str(e)}
    
    async def get_badge_evaluation_stats(self) -> Dict[str, Any]:
        """Get statistics about badge evaluation queue"""
        if not self.badge_evaluation_enabled or self.mongo_db is None:
            return {"enabled": False}
        
        try:
            badge_collection = self.mongo_db[self.badge_collection_name]
            
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
            'watched_tables': list(self.watched_tables.keys()),
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
async def example_sql_change_handler(change_info: Dict[str, Any]):
    """Example handler function that processes SQL changes"""
    print(f"🔄 SQL Change Handler Called:")
    print(f"   Table: {change_info['table_name']}")
    print(f"   Operation: {change_info['operation_type']}")
    print(f"   Company ID: {change_info['company_id']}")
    print(f"   Site Code: {change_info['site_code']}")
    print(f"   Primary Key: {change_info['primary_key']}")
    print(f"   Timestamp: {change_info['timestamp']}")
    print(f"   Row Fields: {list(change_info['row_data'].keys())}")
    print("-" * 50)

async def main():
    """Main function to test SQL database watcher with badge evaluation"""
    print("🚀 Starting Enhanced SQL Database Watcher Test")
    print("=" * 60)
    
    # Create SQL watcher
    watcher = SQLDatabaseWatcher()
    
    # Enable badge evaluation (optional - enabled by default)
    watcher.enable_badge_evaluation("badge_evaluation_queue")
    
    # Example table configurations - MODIFY THESE FOR YOUR TABLES
    table_configs = [
        {
            'table_name': 'companies',        # Replace with your table name
            'timestamp_field': 'updated_at', # Replace with your timestamp field
            'id_field': 'id',
            'company_id_field': 'id',   # Replace with your company field
        },

    ]
    
    try:
        # Test connection first
        await watcher.connect()
        print("✅ Database connections successful!")
        
        # Show badge evaluation stats
        badge_stats = await watcher.get_badge_evaluation_stats()
        print(f"🏆 Badge evaluation: {badge_stats}")
        
        # Show table information
        print("\n📋 Table Information:")
        print("-" * 30)
        for config in table_configs:
            table_name = config['table_name']
            print(f"\nChecking table: {table_name}")
            
            info = await watcher.get_table_info(table_name)
            if 'error' in info:
                print(f"❌ Error: {info['error']}")
            else:
                print(f"✅ Columns: {', '.join(info['columns'])}")
                print(f"✅ Row count: {info['row_count']}")
                
                # Check if required fields exist
                required_fields = [
                    config.get('timestamp_field'),
                    config.get('company_id_field', 'company_id'),
                    config.get('site_code_field', 'site_code')
                ]
                
                missing_fields = []
                for field in required_fields:
                    if field and field not in info['columns']:
                        missing_fields.append(field)
                
                if missing_fields:
                    print(f"⚠️  Missing fields: {', '.join(missing_fields)}")
                else:
                    print(f"✅ All required fields found")
        
        await watcher.disconnect()
        
        # Configure watcher
        print(f"\n🔧 Configuring watcher for {len(table_configs)} tables...")
        watcher.watch_tables(table_configs)
        watcher.set_poll_interval(15)  # Poll every 15 seconds for testing
        watcher.add_change_handler(example_sql_change_handler)
        
        print("\n📊 Starting monitoring...")
        print("💡 Make changes to your SQL tables to see them detected!")
        print("💡 Badge evaluation entries will be created automatically!")
        print("💡 Press Ctrl+C to stop")
        print("-" * 60)
        
        # Start watching (this will run until interrupted)
        await watcher.start_watching()
        
    except KeyboardInterrupt:
        print("\n🛑 Stopping SQL watcher...")
        await watcher.stop_watching()
        
        # Show final statistics
        stats = watcher.get_stats()
        print("\n📊 Final Statistics:")
        print(f"Total changes processed: {stats['total_changes']}")
        print(f"Successful extractions: {stats['successful_extractions']}")
        print(f"Failed extractions: {stats['failed_extractions']}")
        print(f"Success rate: {stats['extraction_success_rate']:.1f}%")
        print(f"Changes by table: {stats['changes_by_table']}")
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