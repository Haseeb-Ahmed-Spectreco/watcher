#!/usr/bin/env python3
"""
Enhanced Combined Database Watcher with CLI Arguments and Configuration
Runs both MongoDB and SQL watchers simultaneously and creates queue entries for badge evaluation
This is a WATCHER only - it does NOT evaluate badges, it only creates queue entries for evaluation
"""

# Debug: Check for module import duplication
import sys
print(f"🐛 DEBUG: Module {__name__} being imported/executed")

import asyncio
import logging
import os
import json
import argparse
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional, Callable
from dataclasses import dataclass
from pathlib import Path
from dotenv import load_dotenv


try:
    from mongo_watcher import MongoDBPollingWatcher
    MONGO_AVAILABLE = True
    print("✅ Enhanced MongoDB watcher imported successfully")
except ImportError as e:
    MONGO_AVAILABLE = False
    print(f"⚠️  Enhanced MongoDB watcher not available: {e}")

try:
    from sql_watcher import SQLDatabaseWatcher  
    SQL_AVAILABLE = True
    print("✅ Enhanced SQL watcher imported successfully")
except ImportError as e:
    SQL_AVAILABLE = False
    print(f"⚠️  Enhanced SQL watcher not available: {e}")

# Load environment variables
load_dotenv()

def setup_logging(log_level: str = "INFO", log_to_file: bool = False, log_file_path: str = None):
    """Setup logging configuration - simplified to prevent duplicates"""
    
    # Check if logging is already configured
    root_logger = logging.getLogger()
    if root_logger.handlers:
        print(f"🐛 DEBUG: Logging already configured with {len(root_logger.handlers)} handlers")
        return
    
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    # Convert string log level to logging constant
    numeric_level = getattr(logging, log_level.upper(), logging.INFO)
    
    # Use basicConfig for simple setup
    handlers = [logging.StreamHandler(sys.stdout)]
    
    if log_to_file and log_file_path:
        try:
            Path(log_file_path).parent.mkdir(parents=True, exist_ok=True)
            handlers.append(logging.FileHandler(log_file_path))
            print(f"📝 Logging to file: {log_file_path}")
        except Exception as e:
            print(f"⚠️  Failed to setup file logging: {e}")
    
    logging.basicConfig(
        level=numeric_level,
        format=log_format,
        handlers=handlers,
        force=True  # This clears existing handlers
    )
    
    print(f"🐛 DEBUG: Logging configured with {len(root_logger.handlers)} handlers")

def str_to_bool(value: str) -> bool:
    """Convert string to boolean"""
    return value.lower() in ('true', '1', 'yes', 'on', 'enabled')

def parse_json_config(json_string: str, config_name: str) -> List[Dict[str, Any]]:
    """Parse JSON configuration string safely"""
    if not json_string:
        return []
    
    try:
        config = json.loads(json_string)
        if isinstance(config, list):
            return config
        else:
            print(f"⚠️  {config_name} should be a list, got {type(config)}")
            return []
    except json.JSONDecodeError as e:
        print(f"❌ Error parsing {config_name}: {e}")
        return []

@dataclass
class EnhancedCombinedWatcherConfig:
    """Configuration for the enhanced combined database watcher with queue entry creation"""
    # MongoDB configuration
    mongo_enabled: bool = False
    mongo_uri: str = None
    mongo_db_name: str = None
    mongo_collections: List[Dict[str, str]] = None
    
    # SQL configuration
    sql_enabled: bool = False
    sql_host: str = None
    sql_user: str = None
    sql_password: str = None
    sql_database: str = None
    sql_port: int = None
    sql_auth_plugin: str = None
    sql_ssl_disabled: bool = False
    sql_tables: List[Dict[str, str]] = None
    
    # Queue configuration (for badge evaluation queue entries)
    queue_enabled: bool = True
    queue_collection_name: str = "badge_evaluation_queue"
    
    # Common configuration
    poll_interval: int = 30
    batch_size: int = 1000
    status_report_interval: int = 300
    max_retries: int = 3
    connection_timeout: int = 30

logger = logging.getLogger(__name__)

class EnhancedCombinedDatabaseWatcher:
    """Enhanced combined watcher that runs both MongoDB and SQL watchers and creates queue entries"""
    
    def __init__(self, config: EnhancedCombinedWatcherConfig = None):
        self.config = config or EnhancedCombinedWatcherConfig()
        
        # Individual watchers
        self.mongo_watcher = None
        self.sql_watcher = None
        
        # Combined state
        self.running = False
        self.change_handlers = []
        self.running_tasks = []
        
        # Combined statistics
        self.combined_stats = {
            'total_changes': 0,
            'changes_by_source': {'mongodb': 0, 'sql': 0},
            'changes_by_collection_table': {},
            'changes_by_operation': {},
            'successful_extractions': 0,
            'failed_extractions': 0,
            'queue_entries_created': 0,
            'queue_entries_updated': 0,
            'start_time': None
        }
    
    def configure_mongodb(self, mongo_uri: str = None, db_name: str = None, 
                         collections: List[Dict[str, str]] = None):
        """Configure MongoDB watcher"""
        if not MONGO_AVAILABLE:
            logger.error("❌ Enhanced MongoDB watcher not available - check import")
            return False
            
        self.config.mongo_enabled = True
        self.config.mongo_uri = mongo_uri or self.config.mongo_uri
        self.config.mongo_db_name = db_name or self.config.mongo_db_name
        self.config.mongo_collections = collections or self.config.mongo_collections or []
        
        return True
    
    def configure_sql(self, host: str = None, user: str = None, password: str = None,
                     database: str = None, port: int = None, tables: List[Dict[str, str]] = None,
                     auth_plugin: str = None, ssl_disabled: bool = False):
        """Configure SQL watcher"""
        if not SQL_AVAILABLE:
            logger.error("❌ Enhanced SQL watcher not available - check import")
            return False
            
        self.config.sql_enabled = True
        self.config.sql_host = host or self.config.sql_host
        self.config.sql_user = user or self.config.sql_user
        self.config.sql_password = password or self.config.sql_password
        self.config.sql_database = database or self.config.sql_database
        self.config.sql_port = port or self.config.sql_port
        self.config.sql_auth_plugin = auth_plugin or self.config.sql_auth_plugin
        self.config.sql_ssl_disabled = ssl_disabled if ssl_disabled else self.config.sql_ssl_disabled
        self.config.sql_tables = tables or self.config.sql_tables or []
        
        return True
    
    def configure_queue(self, enabled: bool = True, collection_name: str = "badge_evaluation_queue"):
        """Configure queue entry creation settings"""
        self.config.queue_enabled = enabled
        self.config.queue_collection_name = collection_name
        
        return True
    
    def add_change_handler(self, handler: Callable):
        """Add a unified change handler for both MongoDB and SQL changes"""
        self.change_handlers.append(handler)
    
    def set_poll_interval(self, seconds: int):
        """Set polling interval for both watchers"""
        self.config.poll_interval = seconds
        # Don't log here - will be shown in configuration summary
    
    async def _initialize_watchers(self):
        """Initialize individual watchers based on configuration"""
        try:
            # Initialize MongoDB watcher if enabled
            if self.config.mongo_enabled and self.config.mongo_collections and MONGO_AVAILABLE:
                logger.info(f"🍃 Initializing MongoDB watcher: {self.config.mongo_db_name} with {len(self.config.mongo_collections)} collections")
                self.mongo_watcher = MongoDBPollingWatcher(
                    mongo_uri=self.config.mongo_uri,
                    db_name=self.config.mongo_db_name
                )
                self.mongo_watcher.watch_collections(self.config.mongo_collections)
                self.mongo_watcher.set_poll_interval(self.config.poll_interval)
                
                # Configure queue entry creation
                if self.config.queue_enabled:
                    self.mongo_watcher.enable_badge_evaluation(self.config.queue_collection_name)
                    logger.info(f"📋 MongoDB queue entry creation enabled - collection: {self.config.queue_collection_name}")
                else:
                    self.mongo_watcher.disable_badge_evaluation()
                    logger.info("❌ MongoDB queue entry creation disabled")
                
                self.mongo_watcher.add_change_handler(self._mongo_change_wrapper)
                logger.info("✅ Enhanced MongoDB watcher initialized")
            
            # Initialize SQL watcher if enabled
            if self.config.sql_enabled and self.config.sql_tables and SQL_AVAILABLE:
                logger.info(f"🗄️ Initializing SQL watcher: {self.config.sql_database} with {len(self.config.sql_tables)} tables")
                
                # Create connection parameters
                sql_params = {
                    'host': self.config.sql_host,
                    'user': self.config.sql_user,
                    'password': self.config.sql_password,
                    'database': self.config.sql_database,
                    'port': self.config.sql_port
                }
                
                # Add optional connection parameters to avoid cryptography requirement
                if self.config.sql_auth_plugin:
                    sql_params['auth_plugin'] = self.config.sql_auth_plugin
                    logger.info(f"🔑 Using auth plugin: {self.config.sql_auth_plugin}")
                if self.config.sql_ssl_disabled:
                    sql_params['ssl_disabled'] = True
                    logger.info("🔒 SSL disabled for SQL connection")
                
                self.sql_watcher = SQLDatabaseWatcher(**sql_params)
                self.sql_watcher.watch_tables(self.config.sql_tables)
                self.sql_watcher.set_poll_interval(self.config.poll_interval)
                
                # Configure queue entry creation
                if self.config.queue_enabled:
                    self.sql_watcher.enable_badge_evaluation(
                        self.config.queue_collection_name,
                        self.config.mongo_uri,
                        self.config.mongo_db_name
                    )
                    logger.info(f"📋 SQL queue entry creation enabled - collection: {self.config.queue_collection_name}")
                else:
                    self.sql_watcher.disable_badge_evaluation()
                    logger.info("❌ SQL queue entry creation disabled")
                
                self.sql_watcher.add_change_handler(self._sql_change_wrapper)
                logger.info("✅ Enhanced SQL watcher initialized")
                
        except Exception as e:
            logger.error(f"❌ Error initializing watchers: {e}")
            raise
    
    async def _mongo_change_wrapper(self, change_info: Dict[str, Any]):
        """Wrapper for MongoDB changes to add source information and update stats"""
        # Add source information
        change_info['source'] = 'mongodb'
        change_info['source_type'] = 'collection'
        change_info['source_name'] = change_info['collection_name']
        
        # Update combined statistics
        self._update_stats('mongodb', change_info)
        
        # Call all registered handlers
        await self._call_handlers(change_info)
    
    async def _sql_change_wrapper(self, change_info: Dict[str, Any]):
        """Wrapper for SQL changes to add source information and update stats"""
        # Add source information
        change_info['source'] = 'sql'
        change_info['source_type'] = 'table'
        change_info['source_name'] = change_info['table_name']
        
        # Update combined statistics
        self._update_stats('sql', change_info)
        
        # Call all registered handlers
        await self._call_handlers(change_info)
    
    def _update_stats(self, source: str, change_info: Dict[str, Any]):
        """Update combined statistics"""
        self.combined_stats['total_changes'] += 1
        self.combined_stats['changes_by_source'][source] += 1
        
        source_name = change_info['source_name']
        if source_name not in self.combined_stats['changes_by_collection_table']:
            self.combined_stats['changes_by_collection_table'][source_name] = 0
        self.combined_stats['changes_by_collection_table'][source_name] += 1
        
        operation_type = change_info.get('operation_type', 'unknown')
        if operation_type not in self.combined_stats['changes_by_operation']:
            self.combined_stats['changes_by_operation'][operation_type] = 0
        self.combined_stats['changes_by_operation'][operation_type] += 1
        
        if change_info.get('company_id') or change_info.get('site_code'):
            self.combined_stats['successful_extractions'] += 1
        else:
            self.combined_stats['failed_extractions'] += 1
    
    async def _call_handlers(self, change_info: Dict[str, Any]):
        """Call all registered change handlers"""
        for handler in self.change_handlers:
            try:
                await handler(change_info)
            except Exception as e:
                logger.error(f"❌ Error in combined change handler {handler.__name__}: {e}")
    
    async def _run_mongo_watcher(self):
        """Run MongoDB watcher in background"""
        if not self.mongo_watcher:
            return
            
        try:
            logger.info("🍃 Starting enhanced MongoDB watcher...")
            await self.mongo_watcher.start_watching()
        except Exception as e:
            logger.error(f"❌ Error in MongoDB watcher: {e}")
    
    async def _run_sql_watcher(self):
        """Run SQL watcher in background"""
        if not self.sql_watcher:
            return
            
        try:
            logger.info("🗄️ Starting enhanced SQL watcher...")
            await self.sql_watcher.start_watching()
        except Exception as e:
            logger.error(f"❌ Error in SQL watcher: {e}")
    
    async def _status_reporter(self):
        """Periodically report combined status including badge evaluation stats"""
        while self.running:
            try:
                await asyncio.sleep(self.config.status_report_interval)
                
                if self.running:
                    await self._log_detailed_status()
                    
            except Exception as e:
                logger.error(f"❌ Error in status reporter: {e}")
                await asyncio.sleep(60)
    
    async def _log_detailed_status(self):
        """Log detailed combined status including badge evaluation"""
        mongo_changes = self.combined_stats['changes_by_source']['mongodb']
        sql_changes = self.combined_stats['changes_by_source']['sql']
        total_changes = self.combined_stats['total_changes']
        success_rate = (
            self.combined_stats['successful_extractions'] / max(1, total_changes) * 100
            if total_changes > 0 else 0
        )
        
        logger.info(f"📊 Combined Status: {total_changes} total changes "
                   f"(MongoDB: {mongo_changes}, SQL: {sql_changes}) - "
                   f"Success rate: {success_rate:.1f}%")
        
        # Update queue entry stats from individual watchers
        await self._update_combined_queue_stats()
        
        # Log queue entry creation status
        if self.config.queue_enabled:
            total_queue_created = self.combined_stats['queue_entries_created']
            total_queue_updated = self.combined_stats['queue_entries_updated']
            logger.info(f"📋 Queue Entries: {total_queue_created} created, "
                       f"{total_queue_updated} updated this session")
        
        # Log individual watcher stats if available
        if self.mongo_watcher:
            mongo_stats = self.mongo_watcher.get_stats()
            logger.info(f"🍃 MongoDB: {mongo_stats['polling_cycles']} cycles, "
                       f"{mongo_stats['total_changes']} changes")
        
        if self.sql_watcher:
            sql_stats = self.sql_watcher.get_stats()
            logger.info(f"🗄️ SQL: {sql_stats['polling_cycles']} cycles, "
                       f"{sql_stats['total_changes']} changes")
    
    async def _update_combined_queue_stats(self):
        """Update combined queue statistics from individual watchers"""
        total_created = 0
        total_updated = 0
        
        if self.mongo_watcher:
            mongo_stats = self.mongo_watcher.get_stats()
            total_created += mongo_stats.get('badge_entries_created', 0)
            total_updated += mongo_stats.get('badge_entries_updated', 0)
        
        if self.sql_watcher:
            sql_stats = self.sql_watcher.get_stats()
            total_created += sql_stats.get('badge_entries_created', 0)
            total_updated += sql_stats.get('badge_entries_updated', 0)
        
        self.combined_stats['queue_entries_created'] = total_created
        self.combined_stats['queue_entries_updated'] = total_updated
    
    async def start_watching(self):
        """Start watching both databases simultaneously"""
        if not self.config.mongo_enabled and not self.config.sql_enabled:
            logger.error("❌ No databases configured to watch")
            return
        
        self.running = True
        self.combined_stats['start_time'] = datetime.now(timezone.utc)
        
        logger.info("🚀 Starting Enhanced Combined Database Watcher...")
        logger.info(f"   MongoDB enabled: {self.config.mongo_enabled}")
        logger.info(f"   SQL enabled: {self.config.sql_enabled}")
        logger.info(f"   Queue entry creation: {'enabled' if self.config.queue_enabled else 'disabled'}")
        logger.info(f"   Poll interval: {self.config.poll_interval} seconds")
        
        try:
            # Initialize watchers
            await self._initialize_watchers()
            
            # Create tasks for each enabled watcher
            tasks = []
            
            if self.config.mongo_enabled and self.mongo_watcher:
                mongo_task = asyncio.create_task(self._run_mongo_watcher())
                tasks.append(('MongoDB', mongo_task))
                self.running_tasks.append(mongo_task)
            
            if self.config.sql_enabled and self.sql_watcher:
                sql_task = asyncio.create_task(self._run_sql_watcher())
                tasks.append(('SQL', sql_task))
                self.running_tasks.append(sql_task)
            
            # Create status reporter task
            status_task = asyncio.create_task(self._status_reporter())
            tasks.append(('Status', status_task))
            self.running_tasks.append(status_task)
            
            logger.info(f"✅ Started {len(tasks)} watcher tasks")
            
            # Wait for all tasks (they should run indefinitely)
            if tasks:
                try:
                    # Wait for any task to complete (shouldn't happen unless there's an error)
                    done, pending = await asyncio.wait(
                        [task for _, task in tasks],
                        return_when=asyncio.FIRST_COMPLETED
                    )
                    
                    # If we get here, one of the tasks completed unexpectedly
                    for task in done:
                        try:
                            await task
                        except Exception as e:
                            logger.error(f"❌ Task completed with error: {e}")
                    
                except KeyboardInterrupt:
                    logger.info("🛑 Received shutdown signal")
                finally:
                    await self.stop_watching()
            else:
                logger.error("❌ No watcher tasks to run")
                
        except Exception as e:
            logger.error(f"❌ Error starting enhanced combined watcher: {e}")
            await self.stop_watching()
            raise
    
    async def stop_watching(self):
        """Stop all watchers gracefully"""
        logger.info("🛑 Stopping Enhanced Combined Database Watcher...")
        
        self.running = False
        
        # Cancel all running tasks
        for task in self.running_tasks:
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                except Exception as e:
                    logger.error(f"❌ Error stopping task: {e}")
        
        # Stop individual watchers
        if self.mongo_watcher:
            try:
                await self.mongo_watcher.stop_watching()
            except Exception as e:
                logger.error(f"❌ Error stopping MongoDB watcher: {e}")
        
        if self.sql_watcher:
            try:
                await self.sql_watcher.stop_watching()
            except Exception as e:
                logger.error(f"❌ Error stopping SQL watcher: {e}")
        
        self.running_tasks.clear()
        logger.info("✅ Enhanced Combined Database Watcher stopped")
    
    async def get_queue_summary(self) -> Dict[str, Any]:
        """Get combined queue statistics from both watchers"""
        if not self.config.queue_enabled:
            return {"enabled": False}
        
        combined_stats = {
            "enabled": True,
            "collection_name": self.config.queue_collection_name,
            "total_entries": 0,
            "recent_entries_1h": 0,
            "status_counts": {},
            "entries_created_this_session": 0,
            "entries_updated_this_session": 0,
            "sources": {}
        }
        
        try:
            # Get stats from MongoDB watcher
            if self.mongo_watcher:
                mongo_queue_stats = await self.mongo_watcher.get_badge_evaluation_stats()
                if mongo_queue_stats.get("enabled"):
                    combined_stats["sources"]["mongodb"] = mongo_queue_stats
                    combined_stats["entries_created_this_session"] += mongo_queue_stats.get("entries_created_this_session", 0)
                    combined_stats["entries_updated_this_session"] += mongo_queue_stats.get("entries_updated_this_session", 0)
            
            # Get stats from SQL watcher
            if self.sql_watcher:
                sql_queue_stats = await self.sql_watcher.get_badge_evaluation_stats()
                if sql_queue_stats.get("enabled"):
                    combined_stats["sources"]["sql"] = sql_queue_stats
                    # Don't double count - these stats come from the same MongoDB collection
                    if "mongodb" not in combined_stats["sources"]:
                        combined_stats["total_entries"] = sql_queue_stats.get("total_entries", 0)
                        combined_stats["recent_entries_1h"] = sql_queue_stats.get("recent_entries_1h", 0)
                        combined_stats["status_counts"] = sql_queue_stats.get("status_counts", {})
            
            # If we have MongoDB stats, use those for the totals (since both write to same collection)
            if "mongodb" in combined_stats["sources"]:
                mongo_stats = combined_stats["sources"]["mongodb"]
                combined_stats["total_entries"] = mongo_stats.get("total_entries", 0)
                combined_stats["recent_entries_1h"] = mongo_stats.get("recent_entries_1h", 0)
                combined_stats["status_counts"] = mongo_stats.get("status_counts", {})
            
            return combined_stats
            
        except Exception as e:
            logger.error(f"❌ Error getting combined queue stats: {e}")
            return {"enabled": True, "error": str(e)}
    
    def get_combined_stats(self) -> Dict[str, Any]:
        """Get combined statistics from both watchers"""
        uptime = None
        if self.combined_stats['start_time']:
            uptime = (datetime.now(timezone.utc) - self.combined_stats['start_time']).total_seconds()
        
        stats = {
            **self.combined_stats,
            'uptime_seconds': uptime,
            'running': self.running,
            'mongodb_enabled': self.config.mongo_enabled,
            'sql_enabled': self.config.sql_enabled,
            'queue_enabled': self.config.queue_enabled,
            'queue_collection_name': self.config.queue_collection_name,
            'poll_interval': self.config.poll_interval,
            'extraction_success_rate': (
                self.combined_stats['successful_extractions'] / 
                max(1, self.combined_stats['total_changes']) * 100
                if self.combined_stats['total_changes'] > 0 else 0
            )
        }
        
        # Add individual watcher stats if available
        if self.mongo_watcher:
            stats['mongo_watcher_stats'] = self.mongo_watcher.get_stats()
        
        if self.sql_watcher:
            stats['sql_watcher_stats'] = self.sql_watcher.get_stats()
        
        return stats

# Example change handler with badge evaluation awareness
async def enhanced_combined_change_handler(change_info: Dict[str, Any]):
    """Enhanced example handler for combined database changes with queue entry creation info"""
    source = change_info.get('source', 'unknown')
    source_name = change_info.get('source_name', 'unknown')
    
    print(f"🔄 ENHANCED COMBINED CHANGE - {source.upper()}:")
    print(f"   Source: {source_name} ({change_info.get('source_type', 'unknown')})")
    print(f"   Operation: {change_info.get('operation_type', 'unknown')}")
    print(f"   Company ID: {change_info.get('company_id', 'N/A')}")
    print(f"   Site Code: {change_info.get('site_code', 'N/A')}")
    print(f"   Timestamp: {change_info.get('timestamp', 'N/A')}")
    
    if source == 'mongodb':
        print(f"   Collection: {change_info.get('collection_name', 'N/A')}")
        print(f"   Document ID: {change_info.get('change_id', 'N/A')}")
    elif source == 'sql':
        print(f"   Table: {change_info.get('table_name', 'N/A')}")
        print(f"   Primary Key: {change_info.get('primary_key', 'N/A')}")
    
    # Queue entry creation info
    company_id = change_info.get('company_id')
    site_code = change_info.get('site_code', '')
    if company_id:
        print(f"📋 Queue entry created/updated for evaluation: {company_id}:{site_code}")
    
    print("-" * 70)

def create_config_from_args(args) -> EnhancedCombinedWatcherConfig:
    """Create configuration from command line arguments and environment variables"""
    
    # Load configuration from environment variables FIRST
    print("📁 Loading configuration from .env file...")
    
    # Basic configuration from .env
    env_poll_interval = int(os.getenv('POLL_INTERVAL', '30'))
    env_batch_size = int(os.getenv('BATCH_SIZE', '1000'))
    env_status_report_interval = int(os.getenv('STATUS_REPORT_INTERVAL', '300'))
    env_max_retries = int(os.getenv('MAX_RETRIES', '3'))
    env_connection_timeout = int(os.getenv('CONNECTION_TIMEOUT', '30'))
    
    # MongoDB configuration from .env
    env_mongo_enabled = str_to_bool(os.getenv('MONGO_ENABLED', 'false'))
    env_mongo_uri = os.getenv('MONGO_URI', 'mongodb://localhost:27017')
    env_mongo_db_name = os.getenv('MONGO_DB_NAME', 'ensogove')
    env_mongo_collections_json = os.getenv('MONGO_COLLECTIONS', '[]')
    env_mongo_collections = parse_json_config(env_mongo_collections_json, 'MONGO_COLLECTIONS')
    
    # SQL configuration from .env
    env_sql_enabled = str_to_bool(os.getenv('SQL_ENABLED', 'false'))
    env_sql_host = os.getenv('SQL_HOST', '127.0.0.1')
    env_sql_user = os.getenv('SQL_USER', 'root')
    env_sql_password = os.getenv('SQL_PASSWORD', '')
    env_sql_database = os.getenv('SQL_DATABASE', 'ensogove')
    env_sql_port = int(os.getenv('SQL_PORT', '3306'))
    env_sql_auth_plugin = os.getenv('SQL_AUTH_PLUGIN', None)
    env_sql_ssl_disabled = str_to_bool(os.getenv('SQL_SSL_DISABLED', 'false'))
    env_sql_tables_json = os.getenv('SQL_TABLES', '[]')
    env_sql_tables = parse_json_config(env_sql_tables_json, 'SQL_TABLES')
    
    # Queue configuration from .env
    env_queue_enabled = str_to_bool(os.getenv('BADGE_EVALUATION_ENABLED', 'true'))  # Keep same env var name for compatibility
    env_queue_collection_name = os.getenv('BADGE_COLLECTION_NAME', 'badge_evaluation_queue')  # Keep same env var name for compatibility
    
    print(f"   📋 MongoDB Enabled: {env_mongo_enabled}")
    print(f"   📋 SQL Enabled: {env_sql_enabled}")
    print(f"   📋 Queue Creation: {env_queue_enabled}")
    print(f"   📋 Poll Interval: {env_poll_interval}s")
    
    # Create config object with .env defaults
    config = EnhancedCombinedWatcherConfig()
    
    # Set configuration from .env values FIRST (as defaults)
    config.poll_interval = env_poll_interval
    config.batch_size = env_batch_size
    config.status_report_interval = env_status_report_interval
    config.max_retries = env_max_retries
    config.connection_timeout = env_connection_timeout
    
    config.mongo_enabled = env_mongo_enabled
    config.mongo_uri = env_mongo_uri
    config.mongo_db_name = env_mongo_db_name
    config.mongo_collections = env_mongo_collections
    
    config.sql_enabled = env_sql_enabled
    config.sql_host = env_sql_host
    config.sql_user = env_sql_user
    config.sql_password = env_sql_password
    config.sql_database = env_sql_database
    config.sql_port = env_sql_port
    config.sql_auth_plugin = env_sql_auth_plugin
    config.sql_ssl_disabled = env_sql_ssl_disabled
    config.sql_tables = env_sql_tables
    
    config.queue_enabled = env_queue_enabled
    config.queue_collection_name = env_queue_collection_name
    
    # THEN override with CLI arguments if provided
    overrides_count = 0
    if hasattr(args, 'poll_interval') and args.poll_interval:
        config.poll_interval = args.poll_interval
        overrides_count += 1
        print(f"   🔧 CLI Override: poll_interval = {args.poll_interval}")
        
    if hasattr(args, 'batch_size') and args.batch_size:
        config.batch_size = args.batch_size
        overrides_count += 1
        print(f"   🔧 CLI Override: batch_size = {args.batch_size}")
    
    if hasattr(args, 'enable_mongo') and args.enable_mongo:
        config.mongo_enabled = True
        overrides_count += 1
        print(f"   🔧 CLI Override: mongo_enabled = True")
        
    if hasattr(args, 'enable_sql') and args.enable_sql:
        config.sql_enabled = True
        overrides_count += 1
        print(f"   🔧 CLI Override: sql_enabled = True")
        
    if hasattr(args, 'mongo_uri') and args.mongo_uri:
        config.mongo_uri = args.mongo_uri
        overrides_count += 1
        print(f"   🔧 CLI Override: mongo_uri = {args.mongo_uri}")
        
    if hasattr(args, 'mongo_db') and args.mongo_db:
        config.mongo_db_name = args.mongo_db
        overrides_count += 1
        print(f"   🔧 CLI Override: mongo_db_name = {args.mongo_db}")
        
    if hasattr(args, 'sql_host') and args.sql_host:
        config.sql_host = args.sql_host
        overrides_count += 1
        print(f"   🔧 CLI Override: sql_host = {args.sql_host}")
        
    if hasattr(args, 'sql_user') and args.sql_user:
        config.sql_user = args.sql_user
        overrides_count += 1
        print(f"   🔧 CLI Override: sql_user = {args.sql_user}")
        
    if hasattr(args, 'sql_password') and args.sql_password:
        config.sql_password = args.sql_password
        overrides_count += 1
        print(f"   🔧 CLI Override: sql_password = [HIDDEN]")
        
    if hasattr(args, 'sql_database') and args.sql_database:
        config.sql_database = args.sql_database
        overrides_count += 1
        print(f"   🔧 CLI Override: sql_database = {args.sql_database}")
        
    if hasattr(args, 'sql_port') and args.sql_port:
        config.sql_port = args.sql_port
        overrides_count += 1
        print(f"   🔧 CLI Override: sql_port = {args.sql_port}")
        
    # Only override queue settings if explicitly provided by user
    if hasattr(args, 'enable_badges') and args.enable_badges is not None:
        config.queue_enabled = args.enable_badges
        overrides_count += 1
        print(f"   🔧 CLI Override: queue_enabled = {args.enable_badges}")
        
    if hasattr(args, 'badge_collection') and args.badge_collection:
        config.queue_collection_name = args.badge_collection
        overrides_count += 1
        print(f"   🔧 CLI Override: queue_collection_name = {args.badge_collection}")
    
    if overrides_count == 0:
        print("   ✅ Using all .env defaults (no CLI overrides)")
    else:
        print(f"   ⚡ Applied {overrides_count} CLI overrides")
    
    return config

def setup_cli_parser() -> argparse.ArgumentParser:
    """Setup command line argument parser"""
    parser = argparse.ArgumentParser(
        description='Enhanced Combined Database Watcher - Creates Queue Entries for Badge Evaluation',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Watch both MongoDB and SQL with default settings
  python %(prog)s --enable-mongo --enable-sql
  
  # Watch only MongoDB with custom polling
  python %(prog)s --enable-mongo --poll-interval 60
  
  # Watch SQL with custom connection
  python %(prog)s --enable-sql --sql-host 192.168.1.100 --sql-user admin
  
  # Disable queue entry creation
  python %(prog)s --enable-mongo --no-badges
  
  # Enable file logging
  python %(prog)s --enable-mongo --log-level DEBUG --log-file ./logs/watcher.log

Environment Variables:
  See .env file for all available environment variables.
  Command line arguments override environment variables.
        """
    )
    
    # Database selection
    db_group = parser.add_argument_group('Database Selection')
    db_group.add_argument('--enable-mongo', action='store_true', help='Enable MongoDB watching')
    db_group.add_argument('--enable-sql', action='store_true', help='Enable SQL database watching')
    
    # MongoDB configuration
    mongo_group = parser.add_argument_group('MongoDB Configuration')
    mongo_group.add_argument('--mongo-uri', help='MongoDB connection URI')
    mongo_group.add_argument('--mongo-db', help='MongoDB database name')
    
    # SQL configuration
    sql_group = parser.add_argument_group('SQL Configuration')
    sql_group.add_argument('--sql-host', help='SQL database host')
    sql_group.add_argument('--sql-user', help='SQL database username')
    sql_group.add_argument('--sql-password', help='SQL database password')
    sql_group.add_argument('--sql-database', help='SQL database name')
    sql_group.add_argument('--sql-port', type=int, help='SQL database port')
    
    # Watcher configuration
    watcher_group = parser.add_argument_group('Watcher Configuration')
    watcher_group.add_argument('--poll-interval', type=int, help='Polling interval in seconds')
    watcher_group.add_argument('--batch-size', type=int, help='Batch size for processing changes')
    
    # Queue configuration
    queue_group = parser.add_argument_group('Queue Entry Creation')
    queue_group.add_argument('--enable-badges', dest='enable_badges', action='store_true', help='Enable queue entry creation for badge evaluation')
    queue_group.add_argument('--no-badges', dest='enable_badges', action='store_false', help='Disable queue entry creation')
    queue_group.add_argument('--badge-collection', help='Queue collection name for badge evaluation entries')
    
    # Set default for enable_badges to None so we can detect if it was explicitly set
    parser.set_defaults(enable_badges=None)
    
    # Logging configuration
    log_group = parser.add_argument_group('Logging Configuration')
    log_group.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'], 
                          default='INFO', help='Logging level')
    log_group.add_argument('--log-file', help='Log to file (provide file path)')
    log_group.add_argument('--quiet', action='store_true', help='Reduce console output')
    
    # Utility commands
    util_group = parser.add_argument_group('Utility Commands')
    util_group.add_argument('--test-config', action='store_true', help='Test configuration and exit')
    util_group.add_argument('--show-env', action='store_true', help='Show environment variables and exit')
    
    return parser

def show_environment_info():
    """Show relevant environment variables"""
    env_vars = [
        'MONGO_URI', 'MONGO_DB_NAME', 'MONGO_COLLECTIONS', 'MONGO_ENABLED',
        'SQL_HOST', 'SQL_PORT', 'SQL_USER', 'SQL_DATABASE', 'SQL_TABLES', 'SQL_ENABLED',
        'POLL_INTERVAL', 'BATCH_SIZE', 'BADGE_EVALUATION_ENABLED', 'BADGE_COLLECTION_NAME',
        'LOG_LEVEL', 'LOG_TO_FILE', 'LOG_FILE_PATH'
    ]
    
    print("🔧 Environment Variables:")
    print("=" * 50)
    for var in env_vars:
        value = os.getenv(var, 'Not Set')
        # Hide password
        if 'PASSWORD' in var and value != 'Not Set':
            value = '*' * len(value)
        print(f"{var:25} = {value}")

async def test_configuration(config: EnhancedCombinedWatcherConfig):
    """Test the configuration without starting watchers"""
    print("🧪 Testing Configuration...")
    print("=" * 50)
    
    success = True
    
    # Test MongoDB configuration
    if config.mongo_enabled:
        print("🍃 MongoDB Configuration:")
        print(f"   URI: {config.mongo_uri}")
        print(f"   Database: {config.mongo_db_name}")
        print(f"   Collections: {len(config.mongo_collections)}")
        for i, col in enumerate(config.mongo_collections):
            print(f"     {i+1}. {col.get('collection_name', 'Unknown')}")
        
        if not MONGO_AVAILABLE:
            print("   ❌ MongoDB watcher not available")
            success = False
        else:
            print("   ✅ MongoDB watcher available")
    
    # Test SQL configuration
    if config.sql_enabled:
        print("\n🗄️ SQL Configuration:")
        print(f"   Host: {config.sql_host}:{config.sql_port}")
        print(f"   Database: {config.sql_database}")
        print(f"   User: {config.sql_user}")
        print(f"   Tables: {len(config.sql_tables)}")
        for i, table in enumerate(config.sql_tables):
            print(f"     {i+1}. {table.get('table_name', 'Unknown')}")
        
        if not SQL_AVAILABLE:
            print("   ❌ SQL watcher not available")
            success = False
        else:
            print("   ✅ SQL watcher available")
    
    # Test Queue configuration
    print(f"\n📋 Queue Entry Creation: {'Enabled' if config.queue_enabled else 'Disabled'}")
    if config.queue_enabled:
        print(f"   Collection: {config.queue_collection_name}")
    
    # General configuration
    print(f"\n⚙️ General Configuration:")
    print(f"   Poll Interval: {config.poll_interval} seconds")
    print(f"   Batch Size: {config.batch_size}")
    print(f"   Status Report Interval: {config.status_report_interval} seconds")
    
    if not config.mongo_enabled and not config.sql_enabled:
        print("\n❌ No databases enabled! Use --enable-mongo or --enable-sql")
        success = False
    
    if success:
        print("\n✅ Configuration looks good!")
    else:
        print("\n❌ Configuration has issues!")
    
    return success

async def main():
    """Main function with CLI argument handling - Database Watcher with Queue Entry Creation"""
    # Debug: Check if main is being called multiple times
    if hasattr(main, '_called'):
        print("⚠️  WARNING: main() function called multiple times!")
        return
    main._called = True
    
    print("🚀 Enhanced Combined Database Watcher - Queue Entry Creator")
    print("=" * 70)
    
    # Setup CLI parser
    parser = setup_cli_parser()
    args = parser.parse_args()
    
    # Handle utility commands first
    if args.show_env:
        show_environment_info()
        return
    
    # Setup logging with .env values as defaults
    log_level = args.log_level if hasattr(args, 'log_level') and args.log_level != 'INFO' else os.getenv('LOG_LEVEL', 'INFO')
    log_to_file = bool(args.log_file) if hasattr(args, 'log_file') and args.log_file else str_to_bool(os.getenv('LOG_TO_FILE', 'false'))
    log_file_path = args.log_file if hasattr(args, 'log_file') and args.log_file else os.getenv('LOG_FILE_PATH', './logs/watcher.log')
    
    setup_logging(
        log_level=log_level,
        log_to_file=log_to_file,
        log_file_path=log_file_path if log_to_file else None
    )
    
    # Create configuration from CLI args and environment
    config = create_config_from_args(args)
    
    # Test configuration if requested
    if args.test_config:
        success = await test_configuration(config)
        sys.exit(0 if success else 1)
    
    # Show startup message with current configuration
    print(f"\n📋 Configuration Summary:")
    print(f"   MongoDB: {'✅ Enabled' if config.mongo_enabled else '❌ Disabled'}")
    if config.mongo_enabled:
        print(f"     URI: {config.mongo_uri}")
        print(f"     Database: {config.mongo_db_name}")
        print(f"     Collections: {len(config.mongo_collections)}")
    
    print(f"   SQL: {'✅ Enabled' if config.sql_enabled else '❌ Disabled'}")
    if config.sql_enabled:
        print(f"     Host: {config.sql_host}:{config.sql_port}")
        print(f"     Database: {config.sql_database}")
        print(f"     Tables: {len(config.sql_tables)}")
    
    print(f"   Queue Creation: {'✅ Enabled' if config.queue_enabled else '❌ Disabled'}")
    print(f"   Poll Interval: {config.poll_interval} seconds")
    print(f"   Log Level: {log_level}")
    
    # Create enhanced combined watcher
    watcher = EnhancedCombinedDatabaseWatcher(config)
    
    # Configure watchers based on enabled options
    mongodb_configured = False
    sql_configured = False
    
    if config.mongo_enabled:
        mongodb_configured = watcher.configure_mongodb(
            mongo_uri=config.mongo_uri,
            db_name=config.mongo_db_name,
            collections=config.mongo_collections
        )
    
    if config.sql_enabled:
        sql_configured = watcher.configure_sql(
            host=config.sql_host,
            user=config.sql_user,
            password=config.sql_password,
            database=config.sql_database,
            port=config.sql_port,
            tables=config.sql_tables,
            auth_plugin=config.sql_auth_plugin,
            ssl_disabled=config.sql_ssl_disabled
        )
    
    # Configure queue entry creation
    watcher.configure_queue(
        enabled=config.queue_enabled,
        collection_name=config.queue_collection_name
    )
    
    # Add change handler if not in quiet mode
    if not (hasattr(args, 'quiet') and args.quiet):
        watcher.add_change_handler(enhanced_combined_change_handler)
        logger.info("🔧 Added combined change handler for database changes")
    
    # Check if any databases are configured
    if not mongodb_configured and not sql_configured:
        print("\n⚠️  No databases could be configured!")
        print("🔧 Please check:")
        print("   1. Set MONGO_ENABLED=true or SQL_ENABLED=true in .env file")
        print("   2. Or use --enable-mongo or --enable-sql command line flags")
        print("   3. Verify your database connection settings in .env")
        print("   4. Ensure your watcher modules are available")
        print("   5. Check your collections/tables configuration")
        print("\n💡 Use --test-config to validate your settings")
        print("💡 Use --show-env to see current environment variables")
        return 1
    
    try:
        print(f"\n🔧 Starting enhanced combined watcher...")
        print("💡 Make changes to your configured databases to see them detected!")
        if config.queue_enabled:
            print("💡 Queue entries will be created automatically for badge evaluation!")
        print("💡 Press Ctrl+C to stop")
        print("-" * 70)
        
        # Start watching (this will run until interrupted)
        await watcher.start_watching()
        
    except KeyboardInterrupt:
        print("\n🛑 Stopping enhanced combined watcher...")
        
        # Show final statistics
        stats = watcher.get_combined_stats()
        print("\n📊 Final Combined Statistics:")
        print(f"Total changes processed: {stats['total_changes']}")
        print(f"MongoDB changes: {stats['changes_by_source']['mongodb']}")
        print(f"SQL changes: {stats['changes_by_source']['sql']}")
        print(f"Successful extractions: {stats['successful_extractions']}")
        print(f"Failed extractions: {stats['failed_extractions']}")
        print(f"Success rate: {stats['extraction_success_rate']:.1f}%")
        
        if stats['changes_by_collection_table']:
            print(f"Changes by source: {stats['changes_by_collection_table']}")
        
        # Show queue entry summary
        if config.queue_enabled:
            try:
                queue_summary = await watcher.get_queue_summary()
                print(f"\n📋 Queue Entry Summary:")
                print(f"Total entries in queue: {queue_summary.get('total_entries', 0)}")
                print(f"Recent entries (1h): {queue_summary.get('recent_entries_1h', 0)}")
                print(f"Created this session: {queue_summary.get('entries_created_this_session', 0)}")
                print(f"Updated this session: {queue_summary.get('entries_updated_this_session', 0)}")
                if queue_summary.get('status_counts'):
                    print(f"Status counts: {queue_summary['status_counts']}")
            except Exception as e:
                logger.error(f"❌ Error getting queue summary: {e}")
        
        if 'mongo_watcher_stats' in stats:
            mongo_stats = stats['mongo_watcher_stats']
            print(f"\n🍃 MongoDB Stats: {mongo_stats['polling_cycles']} cycles")
        
        if 'sql_watcher_stats' in stats:
            sql_stats = stats['sql_watcher_stats']
            print(f"🗄️ SQL Stats: {sql_stats['polling_cycles']} cycles")
            
        return 0
        
    except Exception as e:
        logger.error(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    # Debug: Check for duplicate execution
    import sys
    print(f"🐛 DEBUG: Script execution started, __name__ = {__name__}")
    print(f"🐛 DEBUG: sys.argv = {sys.argv}")
    
    exit_code = asyncio.run(main())
    sys.exit(exit_code)