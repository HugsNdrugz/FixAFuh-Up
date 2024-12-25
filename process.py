import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from sqlalchemy import (
    create_engine, text, Column, Integer, String, DateTime, 
    UniqueConstraint, ForeignKey, event, inspect, Float, Boolean,
    Table, MetaData, and_, or_, not_, func
)
from sqlalchemy.orm import declarative_base, sessionmaker, relationship, scoped_session
from sqlalchemy.exc import SQLAlchemyError, IntegrityError, OperationalError
from sqlalchemy.engine import Engine
from sqlalchemy.pool import QueuePool
from typing import Dict, List, Optional, Union, Any, Tuple, Set
import logging
from logging.handlers import RotatingFileHandler
from dateutil import parser
import traceback
import glob
from pathlib import Path
import shutil
import json
from dataclasses import dataclass, field
import hashlib
from contextlib import contextmanager
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import queue
import signal
import time
import re
from enum import Enum, auto
import uuid
import warnings
import tempfile
import csv

try:
    import openpyxl
    OPENPYXL_AVAILABLE = True
except ImportError:
    OPENPYXL_AVAILABLE = False
    warnings.warn("openpyxl not available. Excel support will be limited.", ImportWarning)

# --------- Configuration ---------
@dataclass
class AppConfig:
    """Application configuration with enhanced settings and validation"""
    # Base directories
    BASE_DIR: Path = field(default_factory=lambda: Path.cwd())
    DATA_DIR: Path = field(default_factory=lambda: Path.cwd() / "data")
    UPLOAD_FOLDER: Path = field(default_factory=lambda: Path.cwd() / "uploads")
    PROCESSED_FOLDER: Path = field(default_factory=lambda: Path.cwd() / "processed")
    FAILED_FOLDER: Path = field(default_factory=lambda: Path.cwd() / "failed")
    ARCHIVE_FOLDER: Path = field(default_factory=lambda: Path.cwd() / "archive")
    BACKUP_FOLDER: Path = field(default_factory=lambda: Path.cwd() / "backups")
    
    # Database settings
    DB_PATH: Path = field(default_factory=lambda: Path.cwd() / "data.db")
    DB_BACKUP_COUNT: int = 5
    DB_POOL_SIZE: int = 5
    DB_MAX_OVERFLOW: int = 10
    DB_POOL_TIMEOUT: int = 30
    DB_POOL_RECYCLE: int = 3600
    
    # Logging settings
    LOG_DIR: Path = field(default_factory=lambda: Path.cwd() / "logs")
    LOG_FILE: Path = field(default_factory=lambda: Path.cwd() / "logs" / "data_processor.log")
    LOG_LEVEL: int = logging.INFO
    LOG_FORMAT: str = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    LOG_MAX_BYTES: int = 10 * 1024 * 1024  # 10MB
    LOG_BACKUP_COUNT: int = 5
    
    # Processing settings
    MAX_WORKERS: int = max(4, os.cpu_count() or 4)
    BATCH_SIZE: int = 1000
    MAX_RETRIES: int = 3
    RETRY_DELAY: int = 5
    ALLOWED_EXTENSIONS: Set[str] = field(default_factory=lambda: {'.csv', '.xls', '.xlsx'})
    
    # Feature flags
    ENABLE_THREADING: bool = True
    ENABLE_BACKUPS: bool = True
    ENABLE_COMPRESSION: bool = True
    ENABLE_MONITORING: bool = True
    
    def __post_init__(self):
        """Initialize and validate configuration"""
        self._create_directories()
        self._validate_settings()
        self._setup_derived_values()
    
    def _create_directories(self):
        """Create necessary directories if they don't exist"""
        directories = [
            self.BASE_DIR, self.DATA_DIR, self.UPLOAD_FOLDER,
            self.PROCESSED_FOLDER, self.FAILED_FOLDER, self.ARCHIVE_FOLDER,
            self.BACKUP_FOLDER, self.LOG_DIR
        ]
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)
    
    def _validate_settings(self):
        """Validate configuration settings"""
        if self.MAX_WORKERS < 1:
            raise ValueError("MAX_WORKERS must be at least 1")
        if self.BATCH_SIZE < 1:
            raise ValueError("BATCH_SIZE must be at least 1")
        if not self.ALLOWED_EXTENSIONS:
            raise ValueError("ALLOWED_EXTENSIONS cannot be empty")
    
    def _setup_derived_values(self):
        """Setup any derived configuration values"""
        self.TEMP_DIR = Path(tempfile.gettempdir()) / "data_processor"
        self.TEMP_DIR.mkdir(parents=True, exist_ok=True)
        
        # Setup monitoring paths if enabled
        if self.ENABLE_MONITORING:
            self.MONITORING_DIR = self.BASE_DIR / "monitoring"
            self.MONITORING_DIR.mkdir(parents=True, exist_ok=True)

config = AppConfig()

# --------- Logging Setup ---------
class CustomLogger:
    """Enhanced logging functionality with rotation and multiple handlers"""
    
    def __init__(self, name: str = 'DataProcessor'):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(config.LOG_LEVEL)
        self.setup_handlers()
        
        # Tracking metrics
        self.error_count = 0
        self.warning_count = 0
        self.start_time = datetime.now()
    
    def setup_handlers(self):
        """Setup file and console handlers with rotation"""
        # Remove existing handlers
        self.logger.handlers = []
        
        # File handler with rotation
        file_handler = RotatingFileHandler(
            config.LOG_FILE,
            maxBytes=config.LOG_MAX_BYTES,
            backupCount=config.LOG_BACKUP_COUNT
        )
        file_handler.setFormatter(logging.Formatter(config.LOG_FORMAT))
        self.logger.addHandler(file_handler)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter(config.LOG_FORMAT))
        self.logger.addHandler(console_handler)
    
    def error(self, message: str, exc_info: bool = True):
        """Log error message and increment error count"""
        self.error_count += 1
        self.logger.error(message, exc_info=exc_info)
    
    def warning(self, message: str):
        """Log warning message and increment warning count"""
        self.warning_count += 1
        self.logger.warning(message)
    
    def info(self, message: str):
        """Log info message"""
        self.logger.info(message)
    
    def debug(self, message: str):
        """Log debug message"""
        self.logger.debug(message)
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get logging statistics"""
        return {
            'start_time': self.start_time,
            'run_time': datetime.now() - self.start_time,
            'error_count': self.error_count,
            'warning_count': self.warning_count
        }

# Initialize logger
logger = CustomLogger()
# --------- Database Models ---------
Base = declarative_base()

class BaseModel(Base):
    """Abstract base model with common fields and functionality"""
    __abstract__ = True

    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    hash_id = Column(String(64), unique=True)
    _version = Column('version', Integer, default=1)
    _is_deleted = Column('is_deleted', Boolean, default=False)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.hash_id = self.generate_hash()

    def generate_hash(self) -> str:
        """Generate unique hash for record"""
        data = json.dumps(self.to_dict(), sort_keys=True).encode()
        return hashlib.sha256(data).hexdigest()

    def to_dict(self) -> dict:
        """Convert model to dictionary excluding SQLAlchemy special attributes"""
        return {
            column.name: getattr(self, column.name)
            for column in self.__table__.columns
            if not column.name.startswith('_')
        }

    @classmethod
    def from_dict(cls, data: dict) -> 'BaseModel':
        """Create model instance from dictionary"""
        return cls(**{
            k: v for k, v in data.items()
            if k in cls.__table__.columns.keys()
        })

    def update(self, data: dict) -> None:
        """Update model with dictionary data"""
        for key, value in data.items():
            if hasattr(self, key):
                setattr(self, key, value)
        self._version += 1
        self.updated_at = datetime.utcnow()

class Keylog(BaseModel):
    """Enhanced keylog entry model"""
    __tablename__ = 'keylogs'

    application = Column(String, primary_key=True)
    time = Column(String, primary_key=True)
    time_dt = Column(DateTime, index=True)
    text = Column(String, primary_key=True)
    package_id = Column(String, ForeignKey('installedapps.package_name'))

    # Relationships
    installed_app = relationship("InstalledApp", back_populates="keylogs")

    def __repr__(self):
        return f"<Keylog(app={self.application}, time={self.time}, text={self.text[:20]}...)>"

class SmsMessage(BaseModel):
    """Enhanced SMS message model"""
    __tablename__ = 'sms_messages'

    sms_type = Column(String, primary_key=True)
    time = Column(String, primary_key=True)
    time_dt = Column(DateTime, index=True)
    from_to = Column(String, primary_key=True)
    text = Column(String, primary_key=True)
    location_id = Column(Integer, ForeignKey('locations.location_id'))
    contact_id = Column(Integer, ForeignKey('contacts.contact_id'))

    # Relationships
    location = relationship("Location", back_populates="sms_messages")
    contact = relationship("Contact", back_populates="sms_messages")

    def __repr__(self):
        return f"<SmsMessage(type={self.sms_type}, time={self.time}, from_to={self.from_to})>"

class ChatMessage(BaseModel):
    """Enhanced chat message model"""
    __tablename__ = 'chat_messages'

    messenger = Column(String, primary_key=True)
    time = Column(String, primary_key=True)
    time_dt = Column(DateTime, index=True)
    sender = Column(String, primary_key=True)
    text = Column(String, primary_key=True)
    contact_id = Column(Integer, ForeignKey('contacts.contact_id'))

    # Relationships
    contact = relationship("Contact", back_populates="chat_messages")

    def __repr__(self):
        return f"<ChatMessage(messenger={self.messenger}, sender={self.sender}, time={self.time})>"

class Contact(BaseModel):
    """Enhanced contact model"""
    __tablename__ = 'contacts'

    contact_id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String)
    phone_number = Column(String)
    email_id = Column(String)
    last_contacted = Column(String)
    last_contacted_dt = Column(DateTime, index=True)

    # Relationships
    sms_messages = relationship("SmsMessage", back_populates="contact")
    chat_messages = relationship("ChatMessage", back_populates="contact")
    calls = relationship("Call", back_populates="contact")

    __table_args__ = (
        UniqueConstraint('name', 'phone_number', 'email_id', name='unique_contact'),
    )

    def __repr__(self):
        return f"<Contact(id={self.contact_id}, name={self.name})>"

class Call(BaseModel):
    """Enhanced call model"""
    __tablename__ = 'calls'

    call_type = Column(String, primary_key=True)
    time = Column(String, primary_key=True)
    time_dt = Column(DateTime, index=True)
    from_to = Column(String, primary_key=True)
    duration = Column(Integer)
    location_id = Column(Integer, ForeignKey('locations.location_id'))
    contact_id = Column(Integer, ForeignKey('contacts.contact_id'))

    # Relationships
    location = relationship("Location", back_populates="calls")
    contact = relationship("Contact", back_populates="calls")

    def __repr__(self):
        return f"<Call(type={self.call_type}, time={self.time}, duration={self.duration}s)>"

class InstalledApp(BaseModel):
    """Enhanced installed app model"""
    __tablename__ = 'installedapps'

    application_name = Column(String, primary_key=True)
    package_name = Column(String, primary_key=True)
    installed_date = Column(DateTime, index=True)

    # Relationships
    keylogs = relationship("Keylog", back_populates="installed_app")

    def __repr__(self):
        return f"<InstalledApp(name={self.application_name}, package={self.package_name})>"

class Location(BaseModel):
    """Enhanced location model"""
    __tablename__ = 'locations'

    location_id = Column(Integer, primary_key=True, autoincrement=True)
    location_text = Column(String, unique=True)
    latitude = Column(Float, nullable=True)
    longitude = Column(Float, nullable=True)
    accuracy = Column(Float, nullable=True)

    # Relationships
    sms_messages = relationship("SmsMessage", back_populates="location")
    calls = relationship("Call", back_populates="location")

    def __repr__(self):
        return f"<Location(id={self.location_id}, text={self.location_text})>"

# --------- Database Management ---------
class DatabaseManager:
    """Enhanced database management with connection pooling and error handling"""
    
    def __init__(self, db_path: Union[str, Path]):
        self.db_path = Path(db_path)
        self._engine = self._create_engine()
        self._session_factory = scoped_session(sessionmaker(bind=self._engine))
        self.initialize_database()
        self._setup_engine_events()

    def _create_engine(self) -> Engine:
        """Create SQLAlchemy engine with connection pooling"""
        return create_engine(
            f"sqlite:///{self.db_path}",
            poolclass=QueuePool,
            pool_size=config.DB_POOL_SIZE,
            max_overflow=config.DB_MAX_OVERFLOW,
            pool_timeout=config.DB_POOL_TIMEOUT,
            pool_recycle=config.DB_POOL_RECYCLE
        )

    def _setup_engine_events(self):
        """Setup SQLAlchemy engine events"""
        @event.listens_for(self._engine, 'connect')
        def set_sqlite_pragma(dbapi_connection, connection_record):
            cursor = dbapi_connection.cursor()
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.execute("PRAGMA synchronous=NORMAL")
            cursor.close()

    @contextmanager
    def session_scope(self):
        """Provide a transactional scope around a series of operations"""
        session = self._session_factory()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Database session error: {str(e)}")
            raise
        finally:
            session.close()

    def initialize_database(self):
        """Initialize database schema and create tables"""
        try:
            Base.metadata.create_all(self._engine)
            logger.info("Database initialized successfully")
        except SQLAlchemyError as e:
            logger.error(f"Database initialization failed: {str(e)}")
            raise

    def backup_database(self) -> Optional[Path]:
        """Create a backup of the database"""
        if not config.ENABLE_BACKUPS:
            return None

        try:
            timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            backup_path = config.BACKUP_FOLDER / f"backup_{timestamp}.db"
            
            # Ensure we're not in WAL mode during backup
            with self._engine.connect() as conn:
                conn.execute(text("PRAGMA journal_mode=DELETE"))
            
            shutil.copy2(self.db_path, backup_path)
            
            # Restore WAL mode
            with self._engine.connect() as conn:
                conn.execute(text("PRAGMA journal_mode=WAL"))
            
            # Clean old backups
            self._cleanup_old_backups()
            
            logger.info(f"Database backed up to {backup_path}")
            return backup_path
        except Exception as e:
            logger.error(f"Database backup failed: {str(e)}")
            return None

    def _cleanup_old_backups(self):
        """Remove old database backups keeping only the most recent ones"""
        try:
            backups = sorted(
                config.BACKUP_FOLDER.glob("backup_*.db"),
                key=lambda x: x.stat().st_mtime,
                reverse=True
            )
            
            for backup in backups[config.DB_BACKUP_COUNT:]:
                backup.unlink()
                logger.debug(f"Removed old backup: {backup}")
        except Exception as e:
            logger.error(f"Error cleaning up old backups: {str(e)}")
# --------- Data Processing Components ---------
class DataProcessingError(Exception):
    """Custom exception for data processing errors"""
    pass

class ProcessingStatus(Enum):
    """Enum for tracking processing status"""
    PENDING = auto()
    IN_PROGRESS = auto()
    COMPLETED = auto()
    FAILED = auto()
    INVALID = auto()

@dataclass
class ProcessingResult:
    """Container for processing results"""
    status: ProcessingStatus
    rows_processed: int = 0
    rows_failed: int = 0
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    processing_time: float = 0.0
    table_name: Optional[str] = None
    file_path: Optional[Path] = None

class DataValidator:
    """Validates data before processing"""
    
    @staticmethod
    def validate_date_format(value: str) -> bool:
        """Validate date string format"""
        try:
            parser.parse(value)
            return True
        except (ValueError, TypeError):
            return False

    @staticmethod
    def validate_phone_number(value: str) -> bool:
        """Validate phone number format"""
        if not value:
            return True
        pattern = re.compile(r'^\+?1?\d{9,15}$')
        return bool(pattern.match(str(value)))

    @staticmethod
    def validate_email(value: str) -> bool:
        """Validate email format"""
        if not value:
            return True
        pattern = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
        return bool(pattern.match(str(value)))

    @staticmethod
    def validate_duration(value: Any) -> bool:
        """Validate duration value"""
        try:
            if pd.isna(value):
                return True
            return float(value) >= 0
        except (ValueError, TypeError):
            return False

class BaseTransform:
    """Enhanced base transformation class"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.validator = DataValidator()
        self.errors = []
        self.warnings = []

    def clean_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Enhanced column cleaning with validation and error handling
        """
        try:
            # Create a copy to avoid modifying the original
            df = df.copy()
            
            # Normalize column names
            df.columns = [
                self._normalize_column_name(col) 
                for col in df.columns
            ]
            
            # Clean string columns
            for col in df.select_dtypes(include=['object']):
                df[col] = df[col].apply(self._clean_string)
            
            # Remove completely empty rows and metadata
            df = df.dropna(how='all')
            if df.iloc[0].str.contains('Tracking Smartphone').any():
                    df = df.iloc[1:]
            
            # Reset index
            df = df.reset_index(drop=True)
            
            return df
        except Exception as e:
            error_msg = f"Error in clean_columns: {str(e)}"
            self.errors.append(error_msg)
            logger.error(error_msg)
            raise DataProcessingError(error_msg)

    @staticmethod
    def _normalize_column_name(column: str) -> str:
        """Normalize column name"""
        return (
            str(column)
            .lower()
            .replace(' ', '_')
            .replace('-', '_')
            .replace('/', '_')
            .replace('\\', '_')
            .replace('(', '')
            .replace(')', '')
            .strip()
        )

    @staticmethod
    def _clean_string(value: Any) -> str:
        """Clean string values"""
        if pd.isna(value):
            return ''
        return str(value).strip()

    def validate_dataframe(self, df: pd.DataFrame, required_columns: List[str]) -> bool:
        """Validate DataFrame structure and content"""
        # Check required columns
        missing_columns = set(required_columns) - set(df.columns)
        if missing_columns:
            self.errors.append(f"Missing required columns: {missing_columns}")
            return False
        
        return True

class KeylogTransform(BaseTransform):
    """Enhanced keylog data transformation"""
    
    required_columns = ['application', 'time', 'text']
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform keylog data"""
        try:
            # Validate input
            if not self.validate_dataframe(df, self.required_columns):
                raise DataProcessingError("Invalid keylog data structure")
            
            # Clean data
            df = self.clean_columns(df)
            
            # Process time
            df['time_dt'] = df['time'].apply(self._parse_datetime)
            
            # Fill missing values
            df['text'] = df['text'].fillna('')
            df['package_id'] = df['package_id'].fillna('')
            
            # Validate transformed data
            self._validate_transformed_data(df)
            
            return df
        except Exception as e:
            error_msg = f"Error transforming keylog data: {str(e)}"
            self.errors.append(error_msg)
            logger.error(error_msg)
            raise DataProcessingError(error_msg)

    def _validate_transformed_data(self, df: pd.DataFrame) -> None:
        """Validate transformed keylog data"""
        # Check for invalid timestamps
        invalid_times = df[df['time_dt'].isna()]['time'].unique()
        if len(invalid_times) > 0:
            self.warnings.append(f"Invalid timestamps found: {invalid_times}")

class SmsMessageTransform(BaseTransform):
    """Enhanced SMS message transformation"""
    
    required_columns = ['sms_type', 'time', 'from_to', 'text']
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        try:
            # Validate input
            if not self.validate_dataframe(df, self.required_columns):
                raise DataProcessingError("Invalid SMS data structure")
            
            # Clean data
            df = self.clean_columns(df)
            
            # Process time
            df['time_dt'] = df['time'].apply(self._parse_datetime)
            
            # Clean phone numbers
            df['from_to'] = df['from_to'].apply(self._clean_phone_number)
            
            # Process location references
            df = self._process_locations(df)
            
            # Process contacts
            df = self._process_contacts(df)
            
            return df
        except Exception as e:
            error_msg = f"Error transforming SMS data: {str(e)}"
            self.errors.append(error_msg)
            logger.error(error_msg)
            raise DataProcessingError(error_msg)

    def _clean_phone_number(self, number: str) -> str:
        """Clean and validate phone numbers"""
        if pd.isna(number):
            return ''
        
        # Remove non-numeric characters
        cleaned = re.sub(r'[^\d+]', '', str(number))
        
        if not self.validator.validate_phone_number(cleaned):
            self.warnings.append(f"Invalid phone number format: {number}")
        
        return cleaned

    def _process_locations(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process and link location data"""
        if 'location' not in df.columns:
            df['location_id'] = None
            return df
        
        with self.db_manager.session_scope() as session:
            for location_text in df['location'].unique():
                if pd.isna(location_text):
                    continue
                    
                location = get_or_create_record(
                    session,
                    Location,
                    location_text=location_text
                )
                
                df.loc[df['location'] == location_text, 'location_id'] = location.location_id
        
        return df

    def _process_contacts(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process and link contact data"""
        df['contact_id'] = None
        
        with self.db_manager.session_scope() as session:
            for from_to in df['from_to'].unique():
                if not from_to:
                    continue
                    
                contact = get_or_create_record(
                    session,
                    Contact,
                    phone_number=from_to
                )
                
                df.loc[df['from_to'] == from_to, 'contact_id'] = contact.contact_id
        
        return df

# Continue with other transform classes...
class ChatMessageTransform(BaseTransform):
    """Enhanced chat message transformation"""
    
    required_columns = ['messenger', 'time', 'sender', 'text']
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform chat message data with enhanced validation and error handling"""
        try:
            # Make a copy to avoid modifying the original
            df = df.copy()
            
            # First, clean the columns
            df = self.clean_columns(df)
            
            # Log the actual columns for debugging
            logger.debug(f"Actual columns in DataFrame: {list(df.columns)}")
            
            # Validate DataFrame structure with detailed feedback
            missing_columns = self._validate_columns(df)
            if missing_columns:
                error_msg = f"Missing required columns: {missing_columns}. Found columns: {list(df.columns)}"
                logger.error(error_msg)
                raise DataProcessingError(error_msg)
            
            # Process timestamps
            df['time_dt'] = df['time'].apply(self._parse_datetime)
            invalid_times = df[df['time_dt'].isna()]
            if not invalid_times.empty:
                logger.warning(f"Found {len(invalid_times)} invalid timestamps")
                logger.debug(f"Sample invalid times: {invalid_times['time'].head()}")
            
            # Clean and validate messenger values
            df['messenger'] = df['messenger'].fillna('unknown')
            df['messenger'] = df['messenger'].str.lower().str.strip()
            self._validate_messenger_platforms(df)
            
            # Clean and validate sender values
            df['sender'] = df['sender'].fillna('unknown')
            df['sender'] = df['sender'].str.strip()
            
            # Clean text content
            df['text'] = df['text'].fillna('')
            df['text'] = df['text'].str.strip()
            
            # Process contacts
            df = self._process_contacts(df)
            
            # Final validation
            self._validate_transformed_data(df)
            
            return df
            
        except DataProcessingError as e:
            raise e
        except Exception as e:
            error_msg = f"Error transforming chat message data: {str(e)}"
            logger.error(error_msg)
            logger.error(traceback.format_exc())
            raise DataProcessingError(error_msg)

    def _validate_columns(self, df: pd.DataFrame) -> List[str]:
        """Validate DataFrame columns and return list of missing columns"""
        current_columns = set(df.columns)
        required_columns = set(self.required_columns)
        return list(required_columns - current_columns)

    def _validate_messenger_platforms(self, df: pd.DataFrame) -> None:
        """Validate messenger platform names with warnings for unknown platforms"""
        known_platforms = {
            'whatsapp', 'telegram', 'signal', 'messenger', 'facebook', 
            'instagram', 'line', 'wechat', 'viber', 'unknown'
        }
        
        platforms = set(df['messenger'].unique())
        unknown_platforms = platforms - known_platforms
        
        if unknown_platforms:
            logger.warning(f"Unknown messenger platforms found: {unknown_platforms}")

    def _process_contacts(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process and link contact data from chat messages"""
        df['contact_id'] = None
        
        try:
            with self.db_manager.session_scope() as session:
                unique_senders = df[df['sender'] != 'unknown']['sender'].unique()
                
                for sender in unique_senders:
                    contact = get_or_create_record(
                        session,
                        Contact,
                        name=sender
                    )
                    
                    df.loc[df['sender'] == sender, 'contact_id'] = contact.contact_id
                    
            return df
            
        except Exception as e:
            logger.error(f"Error processing contacts: {str(e)}")
            logger.error(traceback.format_exc())
            return df

    def _validate_transformed_data(self, df: pd.DataFrame) -> None:
        """Perform final validation on transformed data"""
        # Check for required columns
        for col in self.required_columns:
            if col not in df.columns:
                raise DataProcessingError(f"Required column '{col}' missing after transformation")
        
        # Check for empty DataFrame
        if df.empty:
            raise DataProcessingError("DataFrame is empty after transformation")
        
        # Check for all null values in required columns
        for col in self.required_columns:
            if df[col].isna().all():
                raise DataProcessingError(f"Column '{col}' contains all null values")
        
        # Validate data types
        expected_types = {
            'messenger': 'object',
            'time': 'object',
            'time_dt': 'datetime64[ns]',
            'sender': 'object',
            'text': 'object',
            'contact_id': 'float64'  # Can be null, so float64
        }
        
        for col, expected_type in expected_types.items():
            if col in df.columns:
                actual_type = str(df[col].dtype)
                if actual_type != expected_type:
                    logger.warning(f"Column '{col}' has type '{actual_type}', expected '{expected_type}'")

    @staticmethod
    def _parse_datetime(value: Any) -> Optional[datetime]:
        """Parse datetime with multiple format attempts"""
        if pd.isna(value):
            return None
            
        try:
            # Try pandas to_datetime first
            return pd.to_datetime(value)
        except Exception:
            try:
                # Try dateutil parser
                return parser.parse(str(value))
            except Exception as e:
                logger.warning(f"Failed to parse datetime: {value} ({str(e)})")
                return None

    def clean_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Enhanced column cleaning with validation"""
        try:
            df = super().clean_columns(df)
            
            # Additional cleaning specific to chat messages
            column_mapping = {
                'app': 'messenger',
                'application': 'messenger',
                'platform': 'messenger',
                'timestamp': 'time',
                'date': 'time',
                'datetime': 'time',
                'user': 'sender',
                'username': 'sender',
                'from': 'sender',
                'message': 'text',
                'content': 'text',
                'body': 'text'
            }
            
            # Rename columns if they exist
            df.rename(columns=column_mapping, inplace=True)
            
            return df
            
        except Exception as e:
            error_msg = f"Error cleaning columns: {str(e)}"
            logger.error(error_msg)
            logger.error(traceback.format_exc())
            raise DataProcessingError(error_msg)
class CallTransform(BaseTransform):
    """Enhanced call data transformation"""
    
    required_columns = ['call_type', 'time', 'from_to', 'duration_(sec)']
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        try:
            if not self.validate_dataframe(df, self.required_columns):
                raise DataProcessingError("Invalid call data structure")
            
            df = self.clean_columns(df)
            
            # Process timestamps
            df['time_dt'] = df['time'].apply(self._parse_datetime)
            
            # Process duration
            df['duration'] = df['duration_(sec)'].apply(self._parse_duration)
            df = df.drop(columns=['duration_(sec)'])
            
            # Clean phone numbers
            df['from_to'] = df['from_to'].apply(self._clean_phone_number)
            
            # Process locations and contacts
            df = self._process_locations(df)
            df = self._process_contacts(df)
            
            # Validate call types
            self._validate_call_types(df)
            
            return df
        except Exception as e:
            error_msg = f"Error transforming call data: {str(e)}"
            self.errors.append(error_msg)
            logger.error(error_msg)
            raise DataProcessingError(error_msg)

    def _parse_duration(self, duration: Any) -> Optional[int]:
        """Parse call duration to seconds"""
        if pd.isna(duration):
            return None
            
        try:
            # Handle different duration formats
            duration_str = str(duration).lower()
            total_seconds = 0
            
            # Parse hours
            hour_match = re.search(r'(\d+)\s*(?:hour|hr)s?', duration_str)
            if hour_match:
                total_seconds += int(hour_match.group(1)) * 3600
            
            # Parse minutes
            min_match = re.search(r'(\d+)\s*min', duration_str)
            if min_match:
                total_seconds += int(min_match.group(1)) * 60
            
            # Parse seconds
            sec_match = re.search(r'(\d+)\s*sec', duration_str)
            if sec_match:
                total_seconds += int(sec_match.group(1))
            
            return total_seconds if total_seconds > 0 else None
            
        except Exception as e:
            self.warnings.append(f"Error parsing duration: {duration}")
            return None

    def _validate_call_types(self, df: pd.DataFrame) -> None:
        """Validate call types"""
        valid_types = {'incoming', 'outgoing', 'missed', 'rejected', 'blocked'}
        invalid_types = set(df['call_type'].str.lower()) - valid_types
        if invalid_types:
            self.warnings.append(f"Invalid call types found: {invalid_types}")

class InstalledAppTransform(BaseTransform):
    """Enhanced installed app transformation"""
    
    required_columns = ['application_name', 'package_name', 'installed_date']
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        try:
            if not self.validate_dataframe(df, self.required_columns):
                raise DataProcessingError("Invalid installed app data structure")
            
            df = self.clean_columns(df)
            
            # Process installation dates
            df['installed_date'] = df['installed_date'].apply(self._parse_datetime)
            
            # Validate package names
            df = self._validate_package_names(df)
            
            return df
        except Exception as e:
            error_msg = f"Error transforming installed app data: {str(e)}"
            self.errors.append(error_msg)
            logger.error(error_msg)
            raise DataProcessingError(error_msg)

    def _validate_package_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate and clean package names"""
        package_pattern = re.compile(r'^[a-zA-Z][a-zA-Z0-9_]*(\.[a-zA-Z][a-zA-Z0-9_]*)+$')
        
        invalid_packages = []
        for idx, row in df.iterrows():
            if not package_pattern.match(row['package_name']):
                invalid_packages.append(row['package_name'])                # Set a generic package name
                df.at[idx, 'package_name'] = f"unknown.app.{hashlib.md5(row['application_name'].encode()).hexdigest()[:8]}"
        
        if invalid_packages:
            self.warnings.append(f"Invalid package names found and replaced: {invalid_packages}")
        
        return df

# --------- Data Processing Pipeline ---------
# --------- Data Processing Pipeline ---------
class DataProcessor:
    """Enhanced data processing pipeline"""
    
    # Updated table mappings to match exact file headers
    TABLE_MAPPING = {
        'calls': ['Call type', 'Time', 'From/To', 'Duration (Sec)', 'Location'],
        'sms_messages': ['SMS type', 'Time', 'From/To', 'Text', 'Location'],
        'chat_messages': ['Messenger', 'Time', 'Sender', 'Text'],
        'contacts': ['Name', 'Phone Number', 'Email Id', 'Last Contacted'],
        'installedapps': ['Application Name', 'Package Name', 'Installed Date'],
        'keylogs': ['Application', 'Time', 'Text']
    }

    # Mapping of file names to table names
    FILE_TO_TABLE = {
        'calls': 'calls',
        'sms': 'sms_messages',
        'chatMessages': 'chat_messages',
        'contacts': 'contacts',
        'installedApps': 'installedapps',
        'keylogs': 'keylogs'
    }

    def __init__(self):
        self.db_manager = None
        self.transformers = None
        self.processing_queue = queue.Queue()
        self.results = []
        self._stop_event = threading.Event()
        self.file_manager = FileManager(config)

    def initialize(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.transformers = self._initialize_transformers()

    def _initialize_transformers(self) -> Dict[str, BaseTransform]:
        return {
            'keylogs': KeylogTransform(self.db_manager),
            'sms_messages': SmsMessageTransform(self.db_manager),
            'chat_messages': ChatMessageTransform(self.db_manager),
            'calls': CallTransform(self.db_manager),
            'installedapps': InstalledAppTransform(self.db_manager)
        }

    def _load_file(self, file_path: Path) -> Optional[pd.DataFrame]:
        """Load data from file with enhanced metadata handling"""
        try:
            if file_path.suffix.lower() in ('.xls', '.xlsx'):
                logger.info(f"Loading Excel file: {file_path}")
                try:
                    # Read the first row to check for metadata
                    df_check = pd.read_excel(file_path, nrows=1)
                    
                    if df_check.iloc[0].str.contains('Tracking Smartphone').any():
                        # Skip the metadata row if present
                        df = pd.read_excel(file_path, skiprows=1)
                    else:
                        df = pd.read_excel(file_path)
                    
                    logger.debug(f"Loaded columns: {list(df.columns)}")
                    return df
                    
                except Exception as e:
                    logger.error(f"Error loading Excel file: {str(e)}")
                    return None
            else:
                logger.info(f"Loading CSV file: {file_path}")
                return pd.read_csv(file_path)
                
        except Exception as e:
            logger.error(f"Error loading file {file_path}: {str(e)}")
            return None

    def _identify_table(self, df: pd.DataFrame, file_path: Path) -> Optional[str]:
        """Identify target table based on file name and column structure"""
        try:
            # First try to identify by file name
            file_name = file_path.stem
            if file_name in self.FILE_TO_TABLE:
                table_name = self.FILE_TO_TABLE[file_name]
                expected_columns = self.TABLE_MAPPING[table_name]
                
                # Verify columns match
                df_columns = set(df.columns)
                expected_columns_set = set(expected_columns)
                
                missing_columns = expected_columns_set - df_columns
                if not missing_columns:
                    logger.info(f"Identified table '{table_name}' from filename")
                    return table_name
                else:
                    logger.warning(f"File {file_name} matches table {table_name} but is missing columns: {missing_columns}")
            
            # If file name matching fails, try column matching
            best_match = None
            highest_match_percentage = 0

            for table_name, expected_columns in self.TABLE_MAPPING.items():
                df_columns = set(df.columns)
                expected_columns_set = set(expected_columns)
                
                match_count = len(df_columns & expected_columns_set)
                match_percentage = (match_count / len(expected_columns_set)) * 100
                
                logger.debug(f"Table {table_name} match percentage: {match_percentage}%")
                
                if match_percentage > highest_match_percentage:
                    highest_match_percentage = match_percentage
                    best_match = table_name

            if highest_match_percentage >= 80:
                logger.info(f"Identified table: {best_match} with {highest_match_percentage}% match")
                return best_match
            
            logger.error(f"Could not identify table for file {file_path.name}. Best match was {best_match} with {highest_match_percentage}%")
            return None
            
        except Exception as e:
            logger.error(f"Error identifying table: {str(e)}")
            return None

    def _save_to_database(self, df: pd.DataFrame, table_name: str) -> int:
        rows_added = 0
        try:
            for i in range(0, len(df), config.BATCH_SIZE):
                batch = df.iloc[i:i + config.BATCH_SIZE]
                
                with self.db_manager.session_scope() as session:
                    batch.to_sql(
                        table_name,
                        self.db_manager._engine,
                        if_exists='append',
                        index=False,
                        method='multi'
                    )
                    rows_added += len(batch)
                    
            logger.info(f"Successfully added {rows_added} rows to {table_name}")
            return rows_added
            
        except SQLAlchemyError as e:
            logger.error(f"Database error while saving to {table_name}: {str(e)}")
            raise DataProcessingError(f"Database error: {str(e)}")

    def process_file(self, file_path: Path) -> ProcessingResult:
        """Process a single file with enhanced error handling"""
        start_time = time.time()
        result = ProcessingResult(
            status=ProcessingStatus.PENDING,
            file_path=file_path
        )
        
        try:
            # Load file
            df = self._load_file(file_path)
            if df is None:
                result.status = ProcessingStatus.FAILED
                result.errors.append("Failed to load file")
                self.file_manager.move_to_failed(file_path)
                return result
            
            logger.debug(f"Loaded DataFrame with columns: {list(df.columns)}")
            logger.debug(f"First few rows:\n{df.head()}")
            
            # Identify table
            table_name = self._identify_table(df, file_path)
            if table_name is None:
                result.status = ProcessingStatus.INVALID
                result.errors.append(f"Could not identify target table for file: {file_path.name}")
                self.file_manager.move_to_failed(file_path)
                return result
            
            # Transform data
            transformer = self.transformers.get(table_name)
            if transformer is None:
                result.status = ProcessingStatus.INVALID
                result.errors.append(f"No transformer found for table: {table_name}")
                self.file_manager.move_to_failed(file_path)
                return result
            
            result.status = ProcessingStatus.IN_PROGRESS
            df = transformer.transform(df)
            
            # Save to database
            rows_added = self._save_to_database(df, table_name)
            
            # Update result
            result.status = ProcessingStatus.COMPLETED
            result.rows_processed = rows_added
            result.table_name = table_name
            result.warnings.extend(transformer.warnings)
            
            # Move file to processed folder
            self.file_manager.move_to_processed(file_path)
            
        except Exception as e:
            result.status = ProcessingStatus.FAILED
            result.errors.append(str(e))
            logger.error(f"Error processing file {file_path}: {str(e)}")
            logger.error(traceback.format_exc())
            self.file_manager.move_to_failed(file_path)
        
        finally:
            result.processing_time = time.time() - start_time
            self.results.append(result)
            
        return result

# --------- File Management ---------
class FileManager:
    """Enhanced file management with error handling and monitoring"""
    
    def __init__(self, config: AppConfig):
        self.config = config
        self.processed_files: List[Path] = []
        self.failed_files: List[Path] = []
        self._file_locks: Dict[Path, threading.Lock] = {}

    def get_pending_files(self) -> List[Path]:
        """Get list of files pending processing"""
        try:
            return [
                path for path in self.config.UPLOAD_FOLDER.glob('*')
                if path.suffix.lower() in self.config.ALLOWED_EXTENSIONS
                and not path.name.startswith('.')
            ]
        except Exception as e:
            logger.error(f"Error getting pending files: {str(e)}")
            return []

    def move_file(self, file_path: Path, destination: Path) -> Optional[Path]:
        """Move file with error handling and locking"""
        if not file_path.exists():
            logger.error(f"File not found: {file_path}")
            return None

        # Get or create file lock
        lock = self._file_locks.setdefault(file_path, threading.Lock())
        
        with lock:
            try:
                # Create timestamp suffix
                timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
                new_name = f"{file_path.stem}_{timestamp}{file_path.suffix}"
                destination_path = destination / new_name
                
                # Ensure destination directory exists
                destination.mkdir(parents=True, exist_ok=True)
                
                # Move file
                shutil.move(str(file_path), str(destination_path))
                
                logger.info(f"Moved file: {file_path} -> {destination_path}")
                return destination_path
                
            except Exception as e:
                logger.error(f"Error moving file {file_path}: {str(e)}")
                return None
            finally:
                # Clean up lock
                self._file_locks.pop(file_path, None)

    def move_to_processed(self, file_path: Path) -> Optional[Path]:
        """Move file to processed folder"""
        result = self.move_file(file_path, self.config.PROCESSED_FOLDER)
        if result:
            self.processed_files.append(result)
        return result

    def move_to_failed(self, file_path: Path) -> Optional[Path]:
        """Move file to failed folder"""
        result = self.move_file(file_path, self.config.FAILED_FOLDER)
        if result:
            self.failed_files.append(result)
        return result

    def cleanup_old_files(self, max_age_days: int = 30):
        """Clean up old processed and failed files"""
        try:
            cutoff_time = datetime.utcnow() - timedelta(days=max_age_days)
            
            for folder in [self.config.PROCESSED_FOLDER, self.config.FAILED_FOLDER]:
                for file_path in folder.glob('*'):
                    try:
                        if file_path.stat().st_mtime < cutoff_time.timestamp():
                            # Archive file
                            archive_path = self.config.ARCHIVE_FOLDER / file_path.name
                            if self.config.ENABLE_COMPRESSION:
                                self._compress_and_archive(file_path, archive_path)
                            else:
                                shutil.move(str(file_path), str(archive_path))
                            logger.info(f"Archived old file: {file_path}")
                    except Exception as e:
                        logger.error(f"Error processing old file {file_path}: {str(e)}")
                        
        except Exception as e:
            logger.error(f"Error during file cleanup: {str(e)}")

    def _compress_and_archive(self, source: Path, destination: Path):
        """Compress and archive a file"""
        import gzip
        
        # Ensure archive directory exists
        destination.parent.mkdir(parents=True, exist_ok=True)
        
        # Compress and move file
        with open(source, 'rb') as f_in:
            with gzip.open(str(destination) + '.gz', 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        
        # Remove original file
        source.unlink()

# --------- Processing Monitor ---------
class ProcessingMonitor:
    """Monitor and report processing status"""
    
    def __init__(self):
        self.start_time = datetime.utcnow()
        self.processed_count = 0
        self.failed_count = 0
        self.total_rows_processed = 0
        self.errors: List[str] = []
        self.warnings: List[str] = []
        self._lock = threading.Lock()

    def update_stats(self, result: ProcessingResult):
        """Update processing statistics"""
        with self._lock:
            if result.status == ProcessingStatus.COMPLETED:
                self.processed_count += 1
                self.total_rows_processed += result.rows_processed
            elif result.status == ProcessingStatus.FAILED:
                self.failed_count += 1
            
            self.errors.extend(result.errors)
            self.warnings.extend(result.warnings)

    def generate_report(self) -> Dict[str, Any]:
        """Generate processing report"""
        with self._lock:
            end_time = datetime.utcnow()
            processing_time = (end_time - self.start_time).total_seconds()
            
            return {
                'start_time': self.start_time.isoformat(),
                'end_time': end_time.isoformat(),
                'processing_time_seconds': processing_time,
                'files_processed': self.processed_count,
                'files_failed': self.failed_count,
                'total_rows_processed': self.total_rows_processed,
                'rows_per_second': self.total_rows_processed / processing_time if processing_time > 0 else 0,
                'error_count': len(self.errors),
                'warning_count': len(self.warnings),
                'errors': self.errors[-10:],  # Last 10 errors
                'warnings': self.warnings[-10:]  # Last 10 warnings
            }

    def save_report(self, report: Dict[str, Any]):
        """Save processing report to file"""
        try:
            timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            report_path = config.MONITORING_DIR / f"processing_report_{timestamp}.json"
            
            with open(report_path, 'w') as f:
                json.dump(report, f, indent=2)
            
            logger.info(f"Processing report saved to {report_path}")
            
        except Exception as e:
            logger.error(f"Error saving processing report: {str(e)}")

# --------- Main Application ---------
class DataProcessingApplication:
    """Main application class orchestrating the data processing pipeline"""
    
    def __init__(self):
        self.config = config
        self.db_manager = DatabaseManager(self.config.DB_PATH)
        self.file_manager = FileManager(self.config)
        self.processor = DataProcessor()
        self.processor.initialize(self.db_manager)
        self.monitor = ProcessingMonitor()
        self._stop_event = threading.Event()

    def run(self):
        """Run the application"""
        logger.info("Starting data processing application")
        
        try:
            # Initialize database
            self.db_manager.initialize_database()
            
            # Process files
            if self.config.ENABLE_THREADING:
                self._process_files_threaded()
            else:
                self._process_files_sequential()
            
            # Generate and save report
            report = self.monitor.generate_report()
            self.monitor.save_report(report)
            
            # Backup database
            if self.config.ENABLE_BACKUPS:
                self.db_manager.backup_database()
            
            # Cleanup old files
            self.file_manager.cleanup_old_files()
            
        except Exception as e:
            logger.error(f"Application error: {str(e)}")
            raise
        finally:
            self._cleanup()

    def _process_files_threaded(self):
        """Process files using thread pool"""
        pending_files = self.file_manager.get_pending_files()
        
        with ThreadPoolExecutor(max_workers=self.config.MAX_WORKERS) as executor:
            future_to_file = {
                executor.submit(self._process_file_safe, file_path): file_path
                for file_path in pending_files
            }
            
            for future in as_completed(future_to_file):
                file_path = future_to_file[future]
                try:
                    result = future.result()
                    self.monitor.update_stats(result)
                except Exception as e:
                    logger.error(f"Error processing file {file_path}: {str(e)}")

    def _process_files_sequential(self):
        """Process files sequentially"""
        for file_path in self.file_manager.get_pending_files():
            result = self._process_file_safe(file_path)
            self.monitor.update_stats(result)

    def _process_file_safe(self, file_path: Path) -> ProcessingResult:
        """Safely process a single file with retries"""
        for attempt in range(self.config.MAX_RETRIES):
            try:
                return self.processor.process_file(file_path)
            except Exception as e:
                if attempt < self.config.MAX_RETRIES - 1:
                    logger.warning(f"Retry {attempt + 1} for file {file_path}: {str(e)}")
                    time.sleep(self.config.RETRY_DELAY)
                else:
                    logger.error(f"Failed to process file {file_path} after {self.config.MAX_RETRIES} attempts")
                    return ProcessingResult(
                        status=ProcessingStatus.FAILED,
                        file_path=file_path,
                        errors=[str(e)]
                    )

    def _cleanup(self):
        """Perform cleanup operations"""
        logger.info("Performing cleanup operations")
        try:
            # Clean up temporary files
            shutil.rmtree(self.config.TEMP_DIR, ignore_errors=True)
            
            # Close database connections
            self.db_manager._engine.dispose()
            
        except Exception as e:
            logger.error(f"Error during cleanup: {str(e)}")

def main():
    """Application entry point"""
    try:
        # Set up signal handlers
        def signal_handler(signum, frame):
            logger.info(f"Received signal {signum}")
            app._stop_event.set()
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        # Run application
        app = DataProcessingApplication()
        app.run()
        
    except Exception as e:
        logger.error(f"Application failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
