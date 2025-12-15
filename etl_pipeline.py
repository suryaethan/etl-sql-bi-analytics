import pandas as pd
import numpy as np
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database configuration
DATABASE_URL = "sqlite:///analytics.db"
engine = create_engine(DATABASE_URL, echo=False)
Base = declarative_base()
Session = sessionmaker(bind=engine)

# Define ORM model for sales data
class SalesData(Base):
    __tablename__ = 'sales_data'
    id = Column(Integer, primary_key=True)
    product = Column(String(100))
    region = Column(String(50))
    amount = Column(Float)
    quantity = Column(Integer)
    date = Column(DateTime, default=datetime.now)

# Create tables
Base.metadata.create_all(engine)

class ETLPipeline:
    """ETL Pipeline for data extraction, transformation, and loading"""
    
    def __init__(self):
        self.session = Session()
        
    def extract_data(self, csv_file):
        """Extract data from CSV file"""
        try:
            df = pd.read_csv(csv_file)
            logger.info(f"Extracted {len(df)} records from {csv_file}")
            return df
        except FileNotFoundError:
            logger.error(f"File not found: {csv_file}")
            return None
    
    def transform_data(self, df):
        """Transform and validate data"""
        try:
            df = df.drop_duplicates()
            df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
            df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce')
            df.fillna(0, inplace=True)
            df['total_value'] = df['amount'] * df['quantity']
            logger.info(f"Transformed {len(df)} records")
            return df
        except Exception as e:
            logger.error(f"Error: {e}")
            return None
