import os
import sys
from sqlalchemy import Column, ForeignKey, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy import create_engine

Base = declarative_base()

class Device(Base):
    __tablename__ = 'device'
    id = Column(Integer, primary_key=True)
    ca = Column(String(250), nullable=False)
    unit = Column(String(250), nullable=False)
    scp = Column(String(250), nullable=False)

class Turnstile(Base):
    __tablename__ = 'turnstile'
    id = Column(Integer, primary_key=True)
    device_id = Column(Integer, ForeignKey('device.id'))
    timestamp = Column(Integer, nullable=False)
    description = Column(String(250))
    entry = Column(Integer, nullable=False)
    exit = Column(Integer, nullable=False)

def create_all_table(engine_string='sqlite:///test_data.db'):
    # Create all tables in the engine. Equivalent to "Creat Table" in raw SQL.
    engine = create_engine(engine_string)
    Base.metadata.create_all(engine)
    return engine