import os
import sys
import pandas as pd
from sqlalchemy import Column, ForeignKey, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy import create_engine
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.exc import IntegrityError

Base = declarative_base()

class Station(Base):
    __tablename__ = 'station'
    id = Column(Integer, primary_key=True)
    ca = Column(String(250), nullable=False)
    unit = Column(String(250), nullable=False)

class Device(Base):
    __tablename__ = 'device'
    id = Column(Integer, primary_key=True)
    station_id = Column(Integer, ForeignKey('station.id'))
    #ca = Column(String(250), nullable=False)
    #unit = Column(String(250), nullable=False)
    scp = Column(String(250), nullable=False)

class Turnstile(Base):
    __tablename__ = 'turnstile'
    id = Column(Integer, primary_key=True)
    device_id = Column(Integer, ForeignKey('device.id'))
    timestamp = Column(Integer, nullable=False)
    description = Column(String(250))
    entry = Column(Integer, nullable=False)
    exit = Column(Integer, nullable=False)

class Previous(Base):
    __tablename__ = 'previous'
    id = Column(Integer, primary_key=True)
    device_id = Column(Integer, ForeignKey('device.id'))
    timestamp = Column(Integer, nullable=False)
    description = Column(String(250))
    entry = Column(Integer, nullable=False)
    exit = Column(Integer, nullable=False)
    file_date = Column(Integer, nullable=False)

def create_all_table(engine_string='sqlite:///test_data.db'):
    # Create all tables in the engine. Equivalent to "Creat Table" in raw SQL.
    engine = create_engine(engine_string)
    Base.metadata.create_all(engine)
    return engine


def get_one_or_create(session,
                      model,
                      create_method='',
                      create_method_kwargs=None,
                      **kwargs):
    """https://stackoverflow.com/questions/2546207/does-sqlalchemy-have-an-equivalent-of-djangos-get-or-create
        return (instantce, bool) if a new instance is created or returned
    """
    try:
        return session.query(model).filter_by(**kwargs).one(), False
    except NoResultFound:
        kwargs.update(create_method_kwargs or {})
        created = getattr(model, create_method, model)(**kwargs)
        try:
            session.add(created)
            session.flush()
            return created, True
        except IntegrityError:
            session.rollback()
            return session.query(model).filter_by(**kwargs).one(), False


def data_frame(query, columns):
    """http://danielweitzenfeld.github.io/passtheroc/blog/2014/10/12/datasci-sqlalchemy/
    Takes a sqlalchemy query and a list of columns, returns a dataframe.
    """
    def make_row(x):
        return dict([(c, getattr(x, c)) for c in columns])
    return pd.DataFrame([make_row(x) for x in query])