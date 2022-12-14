from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, create_engine

Base = declarative_base()


class User(Base):
    __tablename__ = 'users'
    id = Column(Integer, primary_key=True)
    name = Column(String)


class Post(Base):
    __tablename__ = 'posts'
    id = Column(Integer, primary_key=True)
    title = Column(String)
    body = Column(String)


engine = create_engine('sqlite:///my_database.db')
engine.create_all([User, Post])

from sqlalchemy import Table, Column, MetaData


def create_table(table_name, columns):
    # Create a MetaData object
    metadata = MetaData()

    # Define the columns of the table
    columns = [
        Column(col_name, col_type)
        for col_name, col_type in columns.items()
    ]

    # Create the table using the columns and metadata
    table = Table(table_name, metadata, *columns)

    return table


from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String


def create_table(table_name, columns):
    # Create a declarative base class
    Base = declarative_base()

    # Define a class that represents the table
    class Table(Base):
        __tablename__ = table_name

        # Define the columns of the table
        columns = [
            Column(col_name, col_type)
            for col_name, col_type in columns.items()
        ]

    return Table
