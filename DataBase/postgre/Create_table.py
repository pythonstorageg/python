from sqlalchemy import (
    create_engine, Column, Integer, String, Boolean, Time, text
)
from sqlalchemy.sql import func
from sqlalchemy.orm import declarative_base

connection_string = 'postgresql+psycopg2://gopi:gopi123@127.0.0.1:5435/database'
engine = create_engine(connection_string)

# Create the Table
Base = declarative_base()
class TestTable(Base):
    __tablename__ = 'test_table'
    __table_args__ = {'schema': 'public'}

    id = Column(Integer, primary_key=True, autoincrement=True)
    title = Column(String, nullable=False, server_default=text("'Untitled'"))
    content = Column(String, nullable=False, server_default=text("''"))
    is_published = Column(Boolean, nullable=False, server_default=text('true'))
    Timestamp = Column(Time(timezone=True), nullable=False, server_default=func.now())
Base.metadata.create_all(engine)

# Adding column to the existing table
