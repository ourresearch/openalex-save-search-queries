from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, Integer, Text, DateTime, VARCHAR

Base = declarative_base()


class SearchQuery(Base):
    __tablename__ = "search_queries"

    id = Column(Integer, primary_key=True)
    type = Column(VARCHAR(255))
    endpoint = Column(VARCHAR(255))
    query = Column(Text, nullable=False)
    count = Column(Integer, nullable=False)
    ip_address = Column(VARCHAR(255))
    timestamp = Column(DateTime, nullable=False)
