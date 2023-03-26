from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Date, String, Float, Integer

Base = declarative_base()

class InterestRate(Base):
    __tablename__ = "interest_rate"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    date = Column(Date)
    value = Column(Float)
    obs_status = Column(String)
    obs_conf = Column(String)
