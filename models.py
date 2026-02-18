from sqlalchemy import Column, Integer, String
from database import Base

class Cabinet(Base):
    __tablename__ = "cabinets"

    id = Column(Integer, primary_key=True, index=True)
    cabinet_code = Column(String)
    bom_line_1 = Column(String)
    bom_line_2 = Column(String)
    bom_line_3 = Column(String)


class ColorCode(Base):
    __tablename__ = "colorcode"

    id = Column(Integer, primary_key=True, index=True)
    colour_name = Column(String)
    colour_code = Column(String)
