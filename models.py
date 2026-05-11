from sqlalchemy import Column, Integer, String
from database import Base

class Cabinet(Base):
    __tablename__ = "cabinets"

    id = Column(Integer, primary_key=True, index=True)
    cabinet_code = Column(String)
    description = Column(String(100))
    bom_line_1 = Column(String)
    bom_line_2 = Column(String)
    bom_line_3 = Column(String)
    bom_line_4 = Column(String)   # ✅ new column added
    bom_line_5 = Column(String)
    bom_line_6 = Column(String)
    bom_line_7 = Column(String)
    bom_line_8 = Column(String)
    bom_line_9 = Column(String)
    bom_line_10 = Column(String)
    bom_line_11 = Column(String)
    bom_line_12 = Column(String)
    bom_line_13 = Column(String)


class ColorCode(Base):
    __tablename__ = "colorcode"

    id = Column(Integer, primary_key=True, index=True)
    colour_name = Column(String)
    colour_code = Column(String)


class CodeRaw(Base):
    __tablename__ = "code_raw"

    infurnia_code = Column(String(50), primary_key=True)
    odoo_code     = Column(String(50))

