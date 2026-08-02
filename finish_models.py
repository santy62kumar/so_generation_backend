from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Integer,
    String,
    UniqueConstraint,
)
from sqlalchemy.sql import func

from database import Base


class Finish(Base):
    __tablename__ = "finishes"

    __table_args__ = (
        UniqueConstraint(
            "category",
            "slug",
            name="uq_finishes_category_slug",
        ),
    )

    id = Column(
        Integer,
        primary_key=True,
        index=True,
    )

    category = Column(
        String(50),
        nullable=False,
        index=True,
    )

    slug = Column(
        String(150),
        nullable=False,
        index=True,
    )

    name = Column(
        String(200),
        nullable=False,
    )

    master_key = Column(
        String(500),
        nullable=False,
    )

    thumb_key = Column(
        String(500),
        nullable=False,
    )

    master_content_type = Column(
        String(100),
        nullable=False,
    )

    thumb_content_type = Column(
        String(100),
        nullable=False,
        default="image/webp",
    )

    is_active = Column(
        Boolean,
        nullable=False,
        default=True,
    )

    # created_at = Column(
    #     DateTime(timezone=True),
    #     nullable=False,
    #     server_default=func.now(),
    # )

    # updated_at = Column(
    #     DateTime(timezone=True),
    #     nullable=False,
    #     server_default=func.now(),
    #     onupdate=func.now(),
    # )