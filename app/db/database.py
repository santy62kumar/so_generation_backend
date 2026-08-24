"""Database engine and session setup."""

from sqlalchemy import create_engine
from sqlalchemy.engine import URL, make_url
from sqlalchemy.orm import sessionmaker, declarative_base
from dotenv import load_dotenv
import os

load_dotenv()


def _build_url():
    """DATABASE_URL / DB_URL wins; otherwise assemble from the discrete DB_* vars."""
    raw = os.getenv("DATABASE_URL") or os.getenv("DB_URL")
    if raw:
        url = make_url(raw)
        # psycopg2 is the installed driver; a bare "postgresql://" URL resolves to
        # whatever DBAPI is default, so pin it.
        if url.drivername == "postgresql":
            url = url.set(drivername="postgresql+psycopg2")
        if not url.database:
            raise RuntimeError("DATABASE_URL must include a database name.")
        return url

    # Password stays optional — local peer/trust auth has none.
    required = ("DB_USER", "DB_HOST", "DB_PORT", "DB_NAME")
    config = {name: os.getenv(name) for name in required}
    missing = [name for name, value in config.items() if not value]
    if missing:
        raise RuntimeError(
            "Set DATABASE_URL, or all of: " + ", ".join(required) + f" (missing: {', '.join(missing)})"
        )
    try:
        db_port = int(config["DB_PORT"])
    except ValueError as exc:
        raise RuntimeError("DB_PORT must be a whole number.") from exc

    return URL.create(
        "postgresql+psycopg2",
        username=config["DB_USER"],
        password=os.getenv("DB_PASSWORD") or None,
        host=config["DB_HOST"],
        port=db_port,
        database=config["DB_NAME"],
    )


DATABASE_URL = _build_url()

try:
    _pool_size = int(os.getenv("DB_POOL_SIZE", "10"))
    _max_overflow = int(os.getenv("DB_MAX_OVERFLOW", "20"))
except ValueError as exc:
    raise RuntimeError("DB_POOL_SIZE and DB_MAX_OVERFLOW must be whole numbers.") from exc

_engine_options = {
    "echo": False,
    "pool_pre_ping": True,
}
if DATABASE_URL.drivername.startswith("postgresql"):
    _engine_options.update(
        pool_size=_pool_size,
        max_overflow=_max_overflow,
        pool_recycle=1800,
        # Statement timeout keeps one runaway query from pinning a pooled connection.
        connect_args={
            "options": f"-c statement_timeout={os.getenv('DB_STATEMENT_TIMEOUT_MS', '30000')}"
        },
    )

engine = create_engine(DATABASE_URL, **_engine_options)

SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine
)

Base = declarative_base()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
