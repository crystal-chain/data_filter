from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os 
load_dotenv()

def get_engine():
    """
    Crée et retourne un moteur SQLAlchemy pour PostgreSQL.
    """
    db_url = (
        f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
        f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT', 5432)}/{os.getenv('DB_NAME')}"
    )
    engine = create_engine(db_url)
    return engine
