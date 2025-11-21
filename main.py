from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel, EmailStr
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from typing import Optional
import asyncio

app = FastAPI()

# Database connection for master (port 5432)
MASTER_DATABASE_URL = "postgresql://postgres:root@localhost:5432/myapp"
master_engine = create_engine(MASTER_DATABASE_URL)
MasterSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=master_engine)

# Database connection for replica (port 5433)
REPLICA_DATABASE_URL = "postgresql://postgres:root@localhost:5433/myapp"
replica_engine = create_engine(REPLICA_DATABASE_URL)
ReplicaSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=replica_engine)

Base = declarative_base()

# SQLAlchemy model for details table
class Details(Base):
    __tablename__ = "details"
    
    id = Column(Integer, primary_key=True, index=True)
    email = Column(String(255), nullable=False)
    name = Column(String(100))

# Pydantic models
class UserCreate(BaseModel):
    user_id: int
    username: str

class DetailsCreate(BaseModel):
    email: str
    name: Optional[str] = None

@app.post("/create_user/")
async def create_user(user_data: UserCreate):
    user_id = user_data.user_id
    username = user_data.username
    return {
        "msg": "we got data succesfully",
        "user_id": user_id,
        "username": username,
    }

def sync_to_replica_sync(email: str, name: Optional[str]):
    """Synchronous function to sync data to replica database"""
    db = ReplicaSessionLocal()
    try:
        new_detail = Details(
            email=email,
            name=name
        )
        db.add(new_detail)
        db.commit()
        db.refresh(new_detail)
        print(f"Data synced to replica: id={new_detail.id}, email={email}")
    except Exception as e:
        db.rollback()
        print(f"Error syncing to replica: {str(e)}")
    finally:
        db.close()

async def sync_to_replica(email: str, name: Optional[str], delay_seconds: int = 5):
    """Background task to sync data to replica database after a delay"""
    await asyncio.sleep(delay_seconds)
    # Run the synchronous database operation in a thread pool
    await asyncio.to_thread(sync_to_replica_sync, email, name)

@app.post("/data")
async def add_data(details_data: DetailsCreate, background_tasks: BackgroundTasks):
    db = MasterSessionLocal()
    try:
        # Create new details record in master database
        new_detail = Details(
            email=details_data.email,
            name=details_data.name
        )
        db.add(new_detail)
        db.commit()
        db.refresh(new_detail)
        
        # Schedule background task to sync to replica after delay
        background_tasks.add_task(sync_to_replica, details_data.email, details_data.name)
        
        return {
            "msg": "Data added successfully to master database",
            "id": new_detail.id,
            "email": new_detail.email,
            "name": new_detail.name
        }
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error adding data: {str(e)}")
    finally:
        db.close()

@app.get("/data/{id}")
async def get_data(id: int):
    """Get data from replica database by ID"""
    db = ReplicaSessionLocal()
    try:
        # Query replica database for the record with the given ID
        detail = db.query(Details).filter(Details.id == id).first()
        
        if detail is None:
            raise HTTPException(status_code=404, detail=f"Record with id {id} not found in replica database")
        
        return {
            "status": "success",
            "message": "Data retrieved from replica database",
            "data": {
                "id": detail.id,
                "email": detail.email,
                "name": detail.name
            }
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching data: {str(e)}")
    finally:
        db.close()

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8000)