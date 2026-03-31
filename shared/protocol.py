from pydantic import BaseModel
from enum import Enum
from typing import Optional, List

class TaskStatus(Enum):
    COMPLETED = "completed"
    SUSPENDED = "suspended"
    FAILED = "failed"

class TaskType(Enum):
    OCR = "ocr"
    EMBEDDING = "embedding"

class HardwareProfile(BaseModel):
    cpu_cores: int
    ram_total_gb: float
    ram_available_gb: float
    os_name: str
    is_idle: bool

class WorkerRegistration(BaseModel):
    worker_id: str
    ip_address: str
    port: int
    hardware: HardwareProfile

class TaskChunk(BaseModel):
    task_id: str
    task_type: TaskType
    payload: str
    is_obfuscated: bool

class TaskResult(BaseModel):
    task_id: str
    worker_id: str
    status: TaskStatus
    result_text: Optional[str] = None
    # NEW: Tell FastAPI to accept the vector array!
    result_data: Optional[List[float]] = None 
    processing_time_sec: float