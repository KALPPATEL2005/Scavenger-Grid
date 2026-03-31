from fastapi import FastAPI, HTTPException
from typing import Dict, List
import time
import uuid
import socket
import contextlib

from zeroconf.asyncio import AsyncZeroconf
from zeroconf import ServiceInfo
from shared.protocol import WorkerRegistration, TaskChunk, TaskResult, TaskType, TaskStatus

from master.vector_store import VectorVault
vault = VectorVault()

from pydantic import BaseModel

class IngestRequest(BaseModel):
    documents: List[str]

aio_zc = None

def get_local_ip():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"

@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    global aio_zc
    ip = get_local_ip()
    print(f"\n[*] Master Local LAN IP: {ip}")
    
    info = ServiceInfo(
        "_scavenger._tcp.local.", "MasterNode._scavenger._tcp.local.",
        addresses=[socket.inet_aton(ip)], port=8000,
        properties={'desc': 'Scavenger Grid Orchestrator'}, server="masternode.local."
    )
    
    aio_zc = AsyncZeroconf()
    await aio_zc.async_register_service(info)
    print("[+] mDNS Broadcast started. Workers can now auto-discover the Master!\n")
    yield 
    print("[-] Stopping mDNS Broadcast...")
    await aio_zc.async_unregister_service(info)
    await aio_zc.async_close()

app = FastAPI(title="Scavenger Grid Orchestrator", lifespan=lifespan)

# --- IN-MEMORY STATE ---
active_workers: Dict[str, dict] = {}
task_queue: List[TaskChunk] = []
active_tasks: Dict[str, TaskChunk] = {}  # Tracks tasks currently being processed
completed_tasks: List[TaskResult] = []

# # --- NEW: REAL DATA ---
# sample_documents = [
#     "The Scavenger Grid was invented to utilize idle computer power.",
#     "The company's Q3 revenue increased by 15% due to new AI initiatives.",
#     "According to the contract, the lease expires on December 31, 2028.",
#     "To reset the server, hold the power button for 10 seconds.",
#     "The CEO of the corporation is named Sarah Jenkins."
# ]

@app.post("/register", response_model=dict)
async def register_worker(worker: WorkerRegistration):
    active_workers[worker.worker_id] = {"info": worker, "last_seen": time.time(), "status": "idle"}
    print(f"[+] Worker {worker.worker_id} registered. RAM: {worker.hardware.ram_available_gb}GB")
    return {"message": "Registration successful", "master_status": "active"}

@app.post("/ingest")
async def ingest_documents(req: IngestRequest):
    """The Shredder hits this endpoint to dump chunks into the queue."""
    count = 0
    for text in req.documents:
        if text.strip():
            new_task = TaskChunk(
                task_id=str(uuid.uuid4()),
                task_type=TaskType.EMBEDDING,
                payload=text.strip(), 
                is_obfuscated=False
            )
            task_queue.append(new_task)
            count += 1
    print(f"[+] Ingested {count} new chunks into the task queue.")
    return {"message": f"Added {count} tasks", "queue_size": len(task_queue)}

@app.get("/tasks/{worker_id}", response_model=TaskChunk)
async def get_task(worker_id: str):
    if worker_id not in active_workers:
        raise HTTPException(status_code=404, detail="Worker not registered")
    
    if not task_queue:
        raise HTTPException(status_code=404, detail="No tasks available")

    # Move from queue to active
    task = task_queue.pop(0)
    active_tasks[task.task_id] = task
    active_workers[worker_id]["status"] = "working"
    
    print(f"[*] Assigned Task {task.task_id[:8]}... to Worker {worker_id}")
    return task

@app.post("/results", response_model=dict)
async def submit_result(result: TaskResult):
    # Retrieve the original task data
    original_task = active_tasks.pop(result.task_id, None)

    if result.status == TaskStatus.SUSPENDED:
        print(f"[!] Worker {result.worker_id} GHOST EXITED. Re-queuing Task {result.task_id[:8]}...")
        if original_task:
            task_queue.insert(0, original_task) 
    else:
        print(f"[✓] Task {result.task_id[:8]}... completed by {result.worker_id}")
        completed_tasks.append(result)
        
        # This is where the magic happens!
        if result.result_data and original_task:
            vault.insert_chunk(
                task_id=result.task_id,
                content=original_task.payload, 
                vector=result.result_data      
            )
            
    if result.worker_id in active_workers:
         active_workers[result.worker_id]["status"] = "idle"
         
    return {"message": "Result acknowledged"}

@app.get("/health")
async def health_check():
    return {
        "active_workers": len(active_workers), 
        "queued_tasks": len(task_queue),
        "in_progress_tasks": len(active_tasks),
        "completed_tasks": len(completed_tasks)
    }