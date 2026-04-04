from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from typing import Dict, List
import time
import uuid
import socket
import contextlib
import json
import httpx
import torch
import asyncio 
from sentence_transformers import SentenceTransformer

from zeroconf.asyncio import AsyncZeroconf
from zeroconf import ServiceInfo
from shared.protocol import WorkerRegistration, TaskChunk, TaskResult, TaskType, TaskStatus

from master.vector_store import VectorVault
vault = VectorVault()

print("[*] Loading Dashboard AI Search Model...")
search_model = SentenceTransformer('all-MiniLM-L6-v2')

class ChatRequest(BaseModel):
    query: str

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

# --- IN-MEMORY STATE ---
active_workers: Dict[str, dict] = {}
task_queue: List[TaskChunk] = []
active_tasks: Dict[str, dict] = {}  
completed_tasks: List[TaskResult] = []
active_websockets: Dict[str, WebSocket] = {} # NEW: Tracks open tunnels!

# --- LOOP 1: THE WATCHDOG ---
TASK_TIMEOUT_SECONDS = 30
WORKER_TIMEOUT_SECONDS = 60  

async def watchdog_loop():
    print("[+] Watchdog initialized. Monitoring for stuck tasks and dead workers...")
    while True:
        current_time = time.time()
        stale_tasks = []
        dead_workers = []
        
        # 1. Identify stuck tasks
        for task_id, task_data in list(active_tasks.items()):
            if current_time - task_data.get("assigned_at", current_time) > TASK_TIMEOUT_SECONDS:
                stale_tasks.append(task_id)
                
        # 2. Rescue tasks
        for task_id in stale_tasks:
            stuck = active_tasks.pop(task_id, None)
            if stuck:
                print(f"\n[!] Task {task_id[:8]} timed out on {stuck['worker_id']}. Re-queuing...")
                task_queue.append(stuck["chunk"]) 
                if stuck['worker_id'] in active_workers:
                    active_workers[stuck['worker_id']]["status"] = "timeout"

        # 3. Clean up dead workers (Zombies)
        for worker_id, worker_data in list(active_workers.items()):
            if current_time - worker_data["last_seen"] > WORKER_TIMEOUT_SECONDS:
                dead_workers.append(worker_id)
                
        for worker_id in dead_workers:
            print(f"[-] Worker {worker_id} went silent. Removing from active roster.")
            del active_workers[worker_id]
            if worker_id in active_websockets:
                del active_websockets[worker_id]
            
        await asyncio.sleep(5) 

# # --- LOOP 2: THE WEBSOCKET DISPATCHER ---
# async def task_dispatcher_loop():
#     print("[+] WebSocket Dispatcher initialized. Ready to push tasks...")
#     while True:
#         if task_queue:
#             # Find an idle worker with an active websocket
#             for worker_id, w_data in list(active_workers.items()):
#                 if w_data["status"] == "idle" and worker_id in active_websockets:
#                     task = task_queue.pop(0)
                    
#                     # Update State
#                     active_tasks[task.task_id] = {
#                         "chunk": task,
#                         "worker_id": worker_id,
#                         "assigned_at": time.time()
#                     }
#                     active_workers[worker_id]["status"] = "working"
#                     active_workers[worker_id]["last_seen"] = time.time()

#                     # PUSH DOWN THE TUNNEL
#                     ws = active_websockets[worker_id]
#                     try:
#                         # CRITICAL FIX: mode='json' safely converts Enums to strings
#                         await ws.send_json(task.model_dump(mode='json'))
#                         print(f"[*] PUSHED Task {task.task_id[:8]} down tunnel to {worker_id}")
#                     except Exception as e:
#                         # Print exactly why it failed
#                         print(f"[!] Failed to push to {worker_id}: {repr(e)}. Re-queuing.")
#                         task_queue.insert(0, task)
#                         active_workers[worker_id]["status"] = "idle"
                    
#                     break # Give the loop a chance to breathe, then process next task
        
#         await asyncio.sleep(0.05) # Check the queue 20 times a second for near-instant latency


# --- LOOP 2: THE WEBSOCKET DISPATCHER ---
async def task_dispatcher_loop():
    print("[+] WebSocket Dispatcher initialized. Ready to push tasks...")
    BATCH_SIZE = 10  # Tell the Master to grab up to 10 tasks at once
    
    while True:
        if task_queue:
            for worker_id, w_data in list(active_workers.items()):
                if w_data["status"] == "idle" and worker_id in active_websockets:
                    # 1. Grab a batch of tasks from the queue
                    batch = []
                    while task_queue and len(batch) < BATCH_SIZE:
                        batch.append(task_queue.pop(0))
                    
                    # 2. Update State for ALL tasks in the batch
                    for task in batch:
                        active_tasks[task.task_id] = {
                            "chunk": task,
                            "worker_id": worker_id,
                            "assigned_at": time.time()
                        }
                        
                    active_workers[worker_id]["status"] = "working"
                    active_workers[worker_id]["last_seen"] = time.time()

                    # 3. PUSH THE ENTIRE BATCH DOWN THE TUNNEL
                    ws = active_websockets[worker_id]
                    try:
                        # Send the list of tasks as a single JSON array
                        await ws.send_json([t.model_dump(mode='json') for t in batch])
                        print(f"[*] PUSHED Batch of {len(batch)} tasks down tunnel to {worker_id}")
                    except Exception as e:
                        print(f"[!] Failed to push to {worker_id}: {repr(e)}. Re-queuing batch.")
                        for task in reversed(batch):
                            task_queue.insert(0, task)
                        active_workers[worker_id]["status"] = "idle"
                    
                    break # Give the loop a chance to breathe
        
        await asyncio.sleep(0.05)

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
    
    # Start BOTH background engines
    asyncio.create_task(watchdog_loop())
    asyncio.create_task(task_dispatcher_loop())
    
    yield 
    
    print("[-] Stopping mDNS Broadcast...")
    await aio_zc.async_unregister_service(info)
    await aio_zc.async_close()

app = FastAPI(title="Scavenger Grid Orchestrator", lifespan=lifespan)

# --- WEBSOCKET ENDPOINT ---
@app.websocket("/ws/{worker_id}")
async def websocket_endpoint(websocket: WebSocket, worker_id: str):
    await websocket.accept()
    active_websockets[worker_id] = websocket
    print(f"[+] WebSocket Tunnel established for {worker_id}")
    try:
        while True:
            # Sit and wait silently to keep connection open
            await websocket.receive_text()
    except WebSocketDisconnect:
        print(f"[-] WebSocket Tunnel closed for {worker_id}")
        if worker_id in active_websockets:
            del active_websockets[worker_id]

# --- HTTP ENDPOINTS ---
@app.post("/register", response_model=dict)
async def register_worker(worker: WorkerRegistration):
    active_workers[worker.worker_id] = {"info": worker, "last_seen": time.time(), "status": "idle"}
    print(f"[+] Worker {worker.worker_id} registered. RAM: {worker.hardware.ram_available_gb}GB")
    return {"message": "Registration successful", "master_status": "active"}

@app.post("/ingest")
async def ingest_documents(req: IngestRequest):
    count = 0
    for text in req.documents:
        if text.strip():
            new_task = TaskChunk(
                task_id=str(uuid.uuid4()), task_type=TaskType.EMBEDDING, payload=text.strip(), is_obfuscated=False
            )
            task_queue.append(new_task)
            count += 1
    print(f"[+] Ingested {count} new chunks into the task queue.")
    return {"message": f"Added {count} tasks", "queue_size": len(task_queue)}

@app.post("/results", response_model=dict)
async def submit_result(result: TaskResult):
    task_data = active_tasks.pop(result.task_id, None)
    original_task = task_data["chunk"] if task_data else None

    if result.status == TaskStatus.SUSPENDED:
        print(f"[!] Worker {result.worker_id} GHOST EXITED. Re-queuing Task {result.task_id[:8]}...")
        if original_task:
            task_queue.insert(0, original_task) 
    else:
        print(f"[✓] Task {result.task_id[:8]}... completed by {result.worker_id}")
        completed_tasks.append(result)
        if result.result_data and original_task:
            vault.insert_chunk(task_id=result.task_id, content=original_task.payload, vector=result.result_data)
            
    if result.worker_id in active_workers:
         # CRITICAL: Mark worker as idle so the Dispatcher loop can push the next task to it!
         active_workers[result.worker_id]["status"] = "idle" 
         active_workers[result.worker_id]["last_seen"] = time.time() 
         
    return {"message": "Result acknowledged"}

@app.post("/results/batch", response_model=dict)
async def submit_batch_results(results: List[TaskResult]):
    """Receives a batch of completed math and saves them all to the Vault."""
    worker_id = None
    
    for result in results:
        worker_id = result.worker_id
        task_data = active_tasks.pop(result.task_id, None)
        original_task = task_data["chunk"] if task_data else None

        if result.status == TaskStatus.SUSPENDED:
            if original_task:
                task_queue.insert(0, original_task) 
        else:
            completed_tasks.append(result)
            if result.result_data and original_task:
                vault.insert_chunk(task_id=result.task_id, content=original_task.payload, vector=result.result_data)
                
    if worker_id and worker_id in active_workers:
         active_workers[worker_id]["status"] = "idle" 
         active_workers[worker_id]["last_seen"] = time.time()
         print(f"[✓] Worker {worker_id} successfully completed a batch of {len(results)} tasks.")
         
    return {"message": f"Batch of {len(results)} results acknowledged"}

@app.get("/health")
async def health_check():
    return {
        "active_workers": len(active_workers), "queued_tasks": len(task_queue),
        "in_progress_tasks": len(active_tasks), "completed_tasks": len(completed_tasks)
    }

@app.get("/", response_class=HTMLResponse)
async def serve_dashboard():
    with open("master/dashboard.html", "r", encoding="utf-8") as f:
        return f.read()

@app.post("/api/chat")
async def chat_endpoint(req: ChatRequest):
    with torch.no_grad():
        query_vector = search_model.encode(req.query, convert_to_numpy=True).tolist()
    cursor = vault.conn.cursor()
    cursor.execute("""
        SELECT d.content, vec_distance_L2(v.vector, ?) as distance
        FROM vec_documents v JOIN documents d ON v.rowid = d.rowid
        ORDER BY distance ASC LIMIT 1
    """, (json.dumps(query_vector),))
    result = cursor.fetchone()
    if not result:
        return {"answer": "The Vault is empty.", "context": "None"}
    best_match_text = result[0]
    prompt = f"You are the Scavenger Grid AI. Use ONLY this context to answer.\nContext:\n{best_match_text}\nQuestion: {req.query}\nAnswer:"
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post("http://localhost:11434/api/generate", json={"model": "llama3", "prompt": prompt, "stream": False}, timeout=60.0)
            answer = resp.json().get("response", "Error generating response.")
    except Exception as e:
        answer = f"Failed to connect to Ollama: {str(e)}"
    return {"answer": answer, "context": best_match_text}