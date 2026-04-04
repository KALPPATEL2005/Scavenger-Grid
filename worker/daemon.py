import time
import platform
import psutil
import httpx
import socket
import json
import asyncio
import websockets
import uuid
from zeroconf import Zeroconf

from shared.protocol import WorkerRegistration, HardwareProfile, TaskStatus, TaskResult
from worker.ghost_exit import ActivityMonitor
from worker.compute_engine import SemanticEngine

# --- ROBUST DYNAMIC FINGERPRINTING ---
# Appends a 4-character unique ID to the hostname so multiple terminals on 1 PC never collide.
base_name = socket.gethostname().lower().replace(' ', '-')
short_id = str(uuid.uuid4())[:4]
WORKER_ID = f"worker-{base_name}-{short_id}"

MASTER_URL = None

monitor = ActivityMonitor(idle_threshold_sec=3)
ai_engine = SemanticEngine()

def discover_master() -> str:
    print("[*] Searching for Scavenger Master on the local Wi-Fi (mDNS)...")
    zc = Zeroconf()
    for _ in range(5):
        info = zc.get_service_info("_scavenger._tcp.local.", "MasterNode._scavenger._tcp.local.")
        if info:
            ip = socket.inet_ntoa(info.addresses[0])
            url = f"http://{ip}:{info.port}"
            print(f"[+] Found Master beacon at {url}\n")
            zc.close()
            return url
        time.sleep(1)
    zc.close()
    print("[-] Could not locate Master on the network.")
    return None

def get_hardware_profile() -> HardwareProfile:
    vm = psutil.virtual_memory()
    return HardwareProfile(
        cpu_cores=psutil.cpu_count(logical=True), 
        ram_total_gb=round(vm.total / (1024 ** 3), 2),
        ram_available_gb=round(vm.available / (1024 ** 3), 2), 
        # Truly read the raw OS kernel:
        os_name=f"{platform.system()} {platform.release()}", 
        is_idle=not monitor.is_user_active()
    )

def register_with_master():
    profile = get_hardware_profile()
    payload = WorkerRegistration(worker_id=WORKER_ID, ip_address="dynamic", port=0, hardware=profile)
    try:
        response = httpx.post(f"{MASTER_URL}/register", json=payload.model_dump())
        response.raise_for_status()
        print(f"[+] Successfully registered as {WORKER_ID}! {profile.cpu_cores} Cores ready.\n")
        return True
    except Exception as e:
        print(f"[!] Failed to connect to Master: {e}")
        return False

# --- THE WEBSOCKET PIPELINE ---
async def listen_and_process_tasks():
    ws_url = MASTER_URL.replace("http://", "ws://") + f"/ws/{WORKER_ID}"
    
    while True:
        try:
            async with websockets.connect(ws_url) as ws:
                print("\n[+] WebSocket Tunnel established! Waiting silently for tasks...")
                
                while True:
                    message = await ws.recv()
                    tasks = json.loads(message)
                    
                    print(f"\n[>] Master pushed a Batch of {len(tasks)} tasks...")
                    
                    start_time = time.time()
                    is_suspended = False
                    vectors = []
                    
                    texts_to_process = [t["payload"] for t in tasks]
                    
                    if monitor.is_user_active():
                        print("    [!!!] GHOST EXIT: User active. Rejecting batch.")
                        is_suspended = True
                    else:
                        print(f"    [~] Crunching {len(tasks)} chunks simultaneously in PyTorch...")
                        vectors = ai_engine.process_batch(texts_to_process)
                        
                        if monitor.is_user_active():
                            print("    [!!!] GHOST EXIT: Interrupt detected during math.")
                            is_suspended = True

                    processing_time = round(time.time() - start_time, 2)
                    final_status = TaskStatus.SUSPENDED if is_suspended else TaskStatus.COMPLETED
                    
                    batch_results = []
                    for i, task in enumerate(tasks):
                        res = TaskResult(
                            task_id=task["task_id"], 
                            worker_id=WORKER_ID, 
                            status=final_status,
                            result_data=vectors[i] if not is_suspended else None,
                            processing_time_sec=processing_time
                        )
                        batch_results.append(res.model_dump(mode='json'))
                    
                    async with httpx.AsyncClient() as client:
                        await client.post(f"{MASTER_URL}/results/batch", json=batch_results)
                        
                    print(f"[{'!' if is_suspended else '✓'}] Sent {len(batch_results)} {final_status.value.upper()} tasks to Master in {processing_time}s!")
                    
                    if is_suspended:
                        await asyncio.sleep(5)
                        
        except websockets.exceptions.ConnectionClosed:
            print("[!] WebSocket connection lost. Reconnecting in 3s...")
            await asyncio.sleep(3)
        except Exception as e:
            print(f"[!] Network error: {e}. Retrying in 5s...")
            await asyncio.sleep(5)

if __name__ == "__main__":
    print("=== Scavenger Grid Worker Daemon ===")
    MASTER_URL = discover_master()
    if MASTER_URL:
        monitor.start()
        if register_with_master():
            try:
                asyncio.run(listen_and_process_tasks())
            except KeyboardInterrupt:
                print(f"\n[!] Ctrl+C detected. Shutting down {WORKER_ID} gracefully. Goodbye!")