import time
import uuid
import platform
import psutil
import httpx
import socket
from zeroconf import Zeroconf

# Import our shared protocols, the OS monitor, and our NEW AI Engine!
from shared.protocol import WorkerRegistration, HardwareProfile, TaskStatus, TaskResult
from worker.ghost_exit import ActivityMonitor
from worker.compute_engine import SemanticEngine

WORKER_ID = f"worker-{uuid.uuid4().hex[:8]}"
MASTER_URL = None

monitor = ActivityMonitor(idle_threshold_sec=3)
ai_engine = SemanticEngine()  # <-- Loads the PyTorch AI model into RAM on startup!

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
        os_name=f"{platform.system()} {platform.release()}",
        is_idle=not monitor.is_user_active()
    )

def register_with_master():
    profile = get_hardware_profile()
    payload = WorkerRegistration(worker_id=WORKER_ID, ip_address="dynamic", port=0, hardware=profile)
    try:
        response = httpx.post(f"{MASTER_URL}/register", json=payload.model_dump())
        response.raise_for_status()
        print(f"[+] Successfully registered! {profile.cpu_cores} Cores ready.\n")
        return True
    except Exception as e:
        print(f"[!] Failed to connect to Master: {e}")
        return False

def request_and_process_task():
    while True:
        try:
            if monitor.is_user_active():
                print("[-] User is active. Waiting in the shadows...")
                time.sleep(2)
                continue

            print("[*] Polling Master for a task...")
            response = httpx.get(f"{MASTER_URL}/tasks/{WORKER_ID}")
            
            if response.status_code == 200:
                task = response.json()
                task_id = task["task_id"]
                print(f"[>] Received Task: {task_id}")
                
                # --- REAL AI WORK WITH PREEMPTION ---
                start_time = time.time()
                is_suspended = False
                vector_result = None
                
                print("    [~] Processing chunk with PyTorch... (Move your mouse to trigger Ghost Exit!)")
                
                # 1. Check if user bumped mouse BEFORE starting the heavy math
                if monitor.is_user_active():
                    print("    [!!!] GHOST EXIT TRIGGERED: Interrupt detected before processing.")
                    is_suspended = True
                else:
                    # 2. Run the actual AI model! (This takes a fraction of a second)
                    vector_result = ai_engine.process_text(task["payload"])
                    
                    # 3. Check if user bumped mouse DURING the math
                    if monitor.is_user_active():
                        print("    [!!!] GHOST EXIT TRIGGERED: Interrupt detected during math.")
                        is_suspended = True

                processing_time = round(time.time() - start_time, 2)
                final_status = TaskStatus.SUSPENDED if is_suspended else TaskStatus.COMPLETED
                
                # 4. Package the real mathematical vector to send back to the Master
                result = TaskResult(
                    task_id=task_id, 
                    worker_id=WORKER_ID, 
                    status=final_status,
                    result_data=vector_result if not is_suspended else None,
                    processing_time_sec=processing_time
                )
                
                httpx.post(f"{MASTER_URL}/results", json=result.model_dump(mode='json'))
                print(f"[{'!' if is_suspended else '✓'}] Sent {final_status.value.upper()} to Master in {processing_time}s!\n")
                time.sleep(2)
                
            else:
                time.sleep(5)
                
        except Exception as e:
            print(f"[!] Connection error: {e}. Retrying in 5s...")
            time.sleep(5)

if __name__ == "__main__":
    print("=== Scavenger Grid Worker Daemon ===")
    MASTER_URL = discover_master()
    if MASTER_URL:
        monitor.start()
        if register_with_master():
            request_and_process_task()