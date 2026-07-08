# 🕸️ Scavenger Grid: Decentralized Edge-AI Vectorization

[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![PyTorch](https://img.shields.io/badge/PyTorch-%23EE4C2C.svg?style=flat&logo=PyTorch&logoColor=white)](https://pytorch.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-005571?style=flat&logo=fastapi)](https://fastapi.tiangolo.com/)

**Scavenger Grid** is a fault-tolerant, decentralized edge computing framework engineered to run heavy AI workloads on volatile, consumer-grade hardware — with zero cloud dependency.

It is specifically designed for **Retrieval-Augmented Generation (RAG)** pipelines. By pooling the idle processing power of everyday enterprise laptops, Scavenger Grid generates high-dimensional vector embeddings entirely on-premises, eliminating expensive cloud API costs and keeping sensitive enterprise data fully private.

---

## The Problem It Solves

Running AI vectorization for RAG at enterprise scale presents a trilemma:

- **Cloud APIs** (OpenAI, AWS) are fast, but costly and privacy-invasive — unsuitable for sensitive or regulated data.
- **Single local machines** preserve privacy but are too slow and prone to thermal throttling under sustained ML workloads.
- **Classical distributed frameworks** (Spark, Hadoop) require dedicated, always-on cluster machines with static IPs — they fail gracefully on consumer hardware that users actually need for their own work.

Scavenger Grid closes this gap: it turns an ordinary office LAN of employee laptops into a resilient, self-healing AI compute cluster that **instantly yields back to the user the moment they need their machine**.

---

## Core Innovations

### The "Ghost Exit" Protocol
A novel OS-level hardware interrupt daemon running on each Worker Node. The moment a user moves their mouse, types, or closes their laptop lid, the daemon immediately suspends all PyTorch tensor computations and releases 100% of CPU and RAM back to the user — with zero perceived slowdown.

### Self-Healing Watchdog
Every task dispatched to a Worker Node is tracked by the Orchestrator's Watchdog process. If a node goes silent (Ghost Exit, lid close, Wi-Fi drop), the Watchdog instantly detects the WebSocket collapse and re-queues all orphaned chunks to other active nodes. **Zero data loss, full fault tolerance.**

### Zero-Configuration mDNS Scaling
No static IPs, no manual configuration. Worker Nodes use Multicast DNS (mDNS) to autonomously discover the Master Orchestrator on the local network. Each node generates a unique hardware fingerprint (hostname + UUID + CPU/RAM/OS telemetry) and self-registers. Nodes join and leave the cluster dynamically without any human intervention.

### Persistent WebSocket Dispatching
Unlike legacy distributed systems that rely on slow HTTP polling, the Orchestrator maintains persistent bi-directional WebSocket tunnels with every Worker Node, enabling low-latency task batching and real-time telemetry.

### Privacy-First Local Execution
Documents are chunked, vectorized, and stored entirely within the local network. Embeddings are generated via local `SentenceTransformers` and RAG queries are executed against a locally running Llama-3 instance. **Data never leaves your LAN.**

---

## Architecture Overview

Scavenger Grid is composed of two primary components communicating over a local network:

```
    ┌─────────────────────── MASTER NODE (Orchestrator) ────────────────────────────────┐
    │                                                                                   │
    │  ┌──────────────┐            ┌──────────────┐            ┌──────────────────┐     │
    │  │  Shredder    │──(chunks)─▶ Orchestrator  │───────────▶  Self-Healing    │     │
    │  │  (Ingestion) │            │ (Dispatcher) │            │    Watchdog      │     │
    │  └──────────────┘            └──────────────┘            └──────────────────┘     │
    │                                     │                           ▲                 │
    │                                     │                           ║                 │
    │                    ┌────────────────▼──────────────────┐        ║ (Telemetry)     │
    │                    │ Stateful Vector Vault (SQLite WAL)│        ║                 │
    │                    │        & Global Task Queue        │        ║                 │
    │                    └───────────────────────────────────┘        ║                 │
    │                                     │                           ║                 │
    └─────────────────────────────────────┼───────────────────────────╫─────────────────┘
                                          │                           ║
                                 WebSocket Tunnels                Out-of-Band
                                   (Local Wi-Fi)               Interrupt Signal
                                          │                           ║
    ┌─────────────────────────────────────▼───────────────────────────╫─────────────────┐
    │                                                                 ║                 │
    │                      ┌──────────────────────┐                   ║                 │
    │                      │  shared/protocol.py  │═══════════════════╝                 │
    │                      │ (Common Data Models) │                                     │
    │                      └──────────────────────┘                                     │
    │                                 │                                                 │
    │  ┌──────────────────────────────┼──────────────────────────────────────────────┐  │
    │  │                              │                                              │  │
    │  │               ┌──────────────▼───────────────┐                              │  │
    │  │               │   Semantic Compute Engine    │                              │  │
    │  │               │    (PyTorch Embeddings)      │                              │  │
    │  │               └───────────────┬──────────────┘                              │  │
    │  │                               │                                             │  │
    │  │                               ▼                                             │  │
    │  │               ┌───────────────┴──────────────┐                              │  │
    │  │               │      Ghost Exit Daemon       │                              │  │
    │  │               │     (OS Interrupt Listener)  │                              │  │
    │  │               └──────────────────────────────┘                              │  │
    │  │                                                                             │  │
    │  └─────────────── WORKER NODE (Dynamic Edge Device) ───────────────────────────┘  │
    │                                                                                   │
    └───────────────────────────────────────────────────────────────────────────────────┘
```

**Master Node (Orchestrator):** Handles document ingestion (`shredder.py`), WebSocket task dispatching, Watchdog fault recovery, vector storage (`vector_store.py`), and local RAG querying (`search.py`) via an SQLite WAL Vector Vault.

**Worker Node (Edge Daemon):** Runs silently in the background (`daemon.py`), listens for task batches over WebSocket, performs localized PyTorch vectorization (`compute_engine.py`), and monitors hardware activity for Ghost Exit triggers (`ghost_exit.py`).

**Shared Layer:** Common WebSocket message schemas and task status types (`protocol.py`) and connection security utilities (`security.py`) used by both Master and Worker.

---

## How It Works

**1. Network Discovery**
On startup, the Master Orchestrator broadcasts an mDNS service beacon. Worker Nodes on the same LAN detect the beacon, fingerprint their hardware, and establish a persistent WebSocket connection — no configuration required.

**2. Document Ingestion & Task Distribution**
A user uploads a PDF via the Orchestrator UI. The document is shredded into semantically meaningful text chunks and added to the Global Task Queue. The Dispatcher pushes batches to idle Worker Nodes over WebSocket.

**3. Localized Vectorization**
Each Worker Node processes its batch using `sentence-transformers/all-MiniLM-L6-v2` locally, generating dense high-dimensional embeddings via PyTorch. Data never leaves the device.

**4. Ghost Exit Interrupt Handling**
If the Worker's local user interacts with their machine (mouse, keyboard, lid), the `ghost_exit.py` daemon fires immediately:
- PyTorch computations are halted at the OS level
- CPU/RAM drop back to near-idle
- A `SUSPENDED` telemetry payload is sent to the Orchestrator

**5. Watchdog Recovery**
The Orchestrator's Watchdog detects the `SUSPENDED` signal (or a raw WebSocket disconnect for hard shutdowns) and instantly re-queues all orphaned task chunks to other live Worker Nodes.

**6. Vector Storage & RAG Query**
Completed embeddings are returned to the Orchestrator and stored in a local SQLite database with Write-Ahead Logging (WAL) for lock-free concurrent writes. When a user submits a query, the system vectorizes it, finds the nearest chunks via L2 distance, and feeds them as context to a locally running Llama-3 instance to generate a private, grounded answer.

---

## Benchmarks

All experiments run on an IEEE 802.11ac (Wi-Fi 5) LAN with 1,000 document chunks from a corporate PDF.

### Horizontal Scaling

| Worker Nodes | Processing Time | Throughput      | mDNS Discovery |
|:------------:|:---------------:|:---------------:|:--------------:|
| 1 (Baseline) | 124.5 s         | 8.03 chunks/s   | 1.2 s          |
| 2            | 63.8 s          | 15.67 chunks/s  | 1.4 s          |
| 3            | 44.1 s          | 22.67 chunks/s  | 1.1 s          |
| 4            | 33.6 s          | 29.76 chunks/s  | 1.3 s          |

Near-linear scaling. New nodes detect the Orchestrator via mDNS and begin processing within **~1.3 seconds** of joining the network.

### Fault Tolerance & Ghost Exit Recovery

| Interruption Event             | Detection Mechanism        | Detection Latency | Re-queue Latency | Data Loss |
|:-------------------------------|:---------------------------|:-----------------:|:----------------:|:---------:|
| User moves mouse / types       | OS Hook (ActivityMonitor)  | < 100 ms          | < 50 ms          | **0%**    |
| Laptop lid closed abruptly     | WebSocketDisconnect        | < 10 ms           | < 50 ms          | **0%**    |
| Silent Wi-Fi drop              | Async Watchdog Timeout     | 30.0 s            | < 50 ms          | **0%**    |

### Resource Yielding (CPU Profiling on Worker Node)

| System State                    | CPU Utilization | RAM Usage      | User Impact              |
|:--------------------------------|:---------------:|:--------------:|:------------------------:|
| Idle (tunnel open, waiting)     | ~1.5%           | Base + 60 MB   | None                     |
| Processing AI tasks (PyTorch)   | 85–100%         | Base + 850 MB  | Noticeable slowdown      |
| Post-interrupt (Ghost Exit)     | < 2.0%          | Base + 60 MB   | **None (instantly restored)** |

---

## Quick Start

### Prerequisites
- Python 3.9+
- [Ollama](https://ollama.com/) installed locally with `llama3` pulled
- A local Wi-Fi or LAN network with mDNS traffic allowed

### Installation

Clone and install dependencies on **every machine** that will participate (both Master and Workers):

```bash
git clone https://github.com/yourusername/scavenger-grid.git
cd scavenger-grid
pip install -r requirements.txt
```

### Running the Grid

**Terminal 1 — Start the Master Orchestrator:**
```bash
python -m master.orchestrator
```
The Orchestrator Dashboard will be available at `http://localhost:8000`. It begins broadcasting its mDNS beacon immediately.

**Terminal 2+ — Start Worker Nodes:**

Run this on any other laptop on the same Wi-Fi network (or the same machine for local testing):
```bash
python -m worker.daemon
```

Each Worker will autonomously locate the Master, generate its hardware fingerprint, establish a WebSocket connection, and appear live in the Orchestrator Dashboard. Scale horizontally by simply running more workers — no reconfiguration needed.

---

## Key Technical Properties

| Property                  | Implementation                                                    |
|:--------------------------|:------------------------------------------------------------------|
| Task fault tolerance      | WebSocket Watchdog + async re-queue on disconnect or suspension   |
| Network discovery         | Multicast DNS (mDNS) — no static IPs required                    |
| Node identity             | Hostname + UUID + hardware telemetry fingerprint                  |
| Resource yielding         | OS-level interrupt hooks via `ghost_exit.py` daemon              |
| Embedding model           | `sentence-transformers/all-MiniLM-L6-v2` (local, no API)         |
| Vector storage            | SQLite with WAL mode (lock-free concurrent reads/writes)          |
| RAG LLM                   | Llama-3 via Ollama (local inference, no cloud)                    |
| Transport layer           | Persistent bi-directional WebSockets (FastAPI / asyncio)          |

---

## Project Structure

```
scavenger-grid/
├── master/
│   ├── __init__.py
│   ├── dashboard.html          # Orchestrator web UI
│   ├── network_profiler.py     # Hardware telemetry and node profiling
│   ├── orchestrator.py         # FastAPI server, mDNS beacon, task dispatcher
│   ├── search.py               # L2 vector search and RAG query engine
│   ├── shredder.py             # PDF parsing and semantic chunking
│   └── vector_store.py         # SQLite WAL vector storage
├── shared/
│   ├── __init__.py
│   ├── protocol.py             # Shared WebSocket message types and task schemas
│   └── security.py             # Auth and connection security utilities
├── worker/
│   ├── __init__.py
│   ├── compute_engine.py       # SentenceTransformers / PyTorch embedding engine
│   ├── daemon.py               # Worker entry point, mDNS discovery, WebSocket client
│   └── ghost_exit.py           # Ghost Exit hardware interrupt daemon
├── .gitignore
├── manifesto.txt
├── requirements.txt
└── README.md
```

---

## Roadmap

- [ ] GPU acceleration support on capable Worker Nodes
- [ ] Web-based real-time cluster topology visualizer
- [ ] Pluggable embedding model support (BAAI/bge, E5, etc.)
- [ ] Cross-subnet multi-LAN federation via relay nodes
- [ ] Windows `ActivityMonitor` implementation (current: macOS/Linux)
- [ ] Docker-based Worker Node packaging

---

## Contributing

Contributions are welcome. Please open an issue to discuss your proposed change before submitting a pull request. For major architectural changes, include benchmark comparisons against the baseline figures above.

