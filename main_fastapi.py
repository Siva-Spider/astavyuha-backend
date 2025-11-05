# main_fastapi.py
import asyncio
import json
from fastapi import FastAPI, BackgroundTasks, Request
from fastapi.responses import StreamingResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import datetime
from logger_util import get_log_buffer, push_log
from trading_core import run_trading_logic_for_all

app = FastAPI(title="Asta Vyuha FastAPI")

# Allow CORS as your frontend expects
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # restrict in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Track background asyncio tasks so we don't spawn duplicates
background_tasks_registry = {}

@app.get("/")
def root():
    return {"status": "FastAPI backend running 🚀"}

@app.post("/api/start-all-trading")
async def start_all_trading(request: Request):
    payload = await request.json()
    trading_parameters = payload.get("tradingParameters", [])
    selected_brokers = payload.get("selectedBrokers", [])

    # create a unique job key if you like (e.g., user + timestamp)
    job_key = f"trading_job"

    if job_key in background_tasks_registry and not background_tasks_registry[job_key].done():
        return JSONResponse({"status": "already_running", "message": "Trading job already running"}, status_code=200)

    # wrap blocking function to run in a threadpool
    loop = asyncio.get_event_loop()
    task = loop.run_in_executor(None, run_trading_logic_for_all, trading_parameters, selected_brokers, None)
    background_tasks_registry[job_key] = task

    push_log("🟢 Started trading task via FastAPI background executor")
    return {"status": "started", "job_key": job_key}

# Simple SSE / streaming logs endpoint using async generator
@app.get("/api/stream-logs")
async def stream_logs():
    async def event_generator():
        last_seen = set()
        while True:
            try:
                items = get_log_buffer() or []
                for it in items:
                    key = (it.get("ts"), str(it.get("message")))
                    if key not in last_seen:
                        last_seen.add(key)
                        data_str = json.dumps(it, default=str)
                        yield f"event: {it.get('type','log')}\ndata: {data_str}\n\n"
                await asyncio.sleep(0.8)
            except asyncio.CancelledError:
                break
            except Exception:
                await asyncio.sleep(1)
    return StreamingResponse(event_generator(), media_type="text/event-stream")

# optional: a health-check endpoint to ensure the background task is running
@app.get("/api/trading-status")
def trading_status():
    running = any(not t.done() for t in background_tasks_registry.values()) if background_tasks_registry else False
    return {"trading_running": running, "active_jobs": len(background_tasks_registry)}

# other convenience endpoints like connect-broker or get_profit_loss
# you can copy relevant logic from your existing app.py here
