import sys
import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import multiprocessing
import logging
_log = logging.getLogger(__name__)

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

from app.routers import router

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router.router)

if __name__ == '__main__':
    _log.info(f"starting app")
    multiprocessing.freeze_support()
    # uvicorn.run(app, host="127.0.0.1", port=8001, reload=True)
    uvicorn.run("__main__:app", host="127.0.0.1", port=8001, reload=True)
