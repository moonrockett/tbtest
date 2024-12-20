from fastapi import FastAPI, Request, Response, HTTPException
from telegram import Update
from telegram.ext import Application
import sys
import os
from pathlib import Path
import asyncio
import logging

# Add parent directory to path to import bot code
sys.path.append(str(Path(__file__).parent.parent))
from testmain import setup_application

app = FastAPI()

# Initialize bot application
token = os.getenv('TELEGRAM_BOT_TOKEN')
if not token:
    raise ValueError("No TELEGRAM_BOT_TOKEN provided")
    
bot_app = Application.builder().token(token).build()
setup_application(bot_app)  # This will be our new setup function

logger = logging.getLogger(__name__)

@app.get("/")
async def index():
    """Simple index route"""
    return {"status": "Bot is running"}

@app.post("/api/webhook")
async def webhook(request: Request):
    """Handle incoming Telegram updates via webhook"""
    try:
        # Get the update data
        data = await request.json()
        
        # Process update with timeout
        update = Update.de_json(data, bot_app.bot)
        # Use asyncio.wait_for to enforce timeout
        await asyncio.wait_for(
            bot_app.process_update(update),
            timeout=8.0  # Set to 8 seconds to allow for some overhead
        )
        
        return Response(status_code=200)
    except asyncio.TimeoutError:
        # If operation takes too long, return 200 to prevent Telegram retries
        return Response(status_code=200)
    except Exception as e:
        # Log the error but return 200 to prevent Telegram retries
        logger.error(f"Error processing update: {e}")
        return Response(status_code=200)

@app.get("/api/webhook")
async def webhook_info():
    """Simple health check endpoint"""
    return {"status": "ok"} 