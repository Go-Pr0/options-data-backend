import os
import asyncio
import logging
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
from contextlib import asynccontextmanager

import requests
import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv

load_dotenv()

# === CONFIGURATION ===
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://myuser:mypassword@localhost:5432/mydatabase")
BYBIT_API_BASE = "https://api.bybit.com/v5"
FETCH_INTERVAL_MINUTES = 15  # Fixed to 15 minutes

# === GLOBAL STATE ===
last_collection_time: Optional[datetime] = None
collection_task: Optional[asyncio.Task] = None

# === LOGGING SETUP ===
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# === PYDANTIC MODELS ===
class OptionsDataPoint(BaseModel):
    timestamp: datetime
    spot_price: float
    option_type: str  # ATM, OTM, ITM
    strike: float
    expiry: datetime
    premium: float
    iv: float

class ChartDataResponse(BaseModel):
    price_data: List[Dict[str, Any]]
    iv_data: List[Dict[str, Any]]

class SystemStatus(BaseModel):
    last_collection: Optional[datetime]
    next_collection_slot: Optional[datetime]
    collection_interval_minutes: int
    current_time: datetime
    is_collection_time: bool

# === TIME MANAGEMENT FUNCTIONS ===
def normalize_datetime(dt: datetime) -> datetime:
    """Convert timezone-aware datetime to timezone-naive UTC."""
    if dt is None:
        return None
    if dt.tzinfo is not None:
        return dt.replace(tzinfo=None)
    return dt

def get_valid_collection_minutes() -> List[int]:
    """Get the valid minutes for data collection: 0, 15, 30, 45."""
    return [0, 15, 30, 45]

def is_collection_time(dt: Optional[datetime] = None) -> bool:
    """Check if the current time is exactly at a collection minute (xx:00, xx:15, xx:30, xx:45)."""
    if dt is None:
        dt = datetime.utcnow()
    
    valid_minutes = get_valid_collection_minutes()
    return dt.minute in valid_minutes and dt.second == 0

def get_next_collection_slot(from_time: Optional[datetime] = None) -> datetime:
    """Get the next valid collection time slot."""
    if from_time is None:
        from_time = datetime.utcnow()
    
    valid_minutes = get_valid_collection_minutes()
    current_minute = from_time.minute
    
    # Find the next valid minute in the current hour
    next_minute = None
    for minute in valid_minutes:
        if minute > current_minute:
            next_minute = minute
            break
    
    if next_minute is not None:
        # Next slot is in the current hour
        next_slot = from_time.replace(minute=next_minute, second=0, microsecond=0)
    else:
        # Next slot is in the next hour (at minute 0)
        next_hour = from_time + timedelta(hours=1)
        next_slot = next_hour.replace(minute=0, second=0, microsecond=0)
    
    return next_slot

def should_collect_data() -> bool:
    """Check if data should be collected now."""
    global last_collection_time
    
    now = datetime.utcnow()
    
    # Only collect at exact collection times
    if not is_collection_time(now):
        return False
    
    # If we have no previous collection, allow it
    if last_collection_time is None:
        return True
    
    last_collection_naive = normalize_datetime(last_collection_time)
    time_since_last = now - last_collection_naive
    
    # Ensure we don't collect twice in the same minute
    return time_since_last >= timedelta(minutes=1)

def update_last_collection_time():
    """Update the last collection timestamp."""
    global last_collection_time
    last_collection_time = datetime.utcnow()

# === DATABASE FUNCTIONS ===
def get_db_connection():
    """Establishes and returns a database connection."""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        return conn
    except psycopg2.OperationalError as e:
        logger.error(f"Could not connect to database: {e}")
        return None

def create_tables():
    """Creates the required tables if they don't exist."""
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS btc_options_data (
                    id SERIAL PRIMARY KEY,
                    timestamp TIMESTAMPTZ NOT NULL,
                    spot_price NUMERIC NOT NULL,
                    option_type TEXT NOT NULL,
                    strike NUMERIC NOT NULL,
                    expiry TIMESTAMPTZ NOT NULL,
                    premium NUMERIC NOT NULL,
                    iv NUMERIC NOT NULL,
                    symbol TEXT NOT NULL
                );
                
                CREATE INDEX IF NOT EXISTS idx_timestamp ON btc_options_data(timestamp);
                CREATE INDEX IF NOT EXISTS idx_option_type ON btc_options_data(option_type);
                CREATE INDEX IF NOT EXISTS idx_expiry ON btc_options_data(expiry);
            """)
            conn.commit()
        logger.info("Database tables created successfully")
        return True
    except Exception as e:
        logger.error(f"Error creating tables: {e}")
        return False
    finally:
        conn.close()

def get_last_collection_from_db() -> Optional[datetime]:
    """Get the last collection time from the database."""
    conn = get_db_connection()
    if not conn:
        return None
    
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT MAX(timestamp) as last_timestamp
                FROM btc_options_data
            """)
            result = cur.fetchone()
            last_time = result[0] if result and result[0] else None
            return normalize_datetime(last_time) if last_time else None
    except Exception as e:
        logger.error(f"Error getting last collection time: {e}")
        return None
    finally:
        conn.close()

def insert_options_data(data_points: List[Dict]):
    """Inserts options data into the database."""
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        with conn.cursor() as cur:
            for data in data_points:
                cur.execute("""
                    INSERT INTO btc_options_data 
                    (timestamp, spot_price, option_type, strike, expiry, premium, iv, symbol)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    data['timestamp'],
                    data['spot_price'],
                    data['option_type'],
                    data['strike'],
                    data['expiry'],
                    data['premium'],
                    data['iv'],
                    data['symbol']
                ))
            conn.commit()
        logger.info(f"Inserted {len(data_points)} data points")
        return True
    except Exception as e:
        logger.error(f"Error inserting data: {e}")
        return False
    finally:
        conn.close()

def get_chart_data(hours: int = 24) -> Dict[str, List[Dict]]:
    """Retrieves chart data from the database."""
    conn = get_db_connection()
    if not conn:
        return {"price_data": [], "iv_data": []}
    
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Get data from the last N hours
            cur.execute("""
                SELECT timestamp, spot_price, option_type, strike, premium, iv
                FROM btc_options_data
                WHERE timestamp >= %s
                ORDER BY timestamp ASC
            """, (datetime.utcnow() - timedelta(hours=hours),))
            
            rows = cur.fetchall()
            
            # Organize data by option type
            price_data = []
            iv_data = []
            
            for row in rows:
                timestamp_str = row['timestamp'].isoformat()
                
                price_point = {
                    'timestamp': timestamp_str,
                    'spot_price': float(row['spot_price']),
                    'option_type': row['option_type'],
                    'strike': float(row['strike']),
                    'premium': float(row['premium'])
                }
                price_data.append(price_point)
                
                iv_point = {
                    'timestamp': timestamp_str,
                    'spot_price': float(row['spot_price']),
                    'option_type': row['option_type'],
                    'iv': float(row['iv'])
                }
                iv_data.append(iv_point)
            
            return {"price_data": price_data, "iv_data": iv_data}
            
    except Exception as e:
        logger.error(f"Error retrieving chart data: {e}")
        return {"price_data": [], "iv_data": []}
    finally:
        conn.close()

# === BYBIT API FUNCTIONS ===
async def fetch_btc_spot_price() -> Optional[float]:
    """Fetches BTC spot price from Bybit."""
    try:
        url = f"{BYBIT_API_BASE}/market/tickers"
        params = {"category": "linear", "symbol": "BTCUSDT"}
        
        response = requests.get(url, params=params, timeout=10)
        data = response.json()
        
        if data.get("retCode") != 0:
            logger.error(f"Bybit API error: {data.get('retMsg')}")
            return None
            
        result = data.get("result", {}).get("list", [])
        if not result:
            logger.warning("No spot price data returned")
            return None
            
        return float(result[0].get("lastPrice"))
        
    except Exception as e:
        logger.error(f"Error fetching spot price: {e}")
        return None

async def fetch_btc_options() -> List[Dict]:
    """Fetches all BTC option instruments from Bybit."""
    try:
        url = f"{BYBIT_API_BASE}/market/instruments-info"
        params = {"category": "option", "baseCoin": "BTC", "limit": 1000}
        
        response = requests.get(url, params=params, timeout=10)
        data = response.json()
        
        if data.get("retCode") != 0:
            logger.error(f"Bybit API error: {data.get('retMsg')}")
            return []
            
        return data.get("result", {}).get("list", [])
        
    except Exception as e:
        logger.error(f"Error fetching options: {e}")
        return []

async def fetch_option_ticker(symbol: str) -> Optional[Dict]:
    """Fetches ticker data for a specific option symbol."""
    try:
        url = f"{BYBIT_API_BASE}/market/tickers"
        params = {"category": "option", "symbol": symbol}
        
        response = requests.get(url, params=params, timeout=10)
        data = response.json()
        
        if data.get("retCode") != 0:
            return None
            
        result = data.get("result", {}).get("list", [])
        return result[0] if result else None
        
    except Exception as e:
        logger.error(f"Error fetching ticker for {symbol}: {e}")
        return None

def find_target_expiry(options: List[Dict], target_days: int = 7) -> Optional[str]:
    """Finds the expiry closest to target days from now."""
    now = datetime.utcnow()
    target_time = now + timedelta(days=target_days)
    
    best_expiry = None
    best_diff = float('inf')
    
    for option in options:
        if option.get('status') != 'Trading':
            continue
            
        delivery_time = option.get('deliveryTime')
        if not delivery_time:
            continue
            
        expiry_dt = datetime.fromtimestamp(int(delivery_time) / 1000)
        diff = abs((expiry_dt - target_time).total_seconds())
        
        if diff < best_diff:
            best_diff = diff
            best_expiry = delivery_time
    
    return best_expiry

def find_moneyness_options(options: List[Dict], spot_price: float, target_expiry: str) -> Dict[str, Optional[str]]:
    """Finds ATM, 5% OTM, and 5% ITM call options for the target expiry."""
    target_strikes = {
        'ITM': spot_price * 0.95,  # 5% ITM
        'ATM': spot_price,         # ATM
        'OTM': spot_price * 1.05   # 5% OTM
    }
    
    result = {'ITM': None, 'ATM': None, 'OTM': None}
    
    # Filter options for target expiry and calls only
    filtered_options = [
        opt for opt in options 
        if (opt.get('deliveryTime') == target_expiry and 
            opt.get('optionsType') == 'Call' and
            opt.get('status') == 'Trading')
    ]
    
    for option_type, target_strike in target_strikes.items():
        best_option = None
        best_diff = float('inf')
        
        for option in filtered_options:
            symbol_parts = option['symbol'].split('-')
            if len(symbol_parts) < 4:
                continue
                
            try:
                strike = float(symbol_parts[2])
                diff = abs(strike - target_strike)
                
                if diff < best_diff:
                    best_diff = diff
                    best_option = option['symbol']
            except (ValueError, IndexError):
                continue
        
        result[option_type] = best_option
    
    return result

# === DATA COLLECTION TASK ===
async def collect_options_data():
    """Main data collection task - only runs at precise collection times."""
    global last_collection_time
    
    now = datetime.utcnow()
    
    logger.info(f"Starting options data collection at {now.strftime('%H:%M:%S')} (minute: {now.minute})")
    
    try:
        # Fetch spot price
        spot_price = await fetch_btc_spot_price()
        if not spot_price:
            logger.error("Failed to fetch spot price")
            return False
        
        logger.info(f"BTC spot price: ${spot_price:,.2f}")
        
        # Fetch all options
        options = await fetch_btc_options()
        if not options:
            logger.error("Failed to fetch options")
            return False
        
        logger.info(f"Found {len(options)} options")
        
        # Find target expiry (closest to 7 days)
        target_expiry = find_target_expiry(options, target_days=7)
        if not target_expiry:
            logger.error("No suitable expiry found")
            return False
        
        expiry_dt = datetime.fromtimestamp(int(target_expiry) / 1000)
        logger.info(f"Target expiry: {expiry_dt}")
        
        # Find moneyness options
        moneyness_options = find_moneyness_options(options, spot_price, target_expiry)
        logger.info(f"Moneyness options: {moneyness_options}")
        
        # Collect data for each option type
        data_points = []
        # Use a normalized timestamp for consistent data points
        timestamp = now.replace(second=0, microsecond=0)
        
        for option_type, symbol in moneyness_options.items():
            if not symbol:
                logger.warning(f"No {option_type} option found")
                continue
            
            ticker = await fetch_option_ticker(symbol)
            if not ticker:
                logger.warning(f"No ticker data for {symbol}")
                continue
            
            mark_price = ticker.get('markPrice')
            iv = ticker.get('markIv')
            
            if not mark_price or not iv:
                logger.warning(f"Missing price/IV data for {symbol}")
                continue
            
            # Extract strike from symbol
            symbol_parts = symbol.split('-')
            strike = float(symbol_parts[2]) if len(symbol_parts) >= 3 else 0
            
            data_point = {
                'timestamp': timestamp,
                'spot_price': spot_price,
                'option_type': option_type,
                'strike': strike,
                'expiry': expiry_dt,
                'premium': float(mark_price),
                'iv': float(iv),
                'symbol': symbol
            }
            data_points.append(data_point)
            
            logger.info(f"{option_type}: {symbol} - Premium: ${float(mark_price):.4f}, IV: {float(iv):.4f}")
        
        # Insert data into database
        if data_points:
            success = insert_options_data(data_points)
            if success:
                # Update last collection time only on successful insertion
                update_last_collection_time()
                logger.info(f"✅ Successfully collected and stored {len(data_points)} data points at {timestamp.strftime('%H:%M')}")
                return True
            else:
                logger.error("Failed to store data points")
                return False
        else:
            logger.warning("No data points collected")
            return False
            
    except Exception as e:
        logger.error(f"Error in data collection: {e}")
        return False

# === CONTINUOUS TIME MONITORING ===
async def time_monitor():
    """Continuously monitor time and trigger collection at precise intervals."""
    logger.info("🕐 Time monitor started - watching for collection times (xx:00, xx:15, xx:30, xx:45)")
    
    while True:
        try:
            if should_collect_data():
                logger.info("🎯 Collection time detected - starting data collection")
                await collect_options_data()
            
            # Sleep for 1 second to check time precisely
            await asyncio.sleep(1)
            
        except Exception as e:
            logger.error(f"Error in time monitor: {e}")
            await asyncio.sleep(5)  # Wait a bit longer on error

# === FASTAPI APP ===
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    global last_collection_time, collection_task
    
    logger.info("🚀 Starting BTC Options Tracker API...")
    
    # Initialize database
    if not create_tables():
        logger.error("Failed to create database tables")
        raise RuntimeError("Database initialization failed")
    
    # Initialize last collection time from database
    last_collection_time = get_last_collection_from_db()
    if last_collection_time:
        logger.info(f"📊 Last collection from database: {last_collection_time}")
    else:
        logger.info("📊 No previous collections found in database")
    
    # Start the time monitoring task
    collection_task = asyncio.create_task(time_monitor())
    logger.info("⏰ Time monitoring started - waiting for collection slots")
    
    # Show next collection time
    next_slot = get_next_collection_slot()
    logger.info(f"⏭️  Next collection slot: {next_slot.strftime('%H:%M:%S')}")
    
    yield
    
    # Shutdown
    if collection_task:
        collection_task.cancel()
        try:
            await collection_task
        except asyncio.CancelledError:
            pass
    logger.info("🛑 Time monitoring stopped")

app = FastAPI(
    title="BTC Options Tracker",
    description="Real-time BTC options tracking with moneyness analysis - Precise 15-minute intervals",
    version="1.0.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# === API ENDPOINTS ===
@app.get("/")
async def root():
    """Health check endpoint."""
    return {
        "message": "BTC Options Tracker API", 
        "status": "running",
        "collection_schedule": "Every 15 minutes at xx:00, xx:15, xx:30, xx:45",
        "last_collection": last_collection_time.isoformat() if last_collection_time else None,
        "next_collection_slot": get_next_collection_slot().isoformat()
    }

@app.get("/api/status", response_model=SystemStatus)
async def get_system_status():
    """Get system status including collection timing information."""
    now = datetime.utcnow()
    return SystemStatus(
        last_collection=last_collection_time,
        next_collection_slot=get_next_collection_slot(),
        collection_interval_minutes=FETCH_INTERVAL_MINUTES,
        current_time=now,
        is_collection_time=is_collection_time(now)
    )

@app.get("/api/chart-data", response_model=ChartDataResponse)
async def get_chart_data_endpoint(hours: int = 24):
    """Get chart data for the specified number of hours."""
    if hours < 1 or hours > 168:  # Max 1 week
        raise HTTPException(status_code=400, detail="Hours must be between 1 and 168")
    
    data = get_chart_data(hours)
    return ChartDataResponse(
        price_data=data["price_data"],
        iv_data=data["iv_data"]
    )

@app.get("/api/latest-data")
async def get_latest_data():
    """Get the latest data point for each option type."""
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT DISTINCT ON (option_type) 
                    timestamp, spot_price, option_type, strike, premium, iv, symbol
                FROM btc_options_data
                ORDER BY option_type, timestamp DESC
            """)
            
            rows = cur.fetchall()
            return [dict(row) for row in rows]
            
    except Exception as e:
        logger.error(f"Error retrieving latest data: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve data")
    finally:
        conn.close()

# Note: No manual trigger endpoint - data collection is strictly time-controlled
# Collections only happen at xx:00, xx:15, xx:30, xx:45

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)