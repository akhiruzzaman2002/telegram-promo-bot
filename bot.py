#!/usr/bin/env python3
"""
PromoBot - Telegram Points & Promotion Bot
Version: 2.0.0
"""

import os
import json
import sqlite3
import urllib.request
import urllib.error
import time
import random
import string
import shutil
import logging
import threading
import signal
import sys
from datetime import datetime, timedelta
from functools import wraps
from typing import Optional, Tuple, List, Dict, Any

from dotenv import load_dotenv

# ========== LOGGING SETUP ==========
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(
    level=logging.INFO,
    format=LOG_FORMAT,
    handlers=[
        logging.FileHandler('bot.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# ========== CONFIGURATION VALIDATION ==========
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    logger.error("BOT_TOKEN not found in environment variables!")
    sys.exit(1)

BOT_USERNAME = os.getenv("BOT_USERNAME", "")
if not BOT_USERNAME:
    logger.warning("BOT_USERNAME not set. Some features may not work.")

API_URL = f"https://api.telegram.org/bot{BOT_TOKEN}/"

# Parse admin IDs
ADMIN_IDS = []
admin_ids_str = os.getenv("ADMIN_IDS", "")
if admin_ids_str:
    try:
        ADMIN_IDS = [int(x.strip()) for x in admin_ids_str.split(",") if x.strip()]
    except ValueError:
        logger.warning("Invalid ADMIN_IDS format. Use comma-separated numbers.")

# Bot configuration
REQUIRED_CHANNEL = os.getenv("REQUIRED_CHANNEL", "")
REQUIRED_CHANNEL_LINK = os.getenv("REQUIRED_CHANNEL_LINK", "")
SUBSCRIPTION_DAYS = int(os.getenv("SUBSCRIPTION_DAYS", "7"))
PENALTY_PERCENT = int(os.getenv("PENALTY_PERCENT", "50"))
DAILY_BONUS = int(os.getenv("DAILY_BONUS", "10"))
PORT = int(os.getenv("PORT", "8080"))

# ========== PRICE CONFIGURATION ==========
PRICE_PACKS = {
    "pack_100": {"stars": 5, "points": 100, "name": "100 Points"},
    "pack_500": {"stars": 20, "points": 500, "name": "500 Points"},
    "pack_1000": {"stars": 35, "points": 1000, "name": "1000 Points"},
    "pack_5000": {"stars": 150, "points": 5000, "name": "5000 Points"},
}

PROMOTION_COSTS = {
    "channel": 500,
    "bot": 300,
    "group": 400,
    "post": 100
}

# ========== RATE LIMITER ==========
class RateLimiter:
    def __init__(self, max_requests: int = 30, time_window: int = 60):
        self.max_requests = max_requests
        self.time_window = time_window
        self.requests: Dict[int, List[float]] = {}
    
    def is_allowed(self, user_id: int) -> Tuple[bool, int, int]:
        now = time.time()
        if user_id not in self.requests:
            self.requests[user_id] = []
        self.requests[user_id] = [t for t in self.requests[user_id] if now - t < self.time_window]
        current = len(self.requests[user_id])
        if current >= self.max_requests:
            return False, self.max_requests, current
        self.requests[user_id].append(now)
        return True, self.max_requests, current

rate_limiter = RateLimiter()

def rate_limit(func):
    @wraps(func)
    def wrapper(update: Dict, *args, **kwargs):
        user_id = None
        if "message" in update:
            user_id = update["message"]["from"]["id"]
        elif "callback_query" in update:
            user_id = update["callback_query"]["from"]["id"]
        if user_id:
            allowed, max_req, current = rate_limiter.is_allowed(user_id)
            if not allowed:
                logger.warning(f"Rate limit exceeded for user {user_id}")
                send_message(user_id, f" Too many requests!\n\nPlease wait.\n\nLimit: {max_req} requests per minute.")
                return
        return func(update, *args, **kwargs)
    return wrapper

# ========== DATABASE WITH RETRY ==========
def with_db_retry(max_retries: int = 3):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except sqlite3.OperationalError as e:
                    if "database is locked" in str(e) and attempt < max_retries - 1:
                        time.sleep(0.1 * (attempt + 1))
                        continue
                    logger.error(f"DB error after {attempt+1} attempts: {e}")
                    raise
                except Exception as e:
                    logger.error(f"Unexpected DB error: {e}")
                    raise
            return None
        return wrapper
    return decorator

def init_database():
    try:
        conn = sqlite3.connect('bot_data.db', timeout=10)
        c = conn.cursor()
        c.execute("PRAGMA journal_mode=WAL")
        c.execute("PRAGMA synchronous=NORMAL")
        c.execute("PRAGMA cache_size=-10000")
        
        c.execute('''CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            points INTEGER DEFAULT 0,
            total_earned INTEGER DEFAULT 0,
            total_spent INTEGER DEFAULT 0,
            total_purchased INTEGER DEFAULT 0,
            total_penalty INTEGER DEFAULT 0,
            refer_code TEXT UNIQUE,
            referrer_id INTEGER,
            joined_date TIMESTAMP,
            last_active TIMESTAMP,
            last_daily TIMESTAMP
        )''')
        
        c.execute('''CREATE TABLE IF NOT EXISTS active_subscriptions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            subscription_type TEXT,
            subscription_id TEXT,
            subscription_name TEXT,
            points_earned INTEGER,
            subscribed_at TIMESTAMP,
            expires_at TIMESTAMP,
            is_active BOOLEAN DEFAULT 1,
            UNIQUE(user_id, subscription_type, subscription_id)
        )''')
        
        c.execute('''CREATE TABLE IF NOT EXISTS task_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            task_type TEXT,
            task_id TEXT,
            points INTEGER,
            completed_at TIMESTAMP
        )''')
        
        c.execute('''CREATE TABLE IF NOT EXISTS campaigns (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            owner_id INTEGER,
            content_type TEXT,
            content_id TEXT,
            title TEXT,
            required_points INTEGER,
            status TEXT DEFAULT 'active',
            created_at TIMESTAMP,
            total_clicks INTEGER DEFAULT 0
        )''')
        
        c.execute('''CREATE TABLE IF NOT EXISTS promotion_views (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            campaign_id INTEGER,
            user_id INTEGER,
            viewed_at TIMESTAMP,
            UNIQUE(campaign_id, user_id)
        )''')
        
        c.execute('''CREATE TABLE IF NOT EXISTS promo_waiting (
            user_id INTEGER PRIMARY KEY,
            promo_type TEXT,
            created_at TIMESTAMP
        )''')
        
        c.execute('''CREATE TABLE IF NOT EXISTS promo_temp (
            user_id INTEGER PRIMARY KEY,
            promo_type TEXT,
            content_id TEXT,
            created_at TIMESTAMP
        )''')
        
        c.execute("CREATE INDEX IF NOT EXISTS idx_users_points ON users(points DESC)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_subs_user ON active_subscriptions(user_id)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_subs_expires ON active_subscriptions(expires_at)")
        
        conn.commit()
        conn.close()
        logger.info("Database initialized successfully")
        return True
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
        return False

# ========== API HELPERS ==========
def api_request(method: str, payload: Dict = None, max_retries: int = 3) -> Optional[Any]:
    url = API_URL + method
    data = json.dumps(payload).encode("utf-8") if payload else None
    
    for attempt in range(max_retries):
        try:
            req = urllib.request.Request(
                url, 
                data=data, 
                headers={"Content-Type": "application/json"} if data else {},
                method="POST" if payload else "GET"
            )
            with urllib.request.urlopen(req, timeout=15) as res:
                result = json.loads(res.read())
                if result.get('ok'):
                    return result.get('result')
                else:
                    logger.warning(f"API error for {method}: {result.get('description', 'Unknown')}")
                    return None
        except urllib.error.URLError as e:
            logger.error(f"Network error (attempt {attempt+1}): {e}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
            else:
                return None
        except Exception as e:
            logger.error(f"Unexpected error in {method}: {e}")
            return None
    return None

def send_message(chat_id: int, text: str, buttons: List = None, parse_mode: str = "Markdown") -> bool:
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": parse_mode,
        "disable_web_page_preview": True
    }
    if buttons:
        payload["reply_markup"] = json.dumps({"inline_keyboard": buttons})
    result = api_request("sendMessage", payload)
    return result is not None

def edit_message(chat_id: int, message_id: int, text: str, buttons: List = None) -> bool:
    payload = {
        "chat_id": chat_id,
        "message_id": message_id,
        "text": text,
        "parse_mode": "Markdown"
    }
    if buttons:
        payload["reply_markup"] = json.dumps({"inline_keyboard": buttons})
    result = api_request("editMessageText", payload)
    return result is not None

def get_chat_member(chat_id: str, user_id: int) -> bool:
    try:
        result = api_request("getChatMember", {"chat_id": chat_id, "user_id": user_id})
        if result:
            status = result.get('status')
            return status in ['member', 'administrator', 'creator']
    except Exception as e:
        logger.error(f"getChatMember error: {e}")
    return False

def check_bot_admin(chat_id: str) -> bool:
    try:
        bot_info = api_request("getMe")
        if bot_info:
            bot_id = bot_info['id']
            return get_chat_member(chat_id, bot_id)
    except Exception as e:
        logger.error(f"check_bot_admin error: {e}")
    return False

# ========== USER MANAGEMENT ==========
def generate_refer_code() -> str:
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))

@with_db_retry()
def register_user(user_id: int, username: str, refer_code: str = None) -> bool:
    try:
        conn = sqlite3.connect('bot_data.db', timeout=10)
        c = conn.cursor()
        c.execute("SELECT * FROM users WHERE user_id=?", (user_id,))
        if not c.fetchone():
            code = generate_refer_code()
            now = datetime.now()
            c.execute("""INSERT INTO users 
                         (user_id, username, refer_code, joined_date, last_active) 
                         VALUES (?,?,?,?,?)""",
                      (user_id, username, code, now, now))
            
            if refer_code:
                c.execute("SELECT user_id FROM users WHERE refer_code=?", (refer_code,))
                referrer = c.fetchone()
                if referrer and referrer[0] != user_id:
                    add_points_with_subscription(
                        referrer[0], 50, "referral", str(user_id), 
                        f"Referral: {username}"
                    )
                    send_message(referrer[0], f" New user @{username} joined! +50 points")
            
            conn.commit()
            logger.info(f"New user registered: {user_id} (@{username})")
        else:
            c.execute("UPDATE users SET last_active=? WHERE user_id=?", (datetime.now(), user_id))
            conn.commit()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"register_user error: {e}")
        return False

@with_db_retry()
def get_user_points(user_id: int) -> Tuple[int, int, int, int, int]:
    try:
        conn = sqlite3.connect('bot_data.db', timeout=10)
        c = conn.cursor()
        c.execute("""SELECT points, total_earned, total_spent, total_purchased, total_penalty 
                     FROM users WHERE user_id=?""", (user_id,))
        result = c.fetchone()
        conn.close()
        return result if result else (0, 0, 0, 0, 0)
    except Exception as e:
        logger.error(f"get_user_points error: {e}")
        return (0, 0, 0, 0, 0)

@with_db_retry()
def check_daily_bonus(user_id: int) -> Tuple[bool, int]:
    try:
        conn = sqlite3.connect('bot_data.db', timeout=10)
        c = conn.cursor()
        c.execute("SELECT last_daily FROM users WHERE user_id=?", (user_id,))
        result = c.fetchone()
        today = datetime.now().date()
        
        if result and result[0]:
            last_daily = datetime.fromisoformat(result[0]).date()
            if today > last_daily:
                c.execute("""UPDATE users SET points = points + ?, last_daily = ? WHERE user_id=?""", 
                          (DAILY_BONUS, datetime.now(), user_id))
                conn.commit()
                conn.close()
                return True, DAILY_BONUS
        elif not result or not result[0]:
            c.execute("""UPDATE users SET points = points + ?, last_daily = ? WHERE user_id=?""", 
                      (DAILY_BONUS, datetime.now(), user_id))
            conn.commit()
            conn.close()
            return True, DAILY_BONUS
        
        conn.close()
        return False, 0
    except Exception as e:
        logger.error(f"check_daily_bonus error: {e}")
        return False, 0

# ========== SUBSCRIPTION MANAGEMENT ==========
@with_db_retry()
def add_points_with_subscription(user_id: int, points: int, sub_type: str, 
                                  sub_id: str, sub_name: str) -> Tuple[bool, str]:
    try:
        conn = sqlite3.connect('bot_data.db', timeout=10)
        c = conn.cursor()
        
        c.execute("""SELECT id FROM active_subscriptions 
                     WHERE user_id=? AND subscription_type=? AND subscription_id=? AND is_active=1""",
                  (user_id, sub_type, sub_id))
        if c.fetchone():
            conn.close()
            return False, " You already have an active subscription to this!"
        
        expires_at = datetime.now() + timedelta(days=SUBSCRIPTION_DAYS)
        
        c.execute("UPDATE users SET points = points + ?, total_earned = total_earned + ? WHERE user_id=?", 
                  (points, points, user_id))
        
        c.execute("""INSERT INTO active_subscriptions 
                     (user_id, subscription_type, subscription_id, subscription_name, 
                      points_earned, subscribed_at, expires_at)
                     VALUES (?,?,?,?,?,?,?)""",
                  (user_id, sub_type, sub_id, sub_name, points, datetime.now(), expires_at))
        
        c.execute("""INSERT INTO task_history (user_id, task_type, task_id, points, completed_at) 
                     VALUES (?,?,?,?,?)""",
                  (user_id, sub_type, sub_id, points, datetime.now()))
        
        conn.commit()
        conn.close()
        
        logger.info(f"User {user_id} earned {points} points from {sub_type}: {sub_name}")
        return True, f" +{points} points earned!\n Stay subscribed for {SUBSCRIPTION_DAYS} days.\n Early exit = {PENALTY_PERCENT}% penalty!"
    except Exception as e:
        logger.error(f"add_points_with_subscription error: {e}")
        return False, " An error occurred. Please try again."

@with_db_retry()
def get_active_subscriptions(user_id: int) -> List[Dict]:
    try:
        conn = sqlite3.connect('bot_data.db', timeout=10)
        c = conn.cursor()
        c.execute("""SELECT subscription_type, subscription_name, points_earned, expires_at 
                     FROM active_subscriptions 
                     WHERE user_id=? AND is_active=1
                     ORDER BY expires_at ASC""", (user_id,))
        results = c.fetchall()
        conn.close()
        
        subscriptions = []
        for sub_type, sub_name, points, expires_at_str in results:
            expires_at = datetime.fromisoformat(expires_at_str)
            days_left = max(0, (expires_at - datetime.now()).days)
            subscriptions.append({
                "type": sub_type,
                "name": sub_name,
                "points": points,
                "expires_at": expires_at.strftime("%Y-%m-%d %H:%M"),
                "days_left": days_left
            })
        return subscriptions
    except Exception as e:
        logger.error(f"get_active_subscriptions error: {e}")
        return []

@with_db_retry()
def deactivate_subscription(sub_id: int):
    try:
        conn = sqlite3.connect('bot_data.db', timeout=10)
        c = conn.cursor()
        c.execute("UPDATE active_subscriptions SET is_active=0 WHERE id=?", (sub_id,))
        conn.commit()
        conn.close()
        logger.info(f"Subscription {sub_id} deactivated")
    except Exception as e:
        logger.error(f"deactivate_subscription error: {e}")

def apply_penalty(user_id: int, sub_id: int, points_earned: int):
    try:
        penalty_points = int(points_earned * PENALTY_PERCENT / 100)
        total_penalty = points_earned + penalty_points
        
        conn = sqlite3.connect('bot_data.db', timeout=10)
        c = conn.cursor()
        
        c.execute("SELECT points FROM users WHERE user_id=?", (user_id,))
        result = c.fetchone()
        if result:
            current_points = result[0]
            new_points = max(0, current_points - total_penalty)
            
            c.execute("""UPDATE users SET points = ?, total_penalty = total_penalty + ? WHERE user_id=?""", 
                      (new_points, total_penalty, user_id))
            c.execute("UPDATE active_subscriptions SET is_active=0 WHERE id=?", (sub_id,))
            conn.commit()
            
            logger.warning(f"Penalty applied to user {user_id}: {total_penalty} points deducted")
            
            send_message(user_id, f""" PENALTY NOTICE!

You left a subscribed channel/bot before {SUBSCRIPTION_DAYS} days.

 Points Earned: {points_earned}
 Penalty ({PENALTY_PERCENT}%): {penalty_points}
 Total Deducted: {total_penalty}

 Stay subscribed for the full period next time!""")
        
        conn.close()
    except Exception as e:
        logger.error(f"apply_penalty error: {e}")

def check_all_subscriptions():
    try:
        conn = sqlite3.connect('bot_data.db', timeout=10)
        c = conn.cursor()
        
        c.execute("""SELECT id, user_id, subscription_type, subscription_id, points_earned, expires_at 
                     FROM active_subscriptions WHERE is_active=1""")
        subscriptions = c.fetchall()
        conn.close()
        
        violations = []
        
        for sub_id, user_id, sub_type, sub_id_value, points_earned, expires_at_str in subscriptions:
            try:
                expires_at = datetime.fromisoformat(expires_at_str)
                if datetime.now() > expires_at:
                    deactivate_subscription(sub_id)
                    continue
                
                still_subscribed = False
                
                if sub_type in ["channel", "group"]:
                    still_subscribed = get_chat_member(sub_id_value, user_id)
                elif sub_type == "bot_admin":
                    still_subscribed = check_bot_admin(sub_id_value)
                elif sub_type == "required_channel":
                    still_subscribed = get_chat_member(REQUIRED_CHANNEL, user_id)
                elif sub_type == "referral":
                    still_subscribed = True
                
                if not still_subscribed and sub_type != "referral":
                    violations.append((sub_id, user_id, points_earned))
                    
            except Exception as e:
                logger.error(f"Error checking subscription {sub_id}: {e}")
        
        for sub_id, user_id, points_earned in violations:
            apply_penalty(user_id, sub_id, points_earned)
            
    except Exception as e:
        logger.error(f"check_all_subscriptions error: {e}")

def subscription_monitor_thread():
    while True:
        time.sleep(3600)
        check_all_subscriptions()

# ========== TASKS ==========
def get_available_tasks(user_id: int) -> List[Dict]:
    tasks = []
    
    if REQUIRED_CHANNEL and not get_chat_member(REQUIRED_CHANNEL, user_id):
        tasks.append({
            "type": "required_channel",
            "id": REQUIRED_CHANNEL,
            "name": " MANDATORY: Official Channel",
            "points": 50,
            "required": True,
            "link": REQUIRED_CHANNEL_LINK or f"https://t.me/{REQUIRED_CHANNEL.replace('@', '')}"
        })
        return tasks
    
    sample_channels = [
        {"id": "@tech_news", "name": "Tech News", "points": 20},
        {"id": "@crypto_updates", "name": "Crypto Updates", "points": 25},
    ]
    
    for ch in sample_channels:
        if not get_chat_member(ch['id'], user_id):
            tasks.append({
                "type": "channel",
                "id": ch['id'],
                "name": ch['name'],
                "points": ch['points'],
                "required": False,
                "link": f"https://t.me/{ch['id'].replace('@', '')}"
            })
    
    tasks.append({
        "type": "bot_admin",
        "id": "bot_admin_task",
        "name": " Add Bot as Admin",
        "points": 100,
        "required": False,
        "description": f""" Bot Admin Task

To earn 100 points:
1. Create a group or channel
2. Add @{BOT_USERNAME} as an ADMIN
3. Send the group/channel link here

 Keep bot as admin for {SUBSCRIPTION_DAYS} days!
 Reward: 100 points"""
    })
    
    return tasks

# ========== PROMOTION SYSTEM ==========
@with_db_retry()
def create_promotion(user_id: int, content_type: str, content_id: str, title: str) -> Tuple[bool, str]:
    cost = PROMOTION_COSTS.get(content_type, 500)
    
    points, _, _, _, _ = get_user_points(user_id)
    if points < cost:
        return False, f" You need {cost} points to promote this content!"
    
    conn = sqlite3.connect('bot_data.db', timeout=10)
    c = conn.cursor()
    
    c.execute("UPDATE users SET points = points - ?, total_spent = total_spent + ? WHERE user_id=?", 
              (cost, cost, user_id))
    
    c.execute("""INSERT INTO campaigns 
                 (owner_id, content_type, content_id, title, required_points, created_at) 
                 VALUES (?,?,?,?,?,?)""",
              (user_id, content_type, content_id, title, cost, datetime.now()))
    campaign_id = c.lastrowid
    conn.commit()
    conn.close()
    
    logger.info(f"User {user_id} created promotion: {title}")
    return True, f" Promotion created!\n Cost: {cost} points\n Title: {title}"

@with_db_retry()
def get_promotion_tasks(user_id: int) -> List[Dict]:
    try:
        conn = sqlite3.connect('bot_data.db', timeout=10)
        c = conn.cursor()
        
        c.execute("""SELECT c.id, c.content_type, c.content_id, c.title, c.required_points
                     FROM campaigns c
                     LEFT JOIN promotion_views pv ON c.id = pv.campaign_id AND pv.user_id = ?
                     WHERE c.status = 'active' AND pv.id IS NULL
                     ORDER BY c.created_at DESC LIMIT 10""", (user_id,))
        results = c.fetchall()
        conn.close()
        
        promotions = []
        for camp_id, content_type, content_id, title, points in results:
            promotions.append({
                "id": camp_id,
                "type": content_type,
                "content_id": content_id,
                "title": title,
                "points": points
            })
        return promotions
    except Exception as e:
        logger.error(f"get_promotion_tasks error: {e}")
        return []

@with_db_retry()
def mark_promotion_viewed(user_id: int, campaign_id: int) -> int:
    try:
        conn = sqlite3.connect('bot_data.db', timeout=10)
        c = conn.cursor()
        
        c.execute("SELECT id FROM promotion_views WHERE campaign_id=? AND user_id=?", 
                  (campaign_id, user_id))
        if c.fetchone():
            conn.close()
            return 0
        
        c.execute("SELECT required_points FROM campaigns WHERE id=?", (campaign_id,))
        points_row = c.fetchone()
        if not points_row:
            conn.close()
            return 0
        
        points = points_row[0]
        
        c.execute("INSERT INTO promotion_views (campaign_id, user_id, viewed_at) VALUES (?,?,?)",
                  (campaign_id, user_id, datetime.now()))
        c.execute("UPDATE campaigns SET total_clicks = total_clicks + 1 WHERE id=?", (campaign_id,))
        c.execute("UPDATE users SET points = points + ?, total_earned = total_earned + ? WHERE user_id=?", 
                  (points, points, user_id))
        
        conn.commit()
        conn.close()
        
        logger.info(f"User {user_id} earned {points} points from promotion {campaign_id}")
        return points
    except Exception as e:
        logger.error(f"mark_promotion_viewed error: {e}")
        return 0

# ========== PAYMENT SYSTEM ==========
def create_invoice_link(user_id: int, pack_id: str) -> Optional[str]:
    pack = PRICE_PACKS.get(pack_id)
    if not pack:
        return None
    
    payload = {
        "title": pack['name'],
        "description": f"Get {pack['points']} points instantly!",
        "payload": json.dumps({
            "user_id": user_id,
            "points": pack['points'],
            "pack_id": pack_id,
            "timestamp": time.time()
        }),
        "provider_token": "",
        "currency": "XTR",
        "prices": [{"label": pack['name'], "amount": pack['stars']}]
    }
    
    return api_request("createInvoiceLink", payload)

def answer_pre_checkout_query(query_id: str) -> bool:
    result = api_request("answerPreCheckoutQuery", {"pre_checkout_query_id": query_id, "ok": True})
    return result is not None

@with_db_retry()
def handle_successful_payment(user_id: int, payload: str) -> bool:
    try:
        data = json.loads(payload)
        points_to_add = data.get("points", 0)
        
        conn = sqlite3.connect('bot_data.db', timeout=10)
        c = conn.cursor()
        c.execute("""UPDATE users SET points = points + ?, total_purchased = total_purchased + ? WHERE user_id=?""", 
                  (points_to_add, points_to_add, user_id))
        conn.commit()
        conn.close()
        
        logger.info(f"User {user_id} purchased {points_to_add} points")
        return True
    except Exception as e:
        logger.error(f"handle_successful_payment error: {e}")
        return False

# ========== ADMIN FUNCTIONS ==========
def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS

@with_db_retry()
def get_bot_stats() -> Dict:
    try:
        conn = sqlite3.connect('bot_data.db', timeout=10)
        c = conn.cursor()
        
        c.execute("SELECT COUNT(*) FROM users")
        total_users = c.fetchone()[0]
        
        c.execute("SELECT SUM(points) FROM users")
        total_points = c.fetchone()[0] or 0
        
        c.execute("SELECT COUNT(*) FROM active_subscriptions WHERE is_active=1")
        active_subs = c.fetchone()[0]
        
        c.execute("SELECT COUNT(*) FROM campaigns WHERE status='active'")
        active_promos = c.fetchone()[0]
        
        c.execute("SELECT SUM(total_earned) FROM users")
        total_earned = c.fetchone()[0] or 0
        
        conn.close()
        
        return {
            "total_users": total_users,
            "total_points": total_points,
            "active_subs": active_subs,
            "active_promos": active_promos,
            "total_earned": total_earned
        }
    except Exception as e:
        logger.error(f"get_bot_stats error: {e}")
        return {}

def broadcast_message(message: str, admin_chat_id: int):
    try:
        conn = sqlite3.connect('bot_data.db', timeout=10)
        c = conn.cursor()
        c.execute("SELECT user_id FROM users")
        users = c.fetchall()
        conn.close()
        
        success = 0
        failed = 0
        
        for user in users:
            if send_message(user[0], f" Announcement\n\n{message}"):
                success += 1
            else:
                failed += 1
            time.sleep(0.05)
        
        send_message(admin_chat_id, f" Broadcast complete!\n Sent: {success}\n Failed: {failed}")
        logger.info(f"Broadcast sent to {success} users")
    except Exception as e:
        logger.error(f"broadcast_message error: {e}")

def create_backup(chat_id: int):
    try:
        backup_name = f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
        shutil.copy('bot_data.db', backup_name)
        send_message(chat_id, f" Backup created: `{backup_name}`")
        logger.info(f"Backup created: {backup_name}")
    except Exception as e:
        logger.error(f"create_backup error: {e}")

def handle_admin_commands(message: Dict) -> bool:
    user_id = message["from"]["id"]
    if not is_admin(user_id):
        return False
    
    chat_id = message["chat"]["id"]
    text = message.get("text", "")
    
    if text == "/stats":
        stats = get_bot_stats()
        stats_text = f""" Bot Statistics

 Total Users: {stats.get('total_users', 0)}
 Total Points: {stats.get('total_points', 0)}
 Total Earned: {stats.get('total_earned', 0)}
 Active Subs: {stats.get('active_subs', 0)}
 Active Promos: {stats.get('active_promos', 0)}"""
        send_message(chat_id, stats_text)
        return True
    
    elif text.startswith("/broadcast "):
        msg = text.replace("/broadcast", "").strip()
        if msg:
            broadcast_message(msg, chat_id)
        return True
    
    elif text == "/backup":
        create_backup(chat_id)
        return True
    
    elif text == "/adminhelp":
        help_text = """ Admin Commands

/stats - Show bot statistics
/broadcast <message> - Send message to all users
/backup - Create database backup
/adminhelp - Show this help"""
        send_message(chat_id, help_text)
        return True
    
    return False

# ========== COMMAND HANDLERS ==========
def get_main_menu(user_id: int) -> List:
    return [
        [{"text": " Tasks", "callback_data": "tasks"}],
        [{"text": " View Promotions", "callback_data": "view_promotions"}],
        [{"text": " Promote Content", "callback_data": "promote_menu"}],
        [{"text": " My Subscriptions", "callback_data": "my_subscriptions"}],
        [{"text": " My Points", "callback_data": "my_points"}],
        [{"text": " Buy Points", "callback_data": "buy_points"}],
        [{"text": " Referral", "callback_data": "referral"}],
        [{"text": " Leaderboard", "callback_data": "leaderboard"}]
    ]

@rate_limit
def handle_command(update: Dict):
    message = update.get("message", {})
    chat_id = message["chat"]["id"]
    user_id = message["from"]["id"]
    username = message["from"].get("username", "Unknown")
    text = message.get("text", "")
    
    refer_code = None
    if text.startswith("/start") and len(text.split()) > 1:
        refer_code = text.split()[1]
    
    if not register_user(user_id, username, refer_code):
        send_message(chat_id, " Database error. Please try again.")
        return
    
    got_bonus, bonus_amount = check_daily_bonus(user_id)
    bonus_msg = f"\n\n Daily Bonus: +{bonus_amount} points!" if got_bonus else ""
    
    if REQUIRED_CHANNEL and not get_chat_member(REQUIRED_CHANNEL, user_id):
        force_join_buttons = [
            [{"text": " Join Required Channel", "url": REQUIRED_CHANNEL_LINK or f"https://t.me/{REQUIRED_CHANNEL.replace('@', '')}"}],
            [{"text": " I've Joined", "callback_data": "check_required_channel"}]
        ]
        send_message(chat_id, f" ACTION REQUIRED!\n\nYou must join our official channel to use this bot.\n\n {REQUIRED_CHANNEL}", force_join_buttons)
        return
    
    if text == "/start" or text == f"/start@{BOT_USERNAME}":
        points, earned, spent, purchased, penalty = get_user_points(user_id)
        welcome_msg = f""" Welcome to PromoBot!

 Points: *{points}*
 Earned: {earned} | Spent: {spent}
 Purchased: {purchased} | Penalties: {penalty}

 Stay subscribed for {SUBSCRIPTION_DAYS} days!
 Early exit = {PENALTY_PERCENT}% penalty{bonus_msg}"""
        
        send_message(chat_id, welcome_msg, get_main_menu(user_id))
    
    elif text == "/points":
        points, earned, spent, purchased, penalty = get_user_points(user_id)
        send_message(chat_id, f" Points Dashboard\n\n Available: *{points}*\n Task Earnings: {earned}\n Purchased: {purchased}\n Spent: {spent}\n Penalties: {penalty}")
    
    elif text == "/help":
        help_text = f""" PromoBot Help

 Commands:
/start - Start the bot
/points - Check your points
/help - Show this help

 How it works:
1. Complete tasks (join channels/groups)
2. Earn points
3. Use points to promote your content

 Rules:
- Stay subscribed for {SUBSCRIPTION_DAYS} days
- Leaving early = {PENALTY_PERCENT}% penalty
- Daily bonus resets every 24 hours"""
        send_message(chat_id, help_text)

@rate_limit
def handle_callback(callback: Dict):
    chat_id = callback["message"]["chat"]["id"]
    user_id = callback["from"]["id"]
    data = callback["data"]
    message_id = callback["message"]["message_id"]
    
    if REQUIRED_CHANNEL and not get_chat_member(REQUIRED_CHANNEL, user_id) and data not in ["check_required_channel", "my_subscriptions"]:
        force_join_buttons = [
            [{"text": " Join Required Channel", "url": REQUIRED_CHANNEL_LINK or f"https://t.me/{REQUIRED_CHANNEL.replace('@', '')}"}],
            [{"text": " I've Joined", "callback_data": "check_required_channel"}]
        ]
        edit_message(chat_id, message_id, f" Please join our official channel first!\n\n {REQUIRED_CHANNEL}", force_join_buttons)
        return
    
    if data == "check_required_channel":
        if get_chat_member(REQUIRED_CHANNEL, user_id):
            add_points_with_subscription(user_id, 50, "required_channel", REQUIRED_CHANNEL, "Official Channel")
            points, _, _, _, _ = get_user_points(user_id)
            edit_message(chat_id, message_id, f" Verification successful!\n\n Your points: {points}", get_main_menu(user_id))
        else:
            edit_message(chat_id, message_id, " Verification failed! Please join the channel first.")
    
    elif data == "tasks":
        tasks = get_available_tasks(user_id)
        if not tasks:
            edit_message(chat_id, message_id, " No tasks available right now!")
            return
        
        for task in tasks:
            if task['type'] == "required_channel":
                buttons = [[{"text": " Join Now", "url": task['link']}],
                          [{"text": " Verify", "callback_data": f"verify_{task['type']}_{task['id']}"}]]
                send_message(chat_id, f" {task['name']}\n Points: {task['points']}\n\n Stay subscribed for {SUBSCRIPTION_DAYS} days!", buttons)
            elif task['type'] == "bot_admin":
                buttons = [[{"text": " Verify Bot Admin", "callback_data": "verify_bot_admin"}]]
                send_message(chat_id, task['description'], buttons)
            else:
                buttons = [[{"text": " Join", "url": task['link']}],
                          [{"text": " Verify", "callback_data": f"verify_{task['type']}_{task['id']}"}]]
                send_message(chat_id, f" {task['name']}\n Points: {task['points']}\n\n1. Join\n2. Click Verify\n\n Stay subscribed for {SUBSCRIPTION_DAYS} days!", buttons)
        
        edit_message(chat_id, message_id, " Tasks sent above!")
    
    elif data.startswith("verify_"):
        parts = data.split("_")
        if len(parts) >= 3:
            task_type = parts[1]
            task_id = "_".join(parts[2:]) if len(parts) > 3 else parts[2]
            
            verified = False
            points = 0
            sub_name = ""
            
            if task_type == "required_channel":
                verified = get_chat_member(REQUIRED_CHANNEL, user_id)
                points = 50
                sub_name = "Official Channel"
            elif task_type == "channel":
                verified = get_chat_member(task_id, user_id)
                points = 20
                sub_name = task_id
            elif task_type == "group":
                verified = get_chat_member(task_id, user_id)
                points = 30
                sub_name = task_id
            
            if verified:
                result, msg = add_points_with_subscription(user_id, points, task_type, task_id, sub_name)
                send_message(chat_id, msg)
            else:
                send_message(chat_id, " Verification failed! Please complete the task first.")
    
    elif data == "verify_bot_admin":
        send_message(chat_id, f""" Bot Admin Verification

To complete this task:
1. Create a group or channel
2. Add @{BOT_USERNAME} as an ADMIN
3. Send the group/channel link here

 Keep bot as admin for {SUBSCRIPTION_DAYS} days!
 Reward: 100 points""")
    
    elif data == "my_subscriptions":
        subscriptions = get_active_subscriptions(user_id)
        if not subscriptions:
            edit_message(chat_id, message_id, " No Active Subscriptions\n\nComplete some tasks to get started!")
            return
        
        msg = " Your Active Subscriptions\n\n"
        for sub in subscriptions:
            status_emoji = "" if sub['days_left'] > 0 else ""
            msg += f"{status_emoji} *{sub['name']}*\n"
            msg += f"    Expires: {sub['expires_at']}\n"
            msg += f"    {sub['days_left']} days left\n"
            msg += f"    Points Earned: {sub['points']}\n\n"
        
        msg += f"\n Leaving early = {PENALTY_PERCENT}% penalty!"
        edit_message(chat_id, message_id, msg)
    
    elif data == "my_points":
        points, earned, spent, purchased, penalty = get_user_points(user_id)
        edit_message(chat_id, message_id, f" Points Dashboard\n\n Available: *{points}*\n Task Earnings: {earned}\n Purchased: {purchased}\n Spent: {spent}\n Penalties: {penalty}")
    
    elif data == "buy_points":
        buttons = []
        for pack_id, pack in PRICE_PACKS.items():
            buttons.append([{"text": f" {pack['stars']} Stars  {pack['points']} Points", "callback_data": f"buy_{pack_id}"}])
        edit_message(chat_id, message_id, f" Buy Points with Telegram Stars\n\nSelect a package.\n\n Current balance: {get_user_points(user_id)[0]} points", buttons)
    
    elif data.startswith("buy_"):
        pack_id = data.replace("buy_", "")
        pack = PRICE_PACKS.get(pack_id)
        if pack:
            invoice_link = create_invoice_link(user_id, pack_id)
            if invoice_link:
                buttons = [[{"text": f" Pay {pack['stars']} Stars", "url": invoice_link}]]
                send_message(chat_id, f" *{pack['name']}*\n Price: {pack['stars']} Stars\n You get: {pack['points']} points", buttons)
            else:
                send_message(chat_id, " Payment system error. Please try again later.")
    
    elif data == "referral":
        conn = sqlite3.connect('bot_data.db', timeout=10)
        c = conn.cursor()
        c.execute("SELECT refer_code FROM users WHERE user_id=?", (user_id,))
        ref_code = c.fetchone()
        conn.close()
        
        if ref_code:
            ref_link = f"https://t.me/{BOT_USERNAME}?start={ref_code[0]}"
            edit_message(chat_id, message_id, f" Referral Program\n\nInvite friends & earn *50 points* each!\n\n Your link: `{ref_link}`")
    
    elif data == "leaderboard":
        conn = sqlite3.connect('bot_data.db', timeout=10)
        c = conn.cursor()
        c.execute("SELECT username, points FROM users ORDER BY points DESC LIMIT 10")
        top_users = c.fetchall()
        conn.close()
        
        lb_text = " Top 10 Users \n\n"
        for i, (username, points) in enumerate(top_users, 1):
            lb_text += f"{i}. @{username}  {points} pts\n"
        edit_message(chat_id, message_id, lb_text)
    
    elif data == "promote_menu":
        buttons = [
            [{"text": " Promote Channel (500 pts)", "callback_data": "promote_channel"}],
            [{"text": " Promote Bot (300 pts)", "callback_data": "promote_bot"}],
            [{"text": " Promote Group (400 pts)", "callback_data": "promote_group"}],
            [{"text": " Promote Post (100 pts)", "callback_data": "promote_post"}]
        ]
        edit_message(chat_id, message_id, " Promote Your Content\n\nSelect what you want to promote:\n\n Costs:\n Channel: 500 points\n Bot: 300 points\n Group: 400 points\n Post: 100 points", buttons)
    
    elif data in ["promote_channel", "promote_bot", "promote_group", "promote_post"]:
        promo_type = data.replace("promote_", "")
        type_names = {"channel": "channel", "bot": "bot", "group": "group", "post": "post"}
        
        conn = sqlite3.connect('bot_data.db', timeout=10)
        c = conn.cursor()
        c.execute("INSERT OR REPLACE INTO promo_waiting (user_id, promo_type, created_at) VALUES (?,?,?)",
                  (user_id, type_names.get(promo_type, promo_type), datetime.now()))
        conn.commit()
        conn.close()
        
        instructions = {
            "channel": "Send me your channel username or invite link.\n\nExample: @yourchannel or https://t.me/yourchannel",
            "bot": "Send me your bot username.\n\nExample: @yourbot",
            "group": "Send me your group username or invite link.\n\nExample: @yourgroup or https://t.me/yourgroup",
            "post": "Send me the post link.\n\nExample: https://t.me/yourchannel/123"
        }
        
        edit_message(chat_id, message_id, f" Promote Your {promo_type.capitalize()}\n\n{instructions.get(promo_type, '')}\n\nAlso send a short title/description.")
    
    elif data == "view_promotions":
        promotions = get_promotion_tasks(user_id)
        if not promotions:
            edit_message(chat_id, message_id, " No Promotions Available\n\nCheck back later or create your own promotion!")
            return
        
        for promo in promotions:
            buttons = [[{"text": " View & Earn Points", "callback_data": f"view_promo_{promo['id']}"}]]
            promo_text = f" Sponsored Content\n\n *{promo['title']}*\n {promo['content_id']}\n Reward: {promo['points']} points"
            send_message(chat_id, promo_text, buttons)
        
        edit_message(chat_id, message_id, " Promotions sent above!")
    
    elif data.startswith("view_promo_"):
        campaign_id = int(data.replace("view_promo_", ""))
        points = mark_promotion_viewed(user_id, campaign_id)
        if points > 0:
            send_message(chat_id, f" +{points} points earned for viewing this promotion!")
        else:
            send_message(chat_id, " You've already viewed this promotion.")

@rate_limit
def handle_text_message(update: Dict):
    message = update.get("message", {})
    chat_id = message["chat"]["id"]
    user_id = message["from"]["id"]
    text = message.get("text", "")
    
    conn = sqlite3.connect('bot_data.db', timeout=10)
    c = conn.cursor()
    c.execute("SELECT promo_type FROM promo_waiting WHERE user_id=?", (user_id,))
    waiting = c.fetchone()
    
    if waiting:
        promo_type = waiting[0]
        c.execute("DELETE FROM promo_waiting WHERE user_id=?", (user_id,))
        conn.commit()
        conn.close()
        
        conn2 = sqlite3.connect('bot_data.db', timeout=10)
        c2 = conn2.cursor()
        c2.execute("INSERT OR REPLACE INTO promo_temp (user_id, promo_type, content_id, created_at) VALUES (?,?,?,?)",
                   (user_id, promo_type, text, datetime.now()))
        conn2.commit()
        conn2.close()
        
        send_message(chat_id, " Now send a short title/description (max 100 chars):")
        return
    
    c.execute("SELECT promo_type, content_id FROM promo_temp WHERE user_id=?", (user_id,))
    temp_data = c.fetchone()
    
    if temp_data:
        promo_type, content_id = temp_data
        c.execute("DELETE FROM promo_temp WHERE user_id=?", (user_id,))
        conn.commit()
        conn.close()
        
        title = text[:100]
        result, msg = create_promotion(user_id, promo_type, content_id, title)
        send_message(chat_id, msg)
        return
    
    conn.close()
    
    if (text.startswith("https://t.me/") or text.startswith("@") or text.startswith("t.me/")) and "join" not in text.lower():
        chat_username = text.replace("https://t.me/", "").replace("t.me/", "").split("/")[0].split("?")[0]
        if chat_username.startswith("@"):
            chat_username = chat_username[1:]
        
        if check_bot_admin(chat_username):
            result, msg = add_points_with_subscription(user_id, 100, "bot_admin", chat_username, f"Group: {chat_username}")
            send_message(chat_id, f" Bot admin verified in @{chat_username}!\n\n{msg}")
        else:
            send_message(chat_id, f" Bot is not admin in @{chat_username}.\nPlease add @{BOT_USERNAME} as admin first.")

def handle_pre_checkout_query(update: Dict):
    query = update.get("pre_checkout_query")
    if query:
        answer_pre_checkout_query(query["id"])

def handle_successful_payment_message(message: Dict):
    user_id = message["from"]["id"]
    payment = message.get("successful_payment")
    
    if payment:
        payload = payment.get("invoice_payload", "{}")
        if handle_successful_payment(user_id, payload):
            points, _, _, _, _ = get_user_points(user_id)
            send_message(user_id, f" Payment Successful!\n\n Points added!\n New balance: {points} points")

# ========== MAIN LOOP ==========
def run_polling():
    last_update_id = None
    logger.info("Bot started in POLLING mode")
    
    while True:
        try:
            payload = {"timeout": 30}
            if last_update_id:
                payload["offset"] = last_update_id + 1
            
            result = api_request("getUpdates", payload)
            
            if result:
                for update in result:
                    last_update_id = update["update_id"]
                    
                    try:
                        if "message" in update:
                            msg = update["message"]
                            if "text" in msg:
                                if not handle_admin_commands(msg):
                                    handle_command(update)
                                    handle_text_message(update)
                            elif "successful_payment" in msg:
                                handle_successful_payment_message(msg)
                        
                        elif "callback_query" in update:
                            handle_callback(update["callback_query"])
                        
                        elif "pre_checkout_query" in update:
                            handle_pre_checkout_query(update)
                    
                    except Exception as e:
                        logger.error(f"Error processing update: {e}")
                        continue
            
            time.sleep(0.5)
        
        except KeyboardInterrupt:
            logger.info("Bot stopped by user")
            break
        except Exception as e:
            logger.error(f"Polling error: {e}")
            time.sleep(5)

def main():
    if not init_database():
        logger.error("Failed to initialize database. Exiting...")
        sys.exit(1)
    
    monitor_thread = threading.Thread(target=subscription_monitor_thread, daemon=True)
    monitor_thread.start()
    
    def signal_handler(sig, frame):
        logger.info("Shutting down gracefully...")
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    run_polling()

if __name__ == "__main__":
    main()
