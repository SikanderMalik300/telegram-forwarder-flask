from flask import Flask, render_template, request, jsonify, send_file
from flask_sqlalchemy import SQLAlchemy
from datetime import datetime
import os
import asyncio
import json
import tempfile
import threading
import time
import logging
import traceback

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Database configuration for Railway/Render (PostgreSQL)
# For local development, use SQLite
try:
    if os.environ.get('DATABASE_URL'):
        database_url = os.environ.get('DATABASE_URL')
        # Fix for SQLAlchemy 1.4+
        if database_url.startswith('postgres://'):
            database_url = database_url.replace('postgres://', 'postgresql://')
        app.config['SQLALCHEMY_DATABASE_URI'] = database_url
        logger.info("Using PostgreSQL database from Railway")
    else:
        app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///telegram_forwarder.db'
        logger.info("Using SQLite database for local development")
except Exception as e:
    logger.error(f"Database configuration error: {e}")
    app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///telegram_forwarder.db'

app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'your-secret-key-here')

# Initialize database with error handling
try:
    db = SQLAlchemy(app)
    logger.info("SQLAlchemy initialized successfully")
except Exception as e:
    logger.error(f"SQLAlchemy initialization error: {e}")
    raise

# Database Models
class Account(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    api_id = db.Column(db.String(50), nullable=False)
    api_hash = db.Column(db.String(100), nullable=False)
    phone_number = db.Column(db.String(20), nullable=False)
    session_string = db.Column(db.Text)
    is_authenticated = db.Column(db.Boolean, default=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class ForwardingRule(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    account_id = db.Column(db.Integer, db.ForeignKey('account.id'), nullable=False)
    name = db.Column(db.String(100), nullable=False)
    source_chat_id = db.Column(db.BigInteger, nullable=False)
    destination_chat_id = db.Column(db.BigInteger, nullable=False)
    keywords = db.Column(db.Text)  # JSON string
    is_active = db.Column(db.Boolean, default=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    account = db.relationship('Account', backref=db.backref('forwarding_rules', lazy=True))

class ChatHistory(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    account_id = db.Column(db.Integer, db.ForeignKey('account.id'), nullable=False)
    chat_id = db.Column(db.BigInteger, nullable=False)
    chat_title = db.Column(db.String(200), nullable=False)
    file_path = db.Column(db.String(500), nullable=False)
    message_count = db.Column(db.Integer, default=0)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    
    account = db.relationship('Account', backref=db.backref('chat_histories', lazy=True))

# Import Telegram modules with better error handling
try:
    from telethon.sync import TelegramClient
    from telethon import errors
    from telethon.sessions import StringSession
    TELEGRAM_AVAILABLE = True
    logger.info("Telegram libraries imported successfully")
except ImportError as e:
    logger.error(f"Telegram import failed: {e}")
    TELEGRAM_AVAILABLE = False

# Global storage for active connections and forwarders
active_connections = {}
active_forwarders = {}

class TelegramService:
    def __init__(self, api_id, api_hash, phone_number, session_string=""):
        if not TELEGRAM_AVAILABLE:
            raise Exception("Telegram client not available")
        
        self.api_id = int(api_id)
        self.api_hash = api_hash
        self.phone_number = phone_number
        self.session = StringSession(session_string)
        self.client = None
        self.is_connected = False

    def _create_client(self):
        """Create client in the same thread where it will be used"""
        self.client = TelegramClient(self.session, self.api_id, self.api_hash)

    async def connect(self):
        try:
            if not self.client:
                self._create_client()
                
            await self.client.connect()
            
            if await self.client.is_user_authorized():
                self.is_connected = True
                logger.info(f"Connected to Telegram for {self.phone_number}")
                return True
            else:
                logger.warning(f"User not authorized for {self.phone_number}")
                raise Exception("Account not authenticated. Please authenticate this account first using a session string.")
                
        except Exception as e:
            logger.error(f"Connection error: {e}")
            raise Exception(f"Failed to connect: {str(e)}")

    async def send_code_request(self):
        """Send OTP code to phone number"""
        try:
            if not self.client:
                self._create_client()
            
            await self.client.connect()
            result = await self.client.send_code_request(self.phone_number)
            return result.phone_code_hash
        except Exception as e:
            logger.error(f"Error sending code: {e}")
            raise

    async def verify_code(self, code, phone_code_hash, password=None):
        """Verify OTP code and complete authentication"""
        try:
            if not self.client:
                self._create_client()
            
            await self.client.connect()
            
            try:
                user = await self.client.sign_in(self.phone_number, code, phone_code_hash=phone_code_hash)
            except errors.SessionPasswordNeededError:
                if password:
                    user = await self.client.sign_in(password=password)
                else:
                    raise Exception("Two-factor authentication enabled. Password required.")
            
            # Get session string for future use
            session_string = self.client.session.save()
            self.is_connected = True
            
            return {
                'success': True,
                'session_string': session_string,
                'user_id': user.id,
                'first_name': user.first_name
            }
            
        except Exception as e:
            logger.error(f"Error verifying code: {e}")
            raise

    def get_session_string(self):
        return self.session.save() if self.session else ''

    async def disconnect(self):
        if self.client and self.client.is_connected():
            await self.client.disconnect()
        self.is_connected = False

def run_async_safely(coro):
    """Run async function safely in Flask context"""
    try:
        # Try to get existing event loop
        loop = asyncio.get_event_loop()
        if loop.is_running():
            # If loop is running, create a new one in a thread
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(asyncio.run, coro)
                return future.result(timeout=30)
        else:
            return loop.run_until_complete(coro)
    except RuntimeError:
        # No event loop, create a new one
        return asyncio.run(coro)

# Error handlers
@app.errorhandler(404)
def not_found_error(error):
    logger.warning(f"404 error: {request.url}")
    return jsonify({'error': 'Endpoint not found'}), 404

@app.errorhandler(500)
def internal_error(error):
    logger.error(f"Internal server error: {error}")
    logger.error(f"Traceback: {traceback.format_exc()}")
    return jsonify({'error': 'Internal server error', 'details': str(error)}), 500

@app.errorhandler(Exception)
def handle_exception(e):
    logger.error(f"Unhandled exception: {e}")
    logger.error(f"Traceback: {traceback.format_exc()}")
    return jsonify({'error': 'An unexpected error occurred', 'details': str(e)}), 500

# Request logging
@app.before_request
def log_request_info():
    logger.info(f"Request: {request.method} {request.url}")

# Routes
@app.route('/')
def dashboard():
    return render_template('dashboard.html')

@app.route('/accounts')
def accounts():
    return render_template('accounts.html')

@app.route('/chats')
def chats():
    return render_template('chats.html')

@app.route('/forwarding')
def forwarding():
    return render_template('forwarding.html')

@app.route('/settings')
def settings():
    return render_template('settings.html')

# Test route
@app.route('/api/test', methods=['GET'])
def api_test():
    try:
        return jsonify({
            'success': True,
            'message': 'API is working correctly',
            'timestamp': datetime.utcnow().isoformat(),
            'telegram_available': TELEGRAM_AVAILABLE,
            'database_connected': True
        })
    except Exception as e:
        logger.error(f"Test route error: {e}")
        return jsonify({'error': str(e)}), 500

# API Routes
@app.route('/api/accounts', methods=['GET', 'POST'])
def api_accounts():
    try:
        if request.method == 'GET':
            logger.info("Fetching accounts from database")
            accounts = Account.query.order_by(Account.created_at.desc()).all()
            logger.info(f"Found {len(accounts)} accounts")
            
            result = []
            for acc in accounts:
                try:
                    result.append({
                        'id': acc.id,
                        'name': acc.name,
                        'api_id': acc.api_id,
                        'api_hash': acc.api_hash,
                        'phone_number': acc.phone_number,
                        'is_authenticated': acc.is_authenticated or False,
                        'created_at': acc.created_at.isoformat(),
                        'updated_at': acc.updated_at.isoformat()
                    })
                except Exception as e:
                    logger.error(f"Error processing account {acc.id}: {e}")
                    continue
            
            return jsonify(result)
        
        elif request.method == 'POST':
            data = request.json
            logger.info(f"Creating new account: {data.get('name')}")
            
            account = Account(
                name=data['name'],
                api_id=data['api_id'],
                api_hash=data['api_hash'],
                phone_number=data['phone_number'],
                is_authenticated=False
            )
            db.session.add(account)
            db.session.commit()
            logger.info(f"Account created with ID: {account.id}")
            return jsonify({'success': True, 'id': account.id})
            
    except Exception as e:
        logger.error(f"Error in api_accounts: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        db.session.rollback()
        return jsonify({'error': str(e)}), 500

@app.route('/api/accounts/<int:account_id>', methods=['PUT', 'DELETE'])
def api_account_detail(account_id):
    try:
        account = Account.query.get_or_404(account_id)
        
        if request.method == 'PUT':
            data = request.json
            account.name = data['name']
            account.api_id = data['api_id']
            account.api_hash = data['api_hash']
            account.phone_number = data['phone_number']
            account.updated_at = datetime.utcnow()
            db.session.commit()
            return jsonify({'success': True})
        
        elif request.method == 'DELETE':
            db.session.delete(account)
            db.session.commit()
            return jsonify({'success': True})
            
    except Exception as e:
        logger.error(f"Error in api_account_detail: {e}")
        db.session.rollback()
        return jsonify({'error': str(e)}), 500

@app.route('/api/accounts/<int:account_id>/send_code', methods=['POST'])
def api_send_code(account_id):
    """Send OTP code to account phone number"""
    try:
        if not TELEGRAM_AVAILABLE:
            return jsonify({'error': 'Telegram client not available'}), 500
            
        account = Account.query.get_or_404(account_id)
        
        service = TelegramService(
            account.api_id,
            account.api_hash,
            account.phone_number
        )
        
        async def send_code():
            phone_code_hash = await service.send_code_request()
            await service.disconnect()
            return phone_code_hash
        
        phone_code_hash = run_async_safely(send_code())
        
        return jsonify({
            'success': True,
            'phone_code_hash': phone_code_hash,
            'message': f'Verification code sent to {account.phone_number}'
        })
        
    except Exception as e:
        logger.error(f"Error sending code: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/accounts/<int:account_id>/verify_code', methods=['POST'])
def api_verify_code(account_id):
    """Verify OTP code and authenticate account"""
    try:
        if not TELEGRAM_AVAILABLE:
            return jsonify({'error': 'Telegram client not available'}), 500
            
        data = request.json
        code = data.get('code')
        phone_code_hash = data.get('phone_code_hash')
        password = data.get('password')  # For 2FA
        
        account = Account.query.get_or_404(account_id)
        
        service = TelegramService(
            account.api_id,
            account.api_hash,
            account.phone_number
        )
        
        async def verify_code():
            result = await service.verify_code(code, phone_code_hash, password)
            await service.disconnect()
            return result
        
        result = run_async_safely(verify_code())
        
        # Save session string and mark as authenticated
        account.session_string = result['session_string']
        account.is_authenticated = True
        account.updated_at = datetime.utcnow()
        db.session.commit()
        
        return jsonify({
            'success': True,
            'message': f'Account authenticated successfully for {result["first_name"]}'
        })
        
    except Exception as e:
        logger.error(f"Error verifying code: {e}")
        db.session.rollback()
        return jsonify({'error': str(e)}), 500

@app.route('/api/forwarding_rules', methods=['GET', 'POST'])
def api_forwarding_rules():
    try:
        if request.method == 'GET':
            rules = ForwardingRule.query.order_by(ForwardingRule.created_at.desc()).all()
            return jsonify([{
                'id': rule.id,
                'account_id': rule.account_id,
                'name': rule.name,
                'source_chat_id': rule.source_chat_id,
                'destination_chat_id': rule.destination_chat_id,
                'keywords': json.loads(rule.keywords) if rule.keywords else [],
                'is_active': rule.is_active,
                'created_at': rule.created_at.isoformat(),
                'updated_at': rule.updated_at.isoformat()
            } for rule in rules])
        
        elif request.method == 'POST':
            data = request.json
            rule = ForwardingRule(
                account_id=data['account_id'],
                name=data['name'],
                source_chat_id=data['source_chat_id'],
                destination_chat_id=data['destination_chat_id'],
                keywords=json.dumps(data.get('keywords', [])),
                is_active=data.get('is_active', False)
            )
            db.session.add(rule)
            db.session.commit()
            return jsonify({'success': True, 'id': rule.id})
            
    except Exception as e:
        logger.error(f"Error in api_forwarding_rules: {e}")
        db.session.rollback()
        return jsonify({'error': str(e)}), 500

@app.route('/api/chat_histories', methods=['GET'])
def api_chat_histories():
    try:
        histories = ChatHistory.query.order_by(ChatHistory.created_at.desc()).all()
        return jsonify([{
            'id': history.id,
            'account_id': history.account_id,
            'chat_id': history.chat_id,
            'chat_title': history.chat_title,
            'file_path': history.file_path,
            'message_count': history.message_count,
            'created_at': history.created_at.isoformat()
        } for history in histories])
    except Exception as e:
        logger.error(f"Error in api_chat_histories: {e}")
        return jsonify({'error': str(e)}), 500

# Initialize database
def init_database():
    try:
        with app.app_context():
            db.create_all()
            logger.info("Database tables created successfully")
    except Exception as e:
        logger.error(f"Database creation error: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")

# Initialize database on startup
init_database()

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)