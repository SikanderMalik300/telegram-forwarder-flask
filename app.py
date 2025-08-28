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
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'your-super-secret-key-change-this-in-production')

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
    delay_seconds = db.Column(db.Integer, default=0)  # Delay in seconds
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

# Global storage for temporary sessions during authentication
temp_auth_sessions = {}

class TelegramService:
    def __init__(self, api_id, api_hash, phone_number, session_string="", temp_session_key=None):
        if not TELEGRAM_AVAILABLE:
            raise Exception("Telegram client not available")
        
        self.api_id = int(api_id)
        self.api_hash = api_hash
        self.phone_number = phone_number
        self.temp_session_key = temp_session_key
        
        # Use temporary session if provided, otherwise use permanent session
        if temp_session_key and temp_session_key in temp_auth_sessions:
            self.session = StringSession(temp_auth_sessions[temp_session_key])
            logger.info(f"Using temporary session for {phone_number}")
        else:
            self.session = StringSession(session_string)
            logger.info(f"Using permanent session for {phone_number}")
            
        self.client = None
        self.is_connected = False

    def _create_client(self):
        """Create client in the same thread where it will be used"""
        self.client = TelegramClient(self.session, self.api_id, self.api_hash)

    async def send_code_request(self):
        """Send OTP code to phone number and store session"""
        try:
            if not self.client:
                self._create_client()
            
            await self.client.connect()
            result = await self.client.send_code_request(self.phone_number)
            
            # Store the session string for the verification step
            session_string = self.client.session.save()
            temp_session_key = f"{self.phone_number}_{result.phone_code_hash}"
            temp_auth_sessions[temp_session_key] = session_string
            
            logger.info(f"Stored temporary session for {self.phone_number}")
            
            # Don't disconnect - keep the session alive
            return {
                'phone_code_hash': result.phone_code_hash,
                'temp_session_key': temp_session_key
            }
        except Exception as e:
            logger.error(f"Error sending code: {e}")
            raise

    async def verify_code(self, code, phone_code_hash, password=None):
        """Verify OTP code using the same session that sent the code"""
        try:
            # Reconstruct the temp session key
            temp_session_key = f"{self.phone_number}_{phone_code_hash}"
            
            if temp_session_key not in temp_auth_sessions:
                raise Exception("Authentication session expired or not found. Please request a new code.")
            
            # Create new service instance with the temporary session
            if not self.temp_session_key:
                # Create a new instance with the temp session
                service = TelegramService(
                    self.api_id, 
                    self.api_hash, 
                    self.phone_number, 
                    temp_session_key=temp_session_key
                )
                service._create_client()
                await service.client.connect()
                client = service.client
            else:
                # We already have the temp session
                if not self.client:
                    self._create_client()
                await self.client.connect()
                client = self.client
            
            try:
                # Sign in with the code
                user = await client.sign_in(
                    phone=self.phone_number,
                    code=code, 
                    phone_code_hash=phone_code_hash
                )
            except errors.SessionPasswordNeededError:
                if password:
                    user = await client.sign_in(password=password)
                else:
                    raise Exception("Two-factor authentication enabled. Password required.")
            
            # Get the final session string for permanent storage
            final_session_string = client.session.save()
            
            # Clean up temporary session
            if temp_session_key in temp_auth_sessions:
                del temp_auth_sessions[temp_session_key]
            
            await client.disconnect()
            
            return {
                'success': True,
                'session_string': final_session_string,
                'user_id': user.id,
                'first_name': user.first_name
            }
            
        except Exception as e:
            logger.error(f"Error verifying code: {e}")
            # Clean up on error
            temp_session_key = f"{self.phone_number}_{phone_code_hash}"
            if temp_session_key in temp_auth_sessions:
                del temp_auth_sessions[temp_session_key]
            raise

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

    async def list_chats(self):
        """List all chats for the account"""
        try:
            if not self.client:
                self._create_client()
            
            await self.client.connect()
            
            if not await self.client.is_user_authorized():
                raise Exception("Account not authenticated")
            
            dialogs = await self.client.get_dialogs()
            chats = []
            
            for dialog in dialogs:
                chat_info = {
                    'id': dialog.id,
                    'title': dialog.title or 'Unknown',
                    'type': 'private' if dialog.is_user else ('channel' if dialog.is_channel else 'group'),
                    'participant_count': getattr(dialog.entity, 'participants_count', None)
                }
                chats.append(chat_info)
                
            return chats
            
        except Exception as e:
            logger.error(f"Error listing chats: {e}")
            raise

    async def forward_messages_to_channel_with_delay(self, source_chat_id, destination_channel_id, keywords, delay_seconds=0):
        """Forward messages from source to destination with configurable delay"""
        try:
            if not self.client:
                self._create_client()
            
            await self.client.connect()
            
            if not await self.client.is_user_authorized():
                raise Exception("Account not authenticated")
            
            # Get the last message ID to start monitoring from
            try:
                last_messages = await self.client.get_messages(source_chat_id, limit=1)
                if last_messages:
                    last_message_id = last_messages[0].id
                else:
                    last_message_id = 0
            except Exception as e:
                logger.warning(f"Could not get last message ID for {source_chat_id}: {e}")
                last_message_id = 0
            
            logger.info(f"Starting message forwarding from {source_chat_id} to {destination_channel_id} with {delay_seconds}s delay")
            
            # Queue to store messages for delayed sending
            message_queue = []
            
            while True:
                try:
                    # Get new messages since the last checked message
                    messages = await self.client.get_messages(source_chat_id, min_id=last_message_id, limit=None)
                    
                    for message in reversed(messages):
                        try:
                            # Check if the message text includes any of the keywords
                            should_forward = False
                            
                            if keywords:
                                if message.text and any(keyword.lower() in message.text.lower() for keyword in keywords):
                                    logger.info(f"Message contains a keyword: {message.text[:50]}...")
                                    should_forward = True
                            else:
                                # Forward all messages if no keywords specified
                                should_forward = True
                            
                            if should_forward and message.text:
                                if delay_seconds > 0:
                                    # Add to queue with timestamp for delayed sending
                                    send_time = datetime.utcnow().timestamp() + delay_seconds
                                    message_queue.append({
                                        'text': message.text,
                                        'send_time': send_time,
                                        'original_message_id': message.id
                                    })
                                    logger.info(f"Message queued for delayed sending in {delay_seconds}s")
                                else:
                                    # Send immediately
                                    await self.client.send_message(destination_channel_id, message.text)
                                    logger.info("Message forwarded immediately")
                            
                            # Update the last message ID
                            last_message_id = max(last_message_id, message.id)
                            
                        except Exception as msg_error:
                            logger.error(f"Error processing message {message.id}: {msg_error}")
                            continue
                    
                    # Process delayed messages
                    current_time = datetime.utcnow().timestamp()
                    messages_to_send = []
                    remaining_messages = []
                    
                    for queued_msg in message_queue:
                        if current_time >= queued_msg['send_time']:
                            messages_to_send.append(queued_msg)
                        else:
                            remaining_messages.append(queued_msg)
                    
                    # Send delayed messages
                    for msg_to_send in messages_to_send:
                        try:
                            await self.client.send_message(destination_channel_id, msg_to_send['text'])
                            logger.info(f"Delayed message sent after {delay_seconds}s delay")
                        except Exception as send_error:
                            logger.error(f"Error sending delayed message: {send_error}")
                    
                    # Update message queue
                    message_queue = remaining_messages
                    
                    # Add a delay before checking for new messages again
                    await asyncio.sleep(2)  # Check every 2 seconds for new messages and queue processing
                    
                except Exception as loop_error:
                    logger.error(f"Error in forwarding loop: {loop_error}")
                    await asyncio.sleep(5)  # Wait longer on error
                    continue
                    
        except Exception as e:
            logger.error(f"Error in message forwarding with delay: {e}")
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
            result = await service.send_code_request()
            # Don't disconnect here - keep session alive
            return result
        
        result = run_async_safely(send_code())
        
        return jsonify({
            'success': True,
            'phone_code_hash': result['phone_code_hash'],
            'temp_session_key': result['temp_session_key'],
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
        temp_session_key = data.get('temp_session_key')
        password = data.get('password')  # For 2FA
        
        account = Account.query.get_or_404(account_id)
        
        # Use the temporary session key
        service = TelegramService(
            account.api_id,
            account.api_hash,
            account.phone_number,
            temp_session_key=temp_session_key
        )
        
        async def verify_code():
            result = await service.verify_code(code, phone_code_hash, password)
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

# Chat listing route
@app.route('/api/chats/list', methods=['POST'])
def api_list_chats():
    """List chats for an account"""
    try:
        if not TELEGRAM_AVAILABLE:
            return jsonify({'error': 'Telegram client not available'}), 500
            
        data = request.json
        account_id = data.get('account_id')
        
        account = Account.query.get_or_404(account_id)
        
        if not account.is_authenticated or not account.session_string:
            return jsonify({'error': 'Account not authenticated'}), 400
        
        service = TelegramService(
            account.api_id,
            account.api_hash,
            account.phone_number,
            account.session_string
        )
        
        async def list_chats():
            chats = await service.list_chats()
            await service.disconnect()
            return chats
        
        chats = run_async_safely(list_chats())
        
        return jsonify({
            'success': True,
            'chats': chats
        })
        
    except Exception as e:
        logger.error(f"Error listing chats: {e}")
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
                'delay_seconds': rule.delay_seconds,
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
                delay_seconds=data.get('delay_seconds', 0),
                is_active=data.get('is_active', False)
            )
            db.session.add(rule)
            db.session.commit()
            
            # Start forwarding if rule is active
            if rule.is_active:
                try:
                    start_forwarding_rule(rule.id)
                except Exception as e:
                    logger.error(f"Failed to start forwarding rule {rule.id}: {e}")
            
            return jsonify({'success': True, 'id': rule.id})
            
    except Exception as e:
        logger.error(f"Error in api_forwarding_rules: {e}")
        db.session.rollback()
        return jsonify({'error': str(e)}), 500

@app.route('/api/forwarding_rules/<int:rule_id>', methods=['PUT', 'DELETE'])
def api_forwarding_rule_detail(rule_id):
    try:
        rule = ForwardingRule.query.get_or_404(rule_id)
        
        if request.method == 'PUT':
            data = request.json
            
            # Stop current forwarding if active
            if rule.is_active and rule_id in active_forwarders:
                stop_forwarding_rule(rule_id)
            
            rule.name = data['name']
            rule.account_id = data['account_id']
            rule.source_chat_id = data['source_chat_id']
            rule.destination_chat_id = data['destination_chat_id']
            rule.keywords = json.dumps(data.get('keywords', []))
            rule.delay_seconds = data.get('delay_seconds', 0)
            rule.is_active = data.get('is_active', False)
            rule.updated_at = datetime.utcnow()
            db.session.commit()
            
            # Start forwarding if rule is now active
            if rule.is_active:
                try:
                    start_forwarding_rule(rule_id)
                except Exception as e:
                    logger.error(f"Failed to start forwarding rule {rule_id}: {e}")
            
            return jsonify({'success': True})
        
        elif request.method == 'DELETE':
            # Stop forwarding if active
            if rule.is_active and rule_id in active_forwarders:
                stop_forwarding_rule(rule_id)
            
            db.session.delete(rule)
            db.session.commit()
            return jsonify({'success': True})
            
    except Exception as e:
        logger.error(f"Error in api_forwarding_rule_detail: {e}")
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

# Forwarding management functions
def start_forwarding_rule(rule_id):
    """Start a forwarding rule in a background thread with delay support"""
    try:
        rule = ForwardingRule.query.get(rule_id)
        if not rule or not rule.is_active:
            return
        
        account = rule.account
        if not account.is_authenticated or not account.session_string:
            logger.error(f"Account {account.id} not authenticated for rule {rule_id}")
            return
        
        # Don't start if already running
        if rule_id in active_forwarders:
            logger.info(f"Forwarding rule {rule_id} already active")
            return
        
        # Create service instance
        service = TelegramService(
            account.api_id,
            account.api_hash,
            account.phone_number,
            account.session_string
        )
        
        # Parse keywords
        keywords = json.loads(rule.keywords) if rule.keywords else []
        
        # Start forwarding in background thread with delay support
        def run_forwarder():
            try:
                asyncio.run(service.forward_messages_to_channel_with_delay(
                    rule.source_chat_id,
                    rule.destination_chat_id,
                    keywords,
                    rule.delay_seconds
                ))
            except Exception as e:
                logger.error(f"Forwarding rule {rule_id} stopped with error: {e}")
                # Remove from active forwarders
                if rule_id in active_forwarders:
                    del active_forwarders[rule_id]
        
        # Start thread
        thread = threading.Thread(target=run_forwarder, daemon=True)
        thread.start()
        
        # Store the thread reference
        active_forwarders[rule_id] = {
            'thread': thread,
            'service': service,
            'delay_seconds': rule.delay_seconds,
            'started_at': datetime.utcnow()
        }
        
        logger.info(f"Started forwarding rule {rule_id} with {rule.delay_seconds}s delay")
        
    except Exception as e:
        logger.error(f"Failed to start forwarding rule {rule_id}: {e}")

def stop_forwarding_rule(rule_id):
    """Stop a forwarding rule"""
    try:
        if rule_id in active_forwarders:
            forwarder_info = active_forwarders[rule_id]
            
            # Try to disconnect the service
            try:
                service = forwarder_info['service']
                asyncio.run(service.disconnect())
            except Exception as e:
                logger.error(f"Error disconnecting service for rule {rule_id}: {e}")
            
            # Remove from active forwarders
            del active_forwarders[rule_id]
            logger.info(f"Stopped forwarding rule {rule_id}")
        
    except Exception as e:
        logger.error(f"Failed to stop forwarding rule {rule_id}: {e}")

def stop_all_forwarding_rules():
    """Stop all active forwarding rules"""
    rule_ids = list(active_forwarders.keys())
    for rule_id in rule_ids:
        stop_forwarding_rule(rule_id)

# Start active rules on application startup
def start_active_forwarding_rules():
    """Start all active forwarding rules on application startup"""
    try:
        with app.app_context():
            active_rules = ForwardingRule.query.filter_by(is_active=True).all()
            logger.info(f"Starting {len(active_rules)} active forwarding rules")
            
            for rule in active_rules:
                try:
                    start_forwarding_rule(rule.id)
                except Exception as e:
                    logger.error(f"Failed to start rule {rule.id} on startup: {e}")
                    
    except Exception as e:
        logger.error(f"Error starting active forwarding rules: {e}")

# Initialize database
def init_database():
    try:
        with app.app_context():
            db.create_all()
            
            # Add delay column if missing (for existing installations)
            try:
                # Check if column exists
                inspector = db.inspect(db.engine)
                columns = inspector.get_columns('forwarding_rule')
                column_names = [col['name'] for col in columns]
                
                if 'delay_seconds' not in column_names:
                    # Add the column using raw SQL
                    if 'postgresql' in app.config['SQLALCHEMY_DATABASE_URI']:
                        db.engine.execute("ALTER TABLE forwarding_rule ADD COLUMN delay_seconds INTEGER DEFAULT 0")
                    else:
                        db.engine.execute("ALTER TABLE forwarding_rule ADD COLUMN delay_seconds INTEGER DEFAULT 0")
                    
                    logger.info("Added delay_seconds column to forwarding_rule table")
                else:
                    logger.info("delay_seconds column already exists")
                    
            except Exception as e:
                logger.error(f"Error adding delay column: {e}")
            
            logger.info("Database tables created successfully")
    except Exception as e:
        logger.error(f"Database creation error: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")

# Cleanup function for graceful shutdown
def cleanup_on_exit():
    """Cleanup function to stop all forwarding rules on exit"""
    logger.info("Cleaning up forwarding rules...")
    stop_all_forwarding_rules()

import atexit
atexit.register(cleanup_on_exit)

# Initialize database on startup
init_database()

# Start active forwarding rules after a short delay
def delayed_start_forwarders():
    time.sleep(5)  # Wait 5 seconds for app to fully initialize
    start_active_forwarding_rules()

# Start forwarders in background thread
threading.Thread(target=delayed_start_forwarders, daemon=True).start()

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)