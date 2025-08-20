from flask import Flask, render_template, request, jsonify, send_file
from flask_sqlalchemy import SQLAlchemy
from datetime import datetime
import os
import asyncio
import json
import tempfile
from telethon.sync import TelegramClient
from telethon import errors
from telethon.sessions import StringSession
import threading
import time

app = Flask(__name__)

# Database configuration for Railway/Render (PostgreSQL)
# For local development, use SQLite
if os.environ.get('DATABASE_URL'):
    app.config['SQLALCHEMY_DATABASE_URI'] = os.environ.get('DATABASE_URL').replace('postgres://', 'postgresql://')
else:
    app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///telegram_forwarder.db'

app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'your-secret-key-here')

db = SQLAlchemy(app)

# Database Models
class Account(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    api_id = db.Column(db.String(50), nullable=False)
    api_hash = db.Column(db.String(100), nullable=False)
    phone_number = db.Column(db.String(20), nullable=False)
    session_string = db.Column(db.Text)
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

# Global storage for active connections and forwarders
active_connections = {}
active_forwarders = {}

class TelegramService:
    def __init__(self, api_id, api_hash, phone_number, session_string=""):
        self.api_id = int(api_id)
        self.api_hash = api_hash
        self.phone_number = phone_number
        self.session = StringSession(session_string)
        self.client = TelegramClient(self.session, self.api_id, self.api_hash)
        self.is_connected = False

    async def connect(self):
        try:
            await self.client.connect()
            if await self.client.is_user_authorized():
                self.is_connected = True
                return True
            else:
                # Try to start client (this will handle authentication)
                await self.client.start(phone=self.phone_number)
                self.is_connected = True
                return True
        except Exception as e:
            print(f"Connection error: {e}")
            return False

    async def get_dialogs(self):
        if not self.is_connected:
            raise Exception("Not connected to Telegram")
        
        dialogs = await self.client.get_dialogs()
        result = []
        
        for dialog in dialogs:
            entity = dialog.entity
            result.append({
                'id': entity.id,
                'title': getattr(entity, 'title', getattr(entity, 'first_name', 'Unknown')),
                'type': self._get_dialog_type(entity),
                'participant_count': getattr(entity, 'participants_count', None),
                'username': getattr(entity, 'username', None),
                'is_channel': getattr(entity, 'broadcast', False),
                'is_group': getattr(entity, 'megagroup', False) or hasattr(entity, 'participants_count'),
                'is_private': not hasattr(entity, 'title')
            })
        
        return result

    async def get_chat_history(self, chat_id, limit=1000):
        if not self.is_connected:
            raise Exception("Not connected to Telegram")
        
        messages = await self.client.get_messages(int(chat_id), limit=limit)
        result = []
        
        for msg in messages:
            result.append({
                'id': msg.id,
                'text': msg.message or '',
                'date': msg.date.timestamp(),
                'from_id': str(msg.from_id) if msg.from_id else '',
                'sender': {
                    'first_name': getattr(msg.sender, 'first_name', '') if msg.sender else '',
                    'last_name': getattr(msg.sender, 'last_name', '') if msg.sender else '',
                    'username': getattr(msg.sender, 'username', '') if msg.sender else '',
                },
                'is_outgoing': msg.out,
                'media_type': self._get_media_type(msg.media) if msg.media else None
            })
        
        # Sort by date (oldest first)
        result.sort(key=lambda x: x['date'])
        return result

    def _get_dialog_type(self, entity):
        if getattr(entity, 'broadcast', False):
            return 'channel'
        elif getattr(entity, 'megagroup', False) or hasattr(entity, 'participants_count'):
            return 'group'
        else:
            return 'private'

    def _get_media_type(self, media):
        if hasattr(media, 'photo'):
            return 'photo'
        elif hasattr(media, 'document'):
            return 'document'
        else:
            return 'unknown'

    def get_session_string(self):
        return self.session.save()

    async def disconnect(self):
        if self.client.is_connected():
            await self.client.disconnect()
        self.is_connected = False

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

# API Routes
@app.route('/api/accounts', methods=['GET', 'POST'])
def api_accounts():
    if request.method == 'GET':
        accounts = Account.query.order_by(Account.created_at.desc()).all()
        return jsonify([{
            'id': acc.id,
            'name': acc.name,
            'api_id': acc.api_id,
            'api_hash': acc.api_hash,
            'phone_number': acc.phone_number,
            'created_at': acc.created_at.isoformat(),
            'updated_at': acc.updated_at.isoformat()
        } for acc in accounts])
    
    elif request.method == 'POST':
        data = request.json
        account = Account(
            name=data['name'],
            api_id=data['api_id'],
            api_hash=data['api_hash'],
            phone_number=data['phone_number']
        )
        db.session.add(account)
        db.session.commit()
        return jsonify({'success': True, 'id': account.id})

@app.route('/api/accounts/<int:account_id>', methods=['PUT', 'DELETE'])
def api_account_detail(account_id):
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

@app.route('/api/chats/list', methods=['POST'])
def api_chats_list():
    data = request.json
    account_id = data['account_id']
    
    account = Account.query.get_or_404(account_id)
    
    try:
        # Create Telegram service
        service = TelegramService(
            account.api_id,
            account.api_hash,
            account.phone_number,
            account.session_string or ""
        )
        
        # Run async function
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            connected = loop.run_until_complete(service.connect())
            if not connected:
                return jsonify({'error': 'Failed to connect to Telegram'}), 500
            
            chats = loop.run_until_complete(service.get_dialogs())
            
            # Save session string
            session_string = service.get_session_string()
            if session_string:
                account.session_string = session_string
                db.session.commit()
            
            return jsonify({'success': True, 'chats': chats})
        finally:
            loop.run_until_complete(service.disconnect())
            loop.close()
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/chats/download', methods=['POST'])
def api_chats_download():
    data = request.json
    account_id = data['account_id']
    chat_id = data['chat_id']
    chat_title = data.get('chat_title', f'Chat {chat_id}')
    limit = data.get('limit', 1000)
    
    account = Account.query.get_or_404(account_id)
    
    try:
        # Create Telegram service
        service = TelegramService(
            account.api_id,
            account.api_hash,
            account.phone_number,
            account.session_string or ""
        )
        
        # Run async function
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            connected = loop.run_until_complete(service.connect())
            if not connected:
                return jsonify({'error': 'Failed to connect to Telegram'}), 500
            
            messages = loop.run_until_complete(service.get_chat_history(chat_id, limit))
            
            # Format messages as text (like Python script)
            text_content = f"Chat History: {chat_title}\n"
            text_content += f"Downloaded: {datetime.now().isoformat()}\n"
            text_content += f"Total Messages: {len(messages)}\n"
            text_content += f"Account: {account.name} ({account.phone_number})\n"
            text_content += "=" * 80 + "\n\n"
            
            for message in messages:
                date = datetime.fromtimestamp(message['date']).strftime('%Y-%m-%d %H:%M:%S')
                
                # Format sender name
                sender = 'Unknown'
                if message['sender']['first_name']:
                    sender = f"{message['sender']['first_name']} {message['sender'].get('last_name', '')}".strip()
                elif message['sender']['username']:
                    sender = f"@{message['sender']['username']}"
                elif message['from_id']:
                    sender = f"User {message['from_id']}"
                
                direction = '→' if message['is_outgoing'] else '←'
                media_info = f" [{message['media_type'].upper()}]" if message['media_type'] else ''
                
                text_content += f"{direction} [{date}] {sender}{media_info}:\n"
                text_content += f"{message['text'] or '[Media/Non-text message]'}\n"
                text_content += "-" * 50 + "\n\n"
            
            # Save chat history record
            chat_history = ChatHistory(
                account_id=account_id,
                chat_id=chat_id,
                chat_title=chat_title,
                file_path=f"{chat_title}_{int(time.time())}.txt",
                message_count=len(messages)
            )
            db.session.add(chat_history)
            db.session.commit()
            
            # Create temporary file
            temp_file = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt', encoding='utf-8')
            temp_file.write(text_content)
            temp_file.close()
            
            filename = f"{chat_title.replace(' ', '_')}_{datetime.now().strftime('%Y%m%d')}.txt"
            
            return send_file(temp_file.name, as_attachment=True, download_name=filename, mimetype='text/plain')
        
        finally:
            loop.run_until_complete(service.disconnect())
            loop.close()
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/forwarding_rules', methods=['GET', 'POST'])
def api_forwarding_rules():
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

@app.route('/api/chat_histories', methods=['GET'])
def api_chat_histories():
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

# Initialize database
with app.app_context():
    db.create_all()

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)