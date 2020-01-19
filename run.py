import configparser, json, time, pytz, requests, logging, re, sqlite3
from telethon import TelegramClient, connection, sync, events
from telethon.tl.functions.messages import GetHistoryRequest
from telethon.tl.types import PeerUser, PeerChat, PeerChannel
from telethon.errors import FloodWaitError
from datetime import date, datetime, timedelta

formatter = logging.Formatter('%(asctime)s %(levelname)-10s %(message)s')
log = logging.getLogger('log')

log_file = logging.FileHandler('log.log')
log_file.setFormatter(formatter)
log.setLevel(logging.DEBUG)
log.addHandler(log_file)

log_stdout = logging.StreamHandler()
log_stdout.setFormatter(formatter)
log.addHandler(log_stdout)

log.debug('Initialization application...')
config = configparser.ConfigParser()
config.read("config.ini")

api_id = config['Telegram']['api_id']
api_hash = config['Telegram']['api_hash']
phone_number = config['Telegram']['phone_number']
proxy_ip = config['Telegram']['proxy_ip']
proxy_port = config['Telegram']['proxy_port']
secret = config['Telegram']['secret']
channel_id = config['Telegram']['channel_id']
interval = int(config['Telegram']['interval'])
debug = config['bot']['debug']
try:                                                                                                                                                                                                                                                            
    intercalate = float(config['deals']['intercalate'])                                                                                                                                                                                                         
except:                                                                                                                                                                                                                                                         
    intercalate = float(1)  
try:
    start_rate = float(config['deals']['start_rate'])
except:
    start_rate = float(1)

proxy = (proxy_ip, int(proxy_port), secret)

offset_msg = 0
limit_msg = 1
total_messages = 0
total_count_limit = 0


def calculate_endtime():
    if debug == 'true':
        endtime = (datetime.now() + timedelta(minutes=denominator + 1)).replace(second=0, microsecond=0)
    else:
        endtime = (datetime.now() + timedelta(minutes=denominator - 2)).replace(second=0, microsecond=0)

    while True:
        if int(endtime.strftime('%M')) % denominator == 0:
            endutcunixtime = round(endtime.timestamp())
            break
        else:
            endtime = endtime + timedelta(minutes=1)

    return endtime, endutcunixtime


def generate_output(offset_msg, current_id, message_id, message_text):
    if "Результат" in message_text:
        currency_pair = re.split("\s+", message_text)[0]
        log.debug(f'offset: {offset_msg} current_id: {current_id} id: {message_id} message: {message_text}')
        request_gateway = f'offset: {offset_msg} current_id: {current_id} id: {message_id} message: {message_text}'
        remove_deal(currency_pair)
        get_request(request_gateway)
    elif any(re.findall(r'ВВЕРХ|ВНИЗ', message_text, re.IGNORECASE)):
        command = re.split("\s+", message_text)

        currency_pair = command[0]

        if command[3] == 'ВВЕРХ':
            currency_move = 'CALL'
        elif command[3] == 'ВНИЗ':
            currency_move = 'PUT'

        endtime, endutcunixtime = calculate_endtime()
        request_gateway = f'http://127.0.0.2/?request=frx{currency_pair}={currency_move}={start_rate}=endtime={endutcunixtime}'
        proccessing_deals(currency_pair, endutcunixtime)
        log.debug(f'offset: {offset_msg} current_id: {current_id} id: {message_id} message: {message_text}')
        log.debug(f'Time: {endtime}, Command: {request_gateway}')
        get_request(request_gateway)
    else:
        log.debug(f'Unknown type of message: {message_text}')
        request_gateway = f'Unknown type of message: {message_text}'
        get_request(request_gateway)


def get_request(request_gateway):
    try:
        r = requests.get(request_gateway, verify=False, timeout=1)
        log.debug(f'Got response from gateway - r.status_code')
    except:
        log.debug(f'Gateway not response')
        


def create_connection_tg(client):
    i = 0
    while True:
        try:
            client.start()
            break
        except:
            i = i + 1;
            log.debug(f'Connection error. Retry {i}')
            if i > 5:
                exit()


def create_connection_sql(db_file):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        c = conn.cursor()
        c.execute("DROP TABLE IF EXISTS deals")
        c.execute(
            "CREATE TABLE IF NOT EXISTS deals (id INTEGER PRIMARY KEY AUTOINCREMENT, subject TEXT, end TEXT, rate TEXT)")
        c.execute("CREATE TABLE IF NOT EXISTS system (id INTEGER PRIMARY KEY AUTOINCREMENT, count INTEGER)")
    except Error as e:
        print(e)

    return conn


def counter_sql(count):
    c = conn.cursor()
    c.execute('''INSERT OR REPLACE INTO system(id,count) VALUES(1,?)''', (count,))
    conn.commit()


def proccessing_deals(subject, end):
    """

    """
    c = conn.cursor()
    c.execute('''INSERT OR REPLACE INTO deals(id,subject,end,rate) VALUES(null,?,?,?)''', (subject, end, start_rate,))
    conn.commit()


def prolongation_deals():
    c = conn.cursor()
    stored_deals = c.execute('''SELECT id,subject,end,rate FROM deals''')
    if stored_deals.fetchone():
        log.debug(f'Stored deals:')
        for row in c.execute('''SELECT id,subject,end,rate FROM deals'''):
            endtime, endutcunixtime = calculate_endtime()
            if int(row[2]) <= round((datetime.now() - timedelta(seconds=5)).timestamp()):
                try:
                    row[3]
                    rate = float(row[3]) + intercalate
                except:
                    rate = float(1)

                if rate > row[3]:
                    log.debug('Prolongation of remaining transaction.')
                    request_gateway = f'http://127.0.0.2/?request=frx{row[1]}=PUT={rate}=endtime={endutcunixtime}'
                    log.debug(f'Time: {endtime}, Command: {request_gateway}')
                    get_request(request_gateway)
                    c = conn.cursor()
                    c.execute('''INSERT OR REPLACE INTO deals(id,subject,end,rate) VALUES(?,?,?,?)''',
                              (row[0], row[1], endutcunixtime, rate,))
                    conn.commit()
                else:
                    remove_deal(row[1])
            else:
                log.debug(f'    - {row}')


def remove_deal(currency_pair):
    c = conn.cursor()
    for row in c.execute('''SELECT * FROM deals WHERE subject=?''', (currency_pair,)):
        log.debug(f'Deal finished: {row}')
        c.execute('''DELETE FROM deals WHERE id=?''', (row[0],))


conn = create_connection_sql('bot.db')

if debug == 'true':
    denominator = 1
else:
    denominator = 15

log.debug('Start event loop...')
while True:
    current_minute = int(datetime.now().strftime('%M'))
    if current_minute % denominator == 0:
        prolongation_deals()  
    elif (current_minute + 1) % denominator == 0:
        client = TelegramClient('session_name', api_id, api_hash,
                                connection=connection.ConnectionTcpMTProxyIntermediate, proxy=proxy)
        client.flood_sleep_threshold = 24 * 60 * 60
        create_connection_tg(client)
        channel = client.get_entity(PeerChannel(int(channel_id)))

        if not client.is_user_authorized():
            client.send_code_request(phone)
            try:
                client.sign_in(phone, input('Enter the code: '))
            except SessionPasswordNeededError:
                client.sign_in(password=input('Password: '))

        log.debug('Parsing channel...')
        all_messages = []

        try:
            current_id
            limit_date = None
        except:
            log.debug('Last id not found. Reset all data.')
            current_id = 0
            limit_date = datetime.now() + timedelta(minutes=-1)

        latest_msg = client(
            GetHistoryRequest(peer=channel, offset_id=offset_msg, offset_date=limit_date, add_offset=0, limit=1,
                              max_id=0, min_id=0, hash=0))

        for message in latest_msg.messages:
            if current_id != 0 and current_id <= message.id:
                offset_msg = message.id - current_id
                if offset_msg == 0:
                    log.debug('No new messages.')
                elif offset_msg >= 1:
                    history = client(
                        GetHistoryRequest(peer=channel, offset_id=message.id + 1, offset_date=None, add_offset=0,
                                          limit=10, min_id=current_id, max_id=message.id + 1, hash=0))
                    for message in history.messages:
                        generate_output(offset_msg, current_id, message.id, message.message)
                        if current_id <= message.id:
                            current_id = message.id
                            counter_sql(current_id)

            else:
                log.debug(f'Error! current_id: {current_id}, message_id:  {message.id}, message: {message.message}')
                current_id = message.id
                counter_sql(current_id)

        log.debug('Clean variables and objects.')
        client.disconnect()
        client = None
        history = None
        offset_msg = 0
        time.sleep(interval)
        log.debug(' ')
        log.debug(' ')
    else:
        log.debug('I havent tasks. Skipping.')
        time.sleep(interval)
