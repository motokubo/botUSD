from binance.client import Client
from config import API_KEY, API_SECRET, AUTHORIZED_ADMIN, AUTHORIZED_USER


def new_client():
    client = Client(API_KEY, API_SECRET)
    return client

def check_adm(f):
    def wrapper(*args, **kw):
        update = args[0]
        user = update.message.from_user
        if user['id'] in AUTHORIZED_ADMIN:
            return f(*args, **kw)
        else:
            return update.message.reply_text('Não autorizado')
    return wrapper

def check_user(f):
    def wrapper(*args, **kw):
        update = args[0]
        user = update.message.from_user
        if user['id'] in AUTHORIZED_USER:
            return f(*args, **kw)
        else:
            return update.message.reply_text('Não autorizado')
    return wrapper