import json
import logging
import sqlite3

import telegram
from config import TELEGRAM_BOT_TEST_API
from logger import setup_logger
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (CallbackContext, CallbackQueryHandler,
                          CommandHandler, Updater)

from helper import new_client, check_user, check_adm

def connect_database():
    conn = sqlite3.connect('../database.db', check_same_thread=False)
    logging.info("Opened database successfully")
    return conn

conn = connect_database()

@check_user
def start(update: Update, _: CallbackContext) -> None:
    logging.debug('start')

    update.message.reply_text('/saldo - Verifica o saldo\n' +
                              '/lucro - Verifica o lucro\n' +
                              #'/trades <symbol> - check trades\n' +
                              '/status - Verifica o status do robô\n' +
                              '/ajuda - mostra todos os comandos')

@check_user
def balance(update: Update, context: CallbackContext) -> None:
    logging.debug('balance')

    try:
        telegramID = update.message.from_user['id']
        cur = conn.cursor()
        cur.execute("SELECT ASSET_BALANCE.btc, ASSET_BALANCE.usdc, ASSET_BALANCE.eth, ASSET_BALANCE.total_amount_real, ASSET_BALANCE.total_amount_btc, USER.name FROM ASSET_BALANCE inner join USER ON USER.id = ASSET_BALANCE.user_id where USER.telegram_id=?", (telegramID,))
        conn.commit()
        rows = cur.fetchall()
        for x in rows:
            logging.debug(x)
            update.message.reply_text('Name: ' + x[5] + "\nBitcoin: " + str(x[0]) + " BTC" + "\nDolar: " + str(x[1]) + " USDC" + "\nEthereum: " + str(x[2]) + " ETH" + "\nConversão para real: " + "R$" +  str(x[3]) + "\nConversão para bitcoin: " + str(x[4]) + " BTC")
    except (IndexError, ValueError):
        update.message.reply_text('Usage: /saldo')
    except Exception as e:
        update.message.reply_text(str(e))

@check_user
def profit(update: Update, _: CallbackContext) -> None:
    logging.debug('profit')
    try:
        telegramID = update.message.from_user['id']
        cur = conn.cursor()
        cur.execute("SELECT PROFIT.total_btc, USER.name FROM USER inner join PROFIT ON USER.id = PROFIT.user_id where USER.telegram_id=?", (telegramID,))
        conn.commit()
        rows = cur.fetchall()
        for x in rows:
            logging.debug(x)
            update.message.reply_text('Name: ' + x[1] + "\nLucro: " + str(x[0]) + " BTC")
    except (IndexError, ValueError):
        update.message.reply_text('Usage: /profit')
    except Exception as e:
        update.message.reply_text(str(e))

@check_user
def trades(update: Update, context: CallbackContext) -> None:
    logging.debug('trades')
    
    update.message.reply_text('Check if is necessary')
    # client = new_client()
    # try:
    #     symbol = context.args[0]
    #     trades = client.get_open_orders(symbol=symbol, recvWindow=60000)
    #     update.message.reply_text(json.dumps(trades, indent=4))
    # except (IndexError, ValueError):
    #     update.message.reply_text('Usage: /trades <symbol>')
    # except Exception as e:
    #     update.message.reply_text(str(e))

@check_user
def status(update: Update, context: CallbackContext) -> None:
    logging.debug('status')
    try:
        telegramID = update.message.from_user['id']
        cur = conn.cursor()
        cur.execute("SELECT * FROM USER where telegram_id=?", (telegramID,))
        conn.commit()
        rows = cur.fetchall()
        for x in rows:
            logging.debug(x)
            update.message.reply_text('Name: ' + x[1] + "\nStatus: " + str(x[3]))
    except (IndexError, ValueError):
        update.message.reply_text('Usage: /status')
    except Exception as e:
        update.message.reply_text(str(e))

@check_adm
def statusadm(update: Update, context: CallbackContext) -> None:
    logging.debug('statusadm')
    try:
        cur = conn.cursor()
        cur.execute("SELECT * FROM USER")
        conn.commit()
        rows = cur.fetchall()
        for x in rows:
            logging.debug(x)
            update.message.reply_text('Name: ' + x[1] + "\nStatus: " + str(x[3]))
    except (IndexError, ValueError):
        update.message.reply_text('Usage: /statusadm')
    except Exception as e:
        update.message.reply_text(str(e))

@check_adm
def profitadm(update: Update, context: CallbackContext) -> None:
    logging.debug('profitadm')
    try:
        userID = context.args[0]
        cur = conn.cursor()
        cur.execute("SELECT PROFIT.total_btc, USER.name FROM PROFIT inner join USER ON USER.id = PROFIT.user_id WHERE user_id=?", (userID))
        conn.commit()
        rows = cur.fetchall()
        for x in rows:
            logging.debug(x)
            update.message.reply_text('Name: ' + x[1] + "\nLucro: " + str(x[0]) + " BTC")
    except (IndexError, ValueError):
        cur = conn.cursor()
        cur.execute("SELECT PROFIT.total_btc, USER.name FROM PROFIT inner join USER ON USER.id = PROFIT.user_id")
        conn.commit()
        rows = cur.fetchall()
        for x in rows:
            logging.debug(x)
            update.message.reply_text('Name: ' + x[1] + "\nLucro: " + str(x[0]) + " BTC")
    except Exception as e:
        update.message.reply_text(str(e))

@check_adm
def paymentadm(update: Update, context: CallbackContext) -> None:
    logging.debug('paymentadm')
    try:
        userID = context.args[0]
        cur = conn.cursor()
        cur.execute("SELECT * FROM PAYMENT_HISTORIC where user_id=? ORDER BY timestamp DESC LIMIT 1", (userID,))
        conn.commit()
        rows = cur.fetchall()
        for x in rows:
            logging.debug(x)
            cur.execute("UPDATE PAYMENT_HISTORIC SET " +
                "paid = ? WHERE id=?",
                ("Paid", x[0]))
            conn.commit()
    except (IndexError, ValueError):
        update.message.reply_text("Usage: /pagamentoadm <user_id>")
    except Exception as e:
        update.message.reply_text(str(e))

def main() -> None:
    setup_logger(log_level="INFO", filename="telegram_bot")

    updater = Updater(TELEGRAM_BOT_TEST_API)

    dispatcher = updater.dispatcher

    dispatcher.add_handler(CommandHandler("start", start))
    dispatcher.add_handler(CommandHandler("ajuda", start))
    dispatcher.add_handler(CommandHandler("saldo", balance))
    dispatcher.add_handler(CommandHandler("lucro", profit))
    dispatcher.add_handler(CommandHandler("trades", trades))
    dispatcher.add_handler(CommandHandler("status", status))
    dispatcher.add_handler(CommandHandler("statusadm", statusadm))
    dispatcher.add_handler(CommandHandler("lucroadm", profitadm))
    dispatcher.add_handler(CommandHandler("pagamentoadm", paymentadm))

    updater.start_polling()
    updater.idle()
    
if __name__ == '__main__':
    main()