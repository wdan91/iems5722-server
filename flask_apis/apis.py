import MySQLdb
import itertools
import json
import datetime
import pika

def get_chatrooms():
    chatrooms = get_chatrooms_from_db()
    return add_status(chatrooms)

def add_status(data):
    result = {}
    result["data"] = data
    result["status"] = "OK"
   # return json.dumps(result,indent=2)
    return result


def dictfetchall(cursor):
    desc = cursor.description
    return [dict(itertools.izip([col[0] for col in desc], row)) 
             for row in cursor.fetchall()]
def dictfetchmany(cursor, num):
    desc = cursor.description
    return [dict(itertools.izip([col[0] for col in desc], row)) 
             for row in cursor.fetchmany(num)]
def get_chatrooms_from_db():
    db = MySQLdb.connect("localhost","root","hanmengbo","iems5722")
    cursor = db.cursor()
    sql = "select id,name from chatrooms"
    cursor.execute(sql)
    data = dictfetchall(cursor)
    cursor.close()
    db.close()
    return data 
def get_messages_from_db(chatroom_id, page):
    #print "in getmessages from db"
    db = MySQLdb.connect("localhost", "root", "hanmengbo", "iems5722")
    cursor = db.cursor()
    sql = "select message, name, timestamp, user_id from messages where chatroom_id = %s order by id desc" % chatroom_id
    cursor.execute(sql)
    #print "after execute"
    numrows = int(cursor.rowcount)
    totalpages = (numrows - 1) / 5 + 1
    data = {}
    if totalpages < page:
        messages = {}
    else:
        cursor.scroll((page - 1) * 5, mode='relative')
        messages = dictfetchmany(cursor, 5)
    #print "after fetch"
    data = add_page_info(messages, totalpages, page)
    cursor.close()
    db.close()
    return data



def add_page_info(messages, totalpages, currentpage):
    data = {}
    data["current_page"] = currentpage
    data["messages"] = messages
    data["total_pages"] = totalpages
    return data

def get_messages(chatroom_id, page):
    #print " in get_messages"
    messages = get_messages_from_db(chatroom_id, page)
    return add_status(messages)

def send_message(chatroom_id, user_id, name, message):
    db = MySQLdb.connect("localhost","root","hanmengbo","iems5722")
    cursor = db.cursor()
    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
    sql = "insert into messages (chatroom_id, user_id, name, message, timestamp) values(%d, %d, '%s', '%s', '%s');" % (chatroom_id, user_id, name, message, now)
    #print sql
    try:
        cursor.execute(sql)
        db.commit()
    except:
        db.rollback()
    sql = "select name from chatrooms where id = %d" % (chatroom_id)
    cursor.execute(sql)
    data = cursor.fetchall()
    cursor.close()
    db.close()
    chatroomname = data[0][0]
    print "chatroom name = %s" % chatroomname
    connection = pika.BlockingConnection(pika.ConnectionParameters(
                       'localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='chat')
    data = {
        "chatroom":chatroomname,
        "message": message
    }
    json_string = json.dumps(data)
    channel.basic_publish(exchange='',
                          routing_key='chat',
                          body=json_string)
    print(" sent to queue")
    connection.close()



def submit_push_token(user_id, token):
    db = MySQLdb.connect("localhost","root","hanmengbo","iems5722")
    cursor = db.cursor()
    sql = "insert into push_tokens (user_id, token) values( %d, '%s');" % (user_id, token)
    try:
        cursor.execute(sql)
        db.commit()
    except:
        db.rollback()
    cursor.close()
    db.close()
