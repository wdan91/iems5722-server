import pika
import json
from gcm import GCM
import itertools
import MySQLdb
import requests

api_key = 'AIzaSyDm5Fyzm3eaXyVkeVVz5JQ007yWYo9MclQ'
url = 'https://gcm-http.googleapis.com/gcm/send'
headers = {'Content-Type': 'application/json','Authorization': 'key=' + api_key}
connection = pika.BlockingConnection(pika.ConnectionParameters(
    host='localhost'))
channel = connection.channel()

channel.queue_declare(queue='chat')

def dictfetchall(cursor):
    desc = cursor.description
    return [dict(itertools.izip([col[0] for col in desc], row)) 
             for row in cursor.fetchall()]
def callback(ch, method, properties, body):
    print(" [x] Received %r" % body)
    db = MySQLdb.connect("localhost","root","hanmengbo","iems5722")
    cursor = db.cursor()
    sql = "select * from push_tokens"
    cursor.execute(sql)
    data = dictfetchall(cursor)
    for row in data:
       client_token = row["token"]
       payload = { 'to': client_token, 'data' : body }
       jsonobject = json.loads(body)
       gcm = GCM(api_key)
       data = {
           "chatroom":jsonobject["chatroom"],
           "message":jsonobject["message"]
       }
       #reg_ids =['dOiqzwOdWjA:APA91bHt6Hd7ei55mQ6DdMz-QCjxaHCc7zMPhpEXr-UopTM5G6piNmuqSXrAj6yNWCQWdMIe8pQzImdK1bq_i4CieeL18FneGOJLgZuQlDVUPP_Tt8FRwaBi-Y8WrOs04W5d0cpk3MoN']
       reg_ids = []
       reg_ids.append(client_token)
       response = gcm.json_request(registration_ids=reg_ids,data=data)
       print response
    cursor.close()
    db.close()
        
channel.basic_consume(callback, queue='chat',no_ack=True)
print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()
