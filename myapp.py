from flask import Flask
from flask import request
from flask import jsonify
import MySQLdb
import itertools
import json
from flask_apis import apis
import pika

app = Flask(__name__)


@app.route('/iems5722/get_chatrooms')
def getchatrooms():
    output = apis.get_chatrooms()
    return jsonify(output)

@app.route('/iems5722/get_messages', methods=['GET'])
def getmessages():
    chatroom_id = request.args.get("chatroom_id",type=int)
    page = request.args.get("page",type=int)
    if page == None:
        return jsonify(status="ERROR", message="Required parameter missing: page")
    elif chatroom_id == None:
        return jsonify(status="ERROR", message="Required parameter missing: chatroom_id")
    else:
        output = jsonify(apis.get_messages(chatroom_id, page))
        return output

@app.route('/iems5722/send_message', methods=['POST'])
def sendmessage():
    chatroom_id = request.form.get("chatroom_id", type=int)
    user_id = request.form.get("user_id", type=int)
    name = request.form.get("name")
    message = request.form.get("message")
    if chatroom_id == None:
        return jsonify(status="ERROR", message="Required parameter missing: chatroom_id")
    elif user_id == None:
        return jsonify(status="ERROR", message="Required parameter missing: user_id")
    elif name == None:
        return jsonify(status="ERROR", message="Required parameter missing: name")
    elif message == None or message == "":
        return jsonify(status="ERROR", message="Required parameter missing: message")
    else:
        apis.send_message(chatroom_id, user_id, name, message)

        return jsonify(status="OK")

@app.route('/iems5722/submit_push_token', methods=['POST'])
def submitpushtoken():
    user_id = request.form.get("user_id", type=int)
    token = request.form.get("token")
    apis.submit_push_token(user_id, token)
    return jsonify(status="OK")








if __name__ == '__main__':
    app.debug = True
    app.run()
