import json
from flask import Blueprint, request, Response
from app.db import insert
from app.messages.get_routes import get_last_message

post_messages_bp = Blueprint('post_messages', __name__)


@post_messages_bp.route('/api/messages/postMessage', methods=['POST'])
def post_message():
    POST_MESSAGE = """
    INSERT INTO message_store.messages
        (global_position, 
        "position", 
        "time", 
        stream_name, 
        "type", 
        "data", 
        metadata, 
        id)
    VALUES(
        nextval('message_store.messages_global_position_seq'::regclass), 
        {position}, 
        (now() AT TIME ZONE 'utc'::text), 
        '{stream_name}', 
        '{type}', 
        '{data}', 
        '{meta_data}', 
        uuid('{message_id}'));
            """
    form = request.form
    message_id = form["message_id"]
    stream_name = form["stream_name"]
    message_type = form["type"]
    data = form["data"]
    meta_data = form["meta_data"]

    last_messages_pos = json.loads(get_last_message(stream_name))
    last_message = last_messages_pos[0]

    position = 1

    if last_message["position"] is not None:
        position = last_message["position"] + 1

    query = POST_MESSAGE.format(
        message_id=message_id,
        stream_name=stream_name,
        type=message_type,
        data=data,
        meta_data=meta_data,
        position=position
    )
    insert(query=query)
    return Response(get_last_message(stream_name), status=201, mimetype='application/json')
