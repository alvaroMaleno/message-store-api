from flask import Blueprint, request

from app.db import query_to_json

get_messages_bp = Blueprint('get_messages', __name__)


@get_messages_bp.route('/api/messages/getAllMessages')
def get_all_messages():
    ALL_MESSAGES = """SELECT * FROM message_store.message_store.messages;"""
    return query_to_json(ALL_MESSAGES)


@get_messages_bp.route('/api/messages/getMessages')
def get_messages():
    MESSAGES = """
    select
        id,
        stream_name,
        "type",
        "position",
        global_position,
        "data",
        metadata,
        "time" 
    from  
        message_store.message_store.messages m 
    where 
        global_position >= {global_position}
    limit 
        {max_messages};
    """
    args = request.args
    global_position = args.get("global_position")
    max_messages = args.get("max_messages")
    query = MESSAGES.format(
        global_position=global_position, max_messages=max_messages)
    return query_to_json(query)


@get_messages_bp.route('/api/messages/getLastMessage', methods=['GET'])
def get_last_message(stream=''):
    LAST_MESSAGE = """
    select
        *
    from  
        message_store.message_store.messages m 
    where 
        stream_name = '{stream_name}'
    order by 
        m.global_position desc
    limit 
        1;
    """
    args = request.args
    stream_name = args.get("stream_name")
    if not stream_name:
        stream_name = stream
    query = LAST_MESSAGE.format(stream_name=stream_name)
    return query_to_json(query=query)


@get_messages_bp.route('/api/messages/getCategoryMessages', methods=['GET'])
def get_category_messages():
    CATEGORY_MESSAGES = """
    select
        *
    from  
        message_store.message_store.messages m 
    where 
        stream_name = '{stream_name}'
    and 
        "position" >= {from_position}
    order by 
        m.global_position desc
    limit 
        {max_messages};
    """
    args = request.args
    stream_name = args.get("stream_name")
    from_position = args.get("from_position")
    max_messages = args.get("max_messages")
    query = CATEGORY_MESSAGES.format(
        stream_name=stream_name, from_position=from_position, max_messages=max_messages)
    return query_to_json(query)


@get_messages_bp.route('/api/messages/getMessagesWithValues', methods=['GET'])
def get_messages_with_values():
    GET_MESSAGES = """
    select
        *
    from  
        message_store.message_store.messages m 
    where 
        m.id  = '{message_id}'
    and 
        m.stream_name = '{stream_name}'
    and 
        m."type" = '{type}'
    and
        m."data" = '{data}'
    and 
        m.metadata = '{meta_data}';
    """
    args = request.args
    message_id = args.get("message_id")
    stream_name = args.get("stream_name")
    message_type = args.get("type")
    data = args.get("data")
    meta_data = args.get("meta_data")
    query = GET_MESSAGES.format(message_id=message_id,
                                stream_name=stream_name, type=message_type, data=data, meta_data=meta_data)
    return query_to_json(query)
