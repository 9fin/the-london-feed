from flask import Flask, render_template, request, jsonify
from flask_socketio import SocketIO, Namespace, emit
from flask_cors import CORS
from threading import Event
import json
import random
import os

SECRET_KEY = os.environ.get('APP_SECRET_KEY', 'not-the-actual-key-in-prod')
DEBUG = True if os.environ.get('DEBUG', False) else False

async_mode = None

app = Flask(__name__)
CORS(app)
app.config['SECRET_KEY'] = SECRET_KEY
socketio = SocketIO(app, async_mode=async_mode)

# data arrays
travel_arr = []
review_arr = []
gif_arr = []
threads = []

# read in travel updates
with open('all_tfl_tweets.json') as tweets:
    data = json.load(tweets)
    travel_arr.extend(data.get('tfl'))

# read in business reviews
with open('yelp_data.json') as business_data:
    data = json.load(business_data)
    review_arr.extend(data.get('businesses'))

# read in gifs
with open('gifs.json') as gif_data:
    data = json.load(gif_data)
    gif_arr.extend(data.get('gifs'))

# unroll all the id's of the data array items into a flat array, for faster id checks later
travel_arr_idx = [_.get('id_str') for _ in travel_arr]
review_arr_idx = [_.get('id') for _ in review_arr]
gif_arr_idx = [_.get('id') for _ in gif_arr]

# websocket Namespaces
keepalive_ns = '/keepalive'
travel_ns = '/travel'
review_ns = '/reviews'
gif_ns = '/gifs'


def background_thread(data_src, ns, sid, thread_stop, cursor_start=5):
    if thread_stop.is_set():
        print "Thread already stopped for sid: {}".format(sid)
        return
    print "Starting thread on ns: {} for client sid: {}".format(ns, sid)
    # calculate required sending rate of 4 msg/s with 20% sd
    local_rnd = random.Random()
    for i, data in enumerate(data_src[cursor_start:], cursor_start):
        if thread_stop.is_set():
            break
        socketio.sleep(abs(local_rnd.gauss(4, 0.8)))
        socketio.emit('message', {'data': data, 'idx': i}, namespace=ns)
        print "emitting on ns: {} for client sid: {} - idx: {} id: {}".format(ns, sid, i, data['id'])
    print "Stopping thread on ns: {} for client sid: {}".format(ns, sid)


def start_stream(data_src, ns, sid, **kwargs):
    global treads
    thread_stop = Event()
    thread = socketio.start_background_task(background_thread, data_src, ns, sid, thread_stop)
    threads.append(tuple([sid, thread, thread_stop]))


def terminate_thread(sid):
    threads_to_kill = filter(lambda x: x[0] == sid and not x[2].is_set(), threads)
    if threads_to_kill:
        print "Delete threads", threads_to_kill
        print [x[2].is_set() for x in threads_to_kill]
        map(lambda x: x[2].set(), threads_to_kill)
        print [x[2].is_set() for x in threads_to_kill]
        map(lambda x: threads.remove(x), threads_to_kill)


class Travel(Namespace):
    def on_connect(self):
        emit('message', {'data': 'Connected: Travel sid: {}'.format(request.sid)})
        print "Serving client on websocket sid: {}".format(request.sid)

    def on_start(self, message):
        print "Travel - message: {}".format(message)
        # start thread and sending messages
        cursor_start = message['data'].get('cursor_start')
        if cursor_start:
            start_stream(travel_arr, travel_ns, request.sid, cursor_start=cursor_start)
        else:
            start_stream(travel_arr, travel_ns, request.sid)
        emit('message', {'data': 'Starting Stream: Travel'})

    def on_disconnect(self):
        print 'Client disconnected sid: {}'.format(request.sid)
        terminate_thread(request.sid)


class Reviews(Namespace):
    def on_connect(self):
        emit('message', {'data': 'Connected: Reviews sid: {}'.format(request.sid)})
        print "Serving client on websocket sid: {}".format(request.sid)

    def on_start(self, message):
        print "Reviews - message: {}".format(message)
        # start thread and sending messages
        cursor_start = message.get('cursor_start')
        if cursor_start:
            start_stream(review_arr, review_ns, request.sid, cursor_start=cursor_start)
        else:
            start_stream(review_arr, review_ns, request.sid)
        emit('message', {'data': 'Starting Stream: Reviews'})

    def on_disconnect(self):
        print 'Client disconnected sid: {}'.format(request.sid)
        terminate_thread(request.sid)


class Gifs(Namespace):
    def on_connect(self):
        emit('message', {'data': 'Connected: Gifs sid: {}'.format(request.sid)})
        print "Serving client on websocket sid: {}".format(request.sid)

    def on_start(self, message):
        print "Gifs - message: {}".format(message)
        # start thread and sending messages
        cursor_start = message.get('cursor_start')
        if cursor_start:
            start_stream(gif_arr, gif_ns, request.sid, cursor_start=cursor_start)
        else:
            start_stream(gif_arr, gif_ns, request.sid)
        emit('message', {'data': 'Starting Stream: Gifs'})

    def on_disconnect(self):
        print 'Client disconnected sid: {}'.format(request.sid)
        terminate_thread(request.sid)


# register websocket handlers with their respective Namespace classes
socketio.on_namespace(Travel(travel_ns))
socketio.on_namespace(Reviews(review_ns))
socketio.on_namespace(Gifs(gif_ns))


@socketio.on('connect', namespace=keepalive_ns)
def test_connect():
    emit('message', {'data': 'Connected: Keepalive sid: {}'.format(request.sid)})
    print "Serving client on websocket sid: {}".format(request.sid)


@socketio.on('ping', namespace=keepalive_ns)
def ping_pong():
    emit('pong')
    print "pong sid: {}".format(request.sid)


@app.route('/', methods=['GET'])
def index():
    print "Serving client on route /"
    return render_template('test.html', async_mode=socketio.async_mode)


@app.route('/websocket_ct', methods=['GET'])
def websocket_ct():
    ns_dict = {}
    ns_dict["keepalive_namespace"] = keepalive_ns
    ns_dict["travel_namespace"] = travel_ns
    ns_dict["review_namespace"] = review_ns
    ns_dict["gif_namespace"] = gif_ns
    server = request.url_root
    print "Serving client on route /websocket_ct"
    return jsonify(socket_server=server, namespaces=ns_dict)


@app.route('/sync', methods=['GET'])
def sync():
    print "Serving client on route /sync"
    return jsonify(travel=travel_arr[:5], reviews=review_arr[:5], gifs=gif_arr[:5])


@app.route('/threads', methods=['GET'])
def thread_check():
    print "Serving client on route /threads"
    return jsonify(threads=len(threads))


@app.route('/star', methods=['POST'])
def star():
    print "Serving client on route /star"
    data = request.get_json()
    if not data:
        return jsonify(errors='No data was supplied'), 400
    data_type = data.get('data_type')
    data_id = data.get('data_id')
    if not data_type or not data_id:
        return jsonify(errors='No data type or no data id supplied'), 400
    if data_type not in ['travel', 'reviews', 'gifs']:
        return jsonify(errors='Data type not found'), 400
    if data_type == 'travel':
        if data_id in travel_arr_idx:
            return jsonify(success=True)
        else:
            return jsonify(errors='id: {} not found'.format(data_id)), 500
    if data_type == 'reviews':
        if data_id in review_arr_idx:
            return jsonify(success=True)
        else:
            return jsonify(errors='id: {} not found'.format(data_id)), 500
    if data_type == 'gifs':
        if data_id in gif_arr_idx:
            return jsonify(success=True)
        else:
            return jsonify(errors='id: {} not found'.format(data_id)), 500


if __name__ == "__main__":
    socketio.run(app, debug=DEBUG)
