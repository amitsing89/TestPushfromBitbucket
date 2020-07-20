# import the Flask class from the flask module
from flask import Flask, render_template, redirect, url_for, request
import redis
import base64
import pandas as pd
from cassandra.cluster import Cluster
from cassandra.protocol import NumpyProtocolHandler
from cassandra.query import tuple_factory
import pickle
from datetime import timedelta
from uuid import uuid4
from redis import Redis
from werkzeug.datastructures import CallbackDict
from flask.sessions import SessionInterface, SessionMixin
# from OpenSSL import SSL

import os

# context = SSL.Context(SSL.SSLv23_METHOD)
cer = os.path.join(os.path.dirname(__file__), 'crt')
key = os.path.join(os.path.dirname(__file__), 'key')


class RedisSession(CallbackDict, SessionMixin):
    def __init__(self, initial=None, sid=None, new=False):
        def on_update(self):
            self.modified = True

        CallbackDict.__init__(self, initial, on_update)
        self.sid = sid
        self.new = new
        self.modified = False


class RedisSessionInterface(SessionInterface):
    serializer = pickle
    session_class = RedisSession

    def __init__(self, redis=None, prefix='session:'):
        if redis is None:
            redis = Redis()
        self.redis = redis
        self.prefix = prefix

    def generate_sid(self):
        return str(uuid4())

    def get_redis_expiration_time(self, app, session):
        if session.permanent:
            return app.permanent_session_lifetime
        return timedelta(days=1)

    def open_session(self, app, request):
        sid = request.cookies.get(app.session_cookie_name)
        if not sid:
            sid = self.generate_sid()
            return self.session_class(sid=sid, new=True)
        val = self.redis.get(self.prefix + sid)
        if val is not None:
            data = self.serializer.loads(val)
            return self.session_class(data, sid=sid)
        return self.session_class(sid=sid, new=True)

    def save_session(self, app, session, response):
        domain = self.get_cookie_domain(app)
        if not session:
            self.redis.delete(self.prefix + session.sid)
            if session.modified:
                response.delete_cookie(app.session_cookie_name,
                                       domain=domain)
            return
        redis_exp = self.get_redis_expiration_time(app, session)
        cookie_exp = self.get_expiration_time(app, session)
        val = self.serializer.dumps(dict(session))
        self.redis.setex(self.prefix + session.sid, val,
                         int(redis_exp.total_seconds()))
        response.set_cookie(app.session_cookie_name, session.sid,
                            expires=cookie_exp, httponly=True,
                            domain=domain)


redis_conn = redis.Redis(host='localhost')

# create the application object
app = Flask(__name__)

cluster = Cluster(contact_points=['localhost'])
session = cluster.connect('adlog')
session.row_factory = tuple_factory
session.client_protocol_handler = NumpyProtocolHandler
query = "SELECT * FROM adtracker"


# use decorators to link the function to a url
@app.route('/')
def home():
    return "STRING MANIPULATION"


@app.route('/updatepassword', methods=['GET', 'POST'])
def updatepassword():
    error = None
    # if request.method == 'POST':
    # render_template('passwordupdate.html',error=error)
    # redis_conn.hset('login', request.form['username'], base64.b64encode(request.form['password']))
    return render_template('passwordupdate.html', error=error)


@app.route('/update', methods=['GET', 'POST'])
def update():
    redis_conn.hset('login', request.form['username'], base64.b64encode(request.form['password']))
    return redirect(url_for('check'))


@app.route('/check', methods=['GET', 'POST'])
def check():
    df = pd.DataFrame()
    for row in session.execute(query):
        df = df.append(pd.DataFrame(row))
    df = df.reset_index(drop=True).fillna(pd.np.nan).to_html()
    return '<html><body><h1>{0}</h1></body></html>'.format(df)
    # return df


# route for handling the login page logic
@app.route('/login', methods=['GET', 'POST'])
def login():
    error = None
    if request.method == 'POST':
        if request.form['username'] in redis_conn.hkeys('login'):
            if request.form['password'] == base64.b64decode(redis_conn.hget('login', request.form['username'])):
                return redirect(url_for('check'))
            elif request.form['password'] != base64.b64decode(redis_conn.hget('login', request.form['username'])):
                error = 'Invalid password Please try again or create a new Username.'
                return redirect(url_for('updatepassword'))
        else:
            error = 'New Username Created'
            redis_conn.hset('login', request.form['username'], base64.b64encode(request.form['password']))

    return render_template('login.html', error=error)


# start the server with the 'run()' method
if __name__ == '__main__':
    # app = Flask(__name__)
    context = (cer, key)
    app.session_interface = RedisSessionInterface()
    app.run(host='0.0.0.0', debug=True)
    # app.run(ssl_context=('cert.pem', 'key.pem'))
