import flask
import psycopg2
import psycopg2.extensions
import select
import os
from flask import render_template, request, jsonify

app = flask.Flask(__name__)

def stream_messages(channel):
    conn = psycopg2.connect(database='postgres', user='postgres',
                            password='1234', host='localhost')
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

    curs = conn.cursor()
    curs.execute("LISTEN channel_%d;" % int(channel))

    while True:
        select.select([conn], [], [])
        conn.poll()
        while conn.notifies:
            notify = conn.notifies.pop()
            yield "data: " + notify.payload + "\n\n"

@app.route("/message/<channel>", methods=['GET'])
def get_messages(channel):
    return flask.Response(stream_messages(channel), mimetype='text/event-stream')

@app.route("/message/new", methods=['POST'])
def new_message():
    canal = request.args['id']
    fonte = request.args['source']
    msg = request.args['message']

    try:
        conn = psycopg2.connect(database='postgres', user='postgres',
                                password='1234', host='localhost' )
        
        curs = conn.cursor()
        curs.execute("INSERT INTO message(channel, source, content) VALUES(%s, %s, %s)", (canal, fonte, msg))
        conn.commit()
        conn.close()
        return jsonify({'message': 'Conteúdo enviado com sucesso'}, 200)

    except ConnectionRefusedError as e:
        print('ERRO DE CONEXÃO', e)
        return jsonify({'error': f'erro de servidor {e}'}, 500)

@app.route("/", methods=['GET', 'POST'])
def index():
    return render_template('index.html')

if __name__ == "__main__":
    app.run()