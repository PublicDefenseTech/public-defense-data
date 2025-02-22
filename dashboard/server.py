from flask import Flask, jsonify, request, send_from_directory
import psycopg2
import os
from dotenv import load_dotenv

app = Flask(__name__)

def get_db_connection():
    file_path='dashboard/.env'
    env_path = os.path.abspath(file_path)
    load_dotenv(env_path)
    try:
        conn = psycopg2.connect(
            host=os.getenv("PGHOST"),
            database=os.getenv("PGDATABASE"),
            user=os.getenv("PGUSER"),
            password=os.getenv("PGPASSWORD")
        )
        return conn
    except psycopg2.Error as e:
        print(f"Error connecting to database: {e}")
        return None

@app.route('/get_tables')
def get_tables():
    conn = get_db_connection()
    if not conn:
        return jsonify([])

    cur = conn.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE'")
    tables = [row[0] for row in cur.fetchall()]
    cur.close()
    conn.close()
    return jsonify(tables)

@app.route('/get_table_data')
def get_table_data():
    table_name = request.args.get('table')
    if not table_name:
        return jsonify([])

    conn = get_db_connection()
    if not conn:
        return jsonify([])

    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {table_name}")
    columns = [desc[0] for desc in cur.description]
    rows = [dict(zip(columns, row)) for row in cur.fetchall()]
    cur.close()
    conn.close()
    return jsonify(rows)

@app.route('/')
def serve_dashboard():
    return send_from_directory('.', 'dashboard.html')

if __name__ == '__main__':
    app.run(debug=True, port=5000)