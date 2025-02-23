from flask import Flask, jsonify, request, send_from_directory
import psycopg2
import os
from dotenv import load_dotenv
import glob
import re

app = Flask(__name__, static_folder='../')  # Serve static files from parent directory

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

@app.route('/get_log_files')
def get_log_files():
    log_files = glob.glob('../logs/*.txt')
    log_files = [os.path.basename(f) for f in log_files]
    return jsonify(log_files)

@app.route('/get_log_summary')
def get_log_summary():
    file_name = request.args.get('file')
    file_path = f"../logs/{file_name}"
    try:
        with open(file_path, 'r') as f:
            log_data = f.read()

        cases_processed = len(re.findall(r'Cleaning data for county', log_data))
        errors = len(re.findall(r'ERROR', log_data))
        warnings = len(re.findall(r'WARNING', log_data))

        dates = re.findall(r'(\d{4}-\d{2}-\d{2})', log_data)
        date_range = f"{min(dates)} - {max(dates)}" if dates else "N/A"

        county_matches = re.findall(r'county: (\w+)', log_data)
        county = ", ".join(set(county_matches)) if county_matches else "N/A"

        summary = {
            "cases": cases_processed,
            "errors": errors,
            "warnings": warnings,
            "date_range": date_range,
            "county": county
        }
        return jsonify(summary)
    except FileNotFoundError:
        return jsonify({"error": "File not found"}), 404

@app.route('/get_log_data/<filename>') # New route to get log file content
def get_log_data(filename):
    file_path = f"../logs/{filename}"
    try:
        with open(file_path, 'r') as f:
            log_data = f.read()
        return log_data, 200, {'Content-Type': 'text/plain'} # Important: Set content type
    except FileNotFoundError:
        return "File not found", 404

@app.route('/')
def serve_dashboard():
    return send_from_directory(app.static_folder, 'dashboard/log_table.html') # Serve from correct location

if __name__ == '__main__':
    app.run(debug=True, port=5000)