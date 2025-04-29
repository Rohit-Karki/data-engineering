from flask import Flask, jsonify, request
import requests
import base64
from datetime import datetime
import time

# Flask app initialization
app = Flask(__name__)

AIRFLOW_DAG_TRIGGER_URL = "http://localhost:8080/api/v1/dags/file_compression_and_email_workflow/dagRuns"
AIRFLOW_AUTH = ("admin", "admin")  # username, password

recent_files = {}

@app.route('/minio-event', methods=['POST'])
def handle_minio_event():
    data = request.json
    # print("🎯 Received event:", data)

    # Extract the object key (filename) from MinIO event
    try:
        event_info = data['Records'][0]
        filename = event_info['s3']['object']['key']
    except (KeyError, IndexError):
        return jsonify({"error": "Invalid MinIO event structure"}), 400
    
    # Generate a timestamp for the dag_run_id
    current_time = datetime.now().strftime("%dT%H%M%S")

    print(f"current time is {current_time}")
    # Prepare DAG trigger payload
    dag_payload = {
        "conf": {
            "filename": filename
        },
        # "dag_run_id": f"manual__{filename.replace('/', '_')}"
        "dag_run_id": f"manual__{current_time}"
    }
    
    # De-duplication: Skip if this file was just seen
    now = time.time()
    if filename in recent_files and now - recent_files[filename] < 10:
        print("⚠️ Duplicate event detected, skipping:", filename)        
        print("❌ Failed to trigger DAG:",)
        return jsonify({"error": "Failed to trigger DAG "}), 500    
    else:
        recent_files[filename] = now
        response = requests.post(
            AIRFLOW_DAG_TRIGGER_URL,
            auth=AIRFLOW_AUTH,
            headers={"Content-Type": "application/json"},
            json=dag_payload
        )
        print("✅ DAG triggered successfully")
        return '', 200    


@app.route('/hello', methods=['GET'])
def hello():
    return "Hello MinIO!"


if __name__ == "__main__":
    app.run(host='0.0.0.0', debug=True, port=5000)
