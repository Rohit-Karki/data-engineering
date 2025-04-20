from flask import Flask, jsonify, request
import requests
import base64

# Flask app initialization
app = Flask(__name__)

AIRFLOW_DAG_TRIGGER_URL = "http://localhost:8080/api/v1/dags/file_processing_workflow/dagRuns"
AIRFLOW_AUTH = ("admin", "admin")  # username, password


@app.route('/minio-event', methods=['POST'])
def handle_minio_event():
    data = request.json
    print("🎯 Received event:", data)

    # Extract the object key (filename) from MinIO event
    try:
        event_info = data['Records'][0]
        filename = event_info['s3']['object']['key']
    except (KeyError, IndexError):
        return jsonify({"error": "Invalid MinIO event structure"}), 400

    # Prepare DAG trigger payload
    dag_payload = {
        "conf": {
            "filename": filename
        },
        "dag_run_id": f"manual__{filename.replace('/', '_')}"
    }

    # Trigger the DAG
    response = requests.post(
        AIRFLOW_DAG_TRIGGER_URL,
        auth=AIRFLOW_AUTH,
        headers={"Content-Type": "application/json"},
        json=dag_payload
    )

    if response.status_code == 200:
        print("✅ DAG triggered successfully")
        return '', 200
    else:
        print("❌ Failed to trigger DAG:", response.text)
        return jsonify({"error": "Failed to trigger DAG", "details": response.text}), 500


@app.route('/hello', methods=['GET'])
def hello():
    return "Hello MinIO!"


if __name__ == "__main__":
    app.run(host='0.0.0.0', debug=True, port=5000)
