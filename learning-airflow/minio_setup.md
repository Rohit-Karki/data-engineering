I am setting up a minio webhook which sends a post request to the flask app.
The flask app is used in between rather than directly joining the minio using the webhook because airflow
dag requires us to have some structure to trigger the dag.

Then the flask app sends the request to the dag to trigger the dag run.
```
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
```

![dag api structure](https://github.com/user-attachments/assets/301dc761-fbb6-4e50-a2b3-853a44fd74f3)

The dag run id must be different for every dag run. Otherwise it will show error by saying that there is duplicate dag run.

# Event-Driven Architecture: MinIO Event Notification Webhooks using Flask

Configuring a webhook in MinIO can be accomplished through various methods ranging from using the user-interface, by using mc (the MinIO client utility), or by way of scripting with various programming languages.

## Understanding the Data Structure of MinIO Event Notifications

The S3 event notifications from MinIO include a detailed JSON data structure, essential for a comprehensive understanding and effective management of events. Below I have listed some of the values found from within the event data:

    Key: The unique identifier of the object in the bucket.
    eTag: The object’s version identifier for integrity and version control.
    Size: The size of the object in bytes, indicating its scale.
    Sequencer: Ensures the events are processed in the exact sequence they occurred.
    ContentType: The media type of the object, specifying how to handle or display the file.
    UserMetadata: User-defined metadata attached to the object, providing additional context.
    Bucket Details:
        ARN (Amazon Resource Name): The unique identifier for the bucket in AWS.
        Name: The name of the bucket where the object is stored.
        OwnerIdentity: Information about the owner of the bucket.
    s3SchemaVersion: Indicates the version of the S3 event notification schema used.
    ConfigurationId: Identifier for the specific notification configuration that triggered this event.

## Setting Up MinIO for Webhooks and Event-Driven Operations

docker exec -it minio /bin/sh 

The reason for running our mc commands from within this shell is because the Docker minio/minio image already has mc installed and ready to go.

Once inside the container’s interactive terminal, the process of configuring MinIO for event notifications using the MinIO Client (mc) involves the following key steps:

    Setting Up MinIO Alias: The first step involves creating an alias for your MinIO server using the MinIO Client (mc). This alias is a shortcut to your MinIO server, allowing you to easily execute further mc commands without repeatedly specifying the server’s address and access credentials. This step  simplifies the management of your MinIO server through the client.

    mc alias set myminio http://localhost:9000 minio minio123

    Adding the Webhook Endpoint to MinIO: Configure a new webhook service endpoint in MinIO. This setup is done using either environment variables or runtime configuration settings, where you define important parameters such as the endpoint URL, an optional authentication token for security, and client certificates for secure connections.

    mc admin config set myminio notify_webhook:1 endpoint="http://flaskapp:5000/minio-event" queue_limit="10"

    Restarting the MinIO Deployment: Once you have configured the settings, restart your MinIO deployment to ensure the changes take effect.

    mc admin service restart myminio

    Expect:
    Restart command successfully sent to myminio. Type Ctrl-C to quit or wait to follow the status of the restart process....Restarted myminio successfully in 1 seconds

    Configuring Bucket Notifications: The next step involves using the mc event add command. This command is used to add new bucket notification events, setting the newly configured Webhook service as the target for these notifications.

    mc event add myminio/mybucket arn:minio:sqs::1:webhook --event put,get,delete

    Expect:
    Successfully added arn:minio:sqs::1:webhook

    List Bucket Subscribed Events: Run this command to list the event assigned to myminio/mybucket:

    minio mc event list myminio/mybucket

    Expect:
    arn:minio:sqs::1:webhook   s3:ObjectCreated:*,s3:ObjectAccessed:*,s3:ObjectRemoved:*   Filter:

    List Bucket Assigned Events (in JSON): Run this command to list the event assigned to myminio/mybucket in JSON format: 

    minio mc event list myminio/mybucket arn:minio:sqs::1:webhook --json

    Expect: 
    { "status": "success", "id": "", "event": ["s3:ObjectCreated:","s3:ObjectAccessed:", "s3:ObjectRemoved:*"], "prefix": "", "suffix": "", "arn": "arn:minio:sqs::1:webhook"}

The Structure of the Event Notification Data Received by Flask

Depending on the services or integration you are building, you may need to identify the event_data from your Flask app, and this requires a good understanding of the data your event provides. 

{
  "s3": {
    "bucket": {
      "arn": "arn:aws:s3:::mybucket",
      "name": "mybucket",
      "ownerIdentity": {
        "principalId": "minio"
      }
    },
    "object": {
      "key": "cmd.md",
      "eTag": "d8e8fca2dc0f896fd7cb4cb0031ba249",
      "size": 5,
      "sequencer": "17A9AB4FA93B35D8",
      "contentType": "text/markdown",
      "userMetadata": {
        "content-type": "text/markdown"
      }
    },
    "configurationId": "Config",
    "s3SchemaVersion": "1.0"
  },
  "source": {
    "host": "127.0.0.1",
    "port": "",
    "userAgent": "MinIO (linux; arm64) minio-go/v7.0.66 mc/RELEASE.2024-01-11T05-49-32Z"
  },
  "awsRegion": "",
  "eventName": "s3:ObjectCreated:Put",
  "eventTime": "2024-01-12T17:58:12.569Z",
  "eventSource": "minio:s3",
  "eventVersion": "2.0",
  "userIdentity": {
    "principalId": "minio"
  },
  "responseElements": {
    "x-amz-id-2": "dd9025bab4ad464b049177c95eb6ebf374d3b3fd1af9251148b658df7ac2e3e8",
    "x-amz-request-id": "17A9AB4FA9328C8F",
    "x-minio-deployment-id": "c3642fb7-ab2a-44a0-96cb-246bf4d18e84",
    "x-minio-origin-endpoint": "http://172.18.0.3:9000"
  },
  "requestParameters": {
    "region": "",
    "principalId": "minio",
    "sourceIPAddress": "127.0.0.1"
  }
}
