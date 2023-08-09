"""Script for starting the Frame Splitter server."""

import json
import logging
from io import BytesIO

import boto3
import cv2

from pathlib import Path

import requests
from flask import Flask, request, jsonify

app = Flask(__name__)
app.logger.setLevel(logging.DEBUG)  # Set the logging level to debug


def confirm_subscription(request_header, request_data):
    """Confirms the SNS subscription."""
    if request_header.get('x-amz-sns-message-type') == 'SubscriptionConfirmation':
        app.logger.info("Got request for confirming subscription")
        app.logger.info(request_header)
        # Extract the request data from the POST body

        subscribe_url = request_data['SubscribeURL']

        # Make an HTTP GET request to the SubscribeURL to confirm the subscription
        # This confirms the subscription with Amazon SNS
        # You can use any HTTP library of your choice (e.g., requests)

        app.logger.info(f"Going to URL: {subscribe_url} to confirm the subscription.")
        response = requests.get(subscribe_url)

        if response.status_code == 200:
            app.logger.info(f"Subscription confirmed. Code: {response.status_code}.")
            return jsonify({'message': 'SubscriptionConfirmed'})
        else:
            app.logger.warning(f"Failed to confirmed subscription. Code {response.status_code}.")
            return jsonify({'message': 'Failed to confirm subscription'}), 500

    return jsonify({"message": "Header does not contain 'x-amz-sns-message-type': 'SubscriptionConfirmation'. No "
                               "subscription to confirm."}), 500


@app.route('/add-timestamp', methods=['POST'])
def add_timestamp():
    request_data = request.data.decode('utf-8')

    # Parse the JSON data into a Python dictionary
    try:
        data = json.loads(request_data)
    except json.JSONDecodeError as e:
        return jsonify({'error': str(e)}), 400

    # if the subscription is confirmed, return after it
    if request.headers.get('x-amz-sns-message-type') == 'SubscriptionConfirmation':
        return confirm_subscription(request.headers, data)

    app.logger.info(f"Received Event: {data}.")

    # extract bucket and key
    message = json.loads(data["Message"])

    if message["detail-type"] == "Object Created":
        app.logger.info("Received object created message.")
        detail = message["detail"]
        bucket = detail["bucket"]["name"]
        object_key = detail["object"]["key"]

        frame_dir = "temp-frame"
        Path(frame_dir).mkdir(parents=True, exist_ok=True)
        frame_path = f"{frame_dir}/{object_key}"

        # download object
        s3 = boto3.client('s3')

        app.logger.info(f"Received following message: {message}")
        app.logger.info(f"Downloading Object: {object_key} from Bucket: {bucket}.")

        with open(frame_path, 'wb') as file:
            s3.download_fileobj(bucket, object_key, file)
            app.logger.info("Download successful.")

    return jsonify({'message': 'Hello from the endpoint'}), 200


@app.route('/hello-world', methods=['GET'])
def hello_world():
    return "Hello World"


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
