"""Script for starting the Frame Splitter server."""

import json
import logging
import os
from pathlib import Path

import boto3
import cv2
import numpy as np
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


def find_in_game_time(image: np.array):
    """Finds the quarter and in game time of the provided image.

    The quarter is either 1, 2, 3, or 4. The in game time is given in seconds for that quarter.

    :arg
        image (np.array): the image of which to find the game time and the quarter.

    :return
        (int, int): the quarter and in game time in seconds.
    """
    return 0, 0


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

        frame_path = f"temp-frame/{object_key}"
        Path(frame_path).parent.mkdir(parents=True, exist_ok=True)

        # download object
        s3 = boto3.client('s3')

        app.logger.info(f"Extracting Game ID Metadata for {object_key} from {bucket}.")
        # Retrieve the object's metadata using head_object
        response = s3.head_object(Bucket=bucket, Key=object_key)
        # Extract metadata from the response
        metadata = response.get('Metadata', {})
        metadata_game_id_key = "game-id"
        app.logger.info(f"Got metadata {metadata}.")
        game_id = metadata[metadata_game_id_key]

        app.logger.info(f"Received following message: {message}")
        app.logger.info(f"Downloading Object: {object_key} from Bucket: {bucket}.")

        with open(frame_path, 'wb') as file:
            s3.download_fileobj(bucket, object_key, file)
            app.logger.info(f"Download successful. Image stored at {frame_path}.")

        app.logger.info(f"Reading frame from {frame_path}.")
        image = cv2.imread(frame_path)
        image = np.array(image)

        app.logger.info(f"Finding in game time and quarter.")
        quarter, time = find_in_game_time(image)

        primary_key_name = "id"
        primary_key_value = f"{game_id}_{bucket}_{object_key.split('/')[-1]}"

        game_id_key = "game-id"
        s3_bucket_key = "s3-bucket"
        s3_object_key = "s3-object"
        quarter_key = "quarter"
        time_key = "time"

        item_to_write = {
            primary_key_name: primary_key_value,
            game_id_key: game_id,
            s3_bucket_key: bucket,
            s3_object_key: object_key,
            quarter_key: quarter,
            time_key: time
        }

        table_name = "nba-game-frames"
        dynamodb = boto3.resource('dynamodb')

        app.logger.info(f"Writing {item_to_write} object to DynamoDB Table {table_name}.")
        table = dynamodb.Table(table_name)

        try:
            table.put_item(Item=item_to_write)
            app.logger.info(f"Item {item_to_write} successfully written to DynamoDB Table {table_name}.")
            os.remove(frame_path)
            app.logger.info(f"Successfully removed local file {frame_path}.")
        except OSError as e:
            app.logger.warning(f"Could not remove local frame: {frame_path}", exc_info=e)
        except Exception as e:
            app.logger.warning(f"Problem occurred while writing {item_to_write} to DynamoDB table {table_name}.", exc_info=e)

    return jsonify({'message': 'Hello from the endpoint'}), 200


@app.route('/health', methods=["GET"])
def health_check():
    return jsonify({"message": "Health Check OK"}), 200


@app.route('/hello-world', methods=['GET'])
def hello_world():
    return "Hello World"


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=6000)
