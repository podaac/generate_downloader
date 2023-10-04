"""Logs and sends email notification when downloader encounters an error.

Logs the error message.
Pusblishes error message to SNS Topic.
"""

# Standard imports
import datetime
import time
import os

# Third-party imports
import boto3
import botocore
import requests

# Constants
TOPIC_STRING = "batch-job-failure"
MODULE = "notify"

def notify(sigevent_type, sigevent_description, sigevent_data):
    """Handles error events."""
    
    status = 1
    log_event(sigevent_type, sigevent_description, sigevent_data)
    log_metadata = get_ecs_task_metadata()
    if sigevent_type == "ERROR": publish_event(sigevent_type, sigevent_description, sigevent_data, log_metadata)
    return status

def log_event(sigevent_type, sigevent_description, sigevent_data):
    """Log event details in CloudWatch."""
    
    # Log to batch log stream
    print(f"notify - INFO: Job Identifier: {os.getenv('AWS_BATCH_JOB_ID')}")
    print(f"notify - INFO: Job Queue: {os.getenv('AWS_BATCH_JQ_NAME')}")
    print(f"notify - INFO: Error type: {sigevent_type}")
    print(f"notify - INFO: Error description: {sigevent_description}")
    if sigevent_data != "": print(f"notify - INFO: Error data: {sigevent_data}")
    
    # Log to downloader error log stream
    logs = boto3.client("logs")
    try:
        # Locate log group
        describe_response = logs.describe_log_groups(
            logGroupNamePattern="downloader-errors"
        )
        log_group_name = describe_response["logGroups"][0]["logGroupName"]
        
         # Find or create log stream - New creation happens every hour
        log_stream_name = f"{os.getenv('AWS_BATCH_JQ_NAME')}-downloader-job-error-{datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H')}0000"
        describe_stream_response = logs.describe_log_streams(
            logGroupName=log_group_name,
            logStreamNamePrefix=log_stream_name
        )
        if len(describe_stream_response["logStreams"]) == 0:       
            create_response = logs.create_log_stream(
                logGroupName=log_group_name,
                logStreamName=log_stream_name
            )
        else:
            log_stream_name=describe_stream_response["logStreams"][0]["logStreamName"]
        
        # Send logs
        log_events = [
            {
                "timestamp": int(time.time() * 1000),
                "message": "==================================================="
            },
            {
                "timestamp": int(time.time() * 1000),
                "message": "Downloader job error encountered."
            },
            {
                "timestamp": int(time.time() * 1000),
                "message": f"Job Identifier: {os.getenv('AWS_BATCH_JOB_ID')}"
            },
            {
                "timestamp": int(time.time() * 1000),
                "message": f"Job Queue: {os.getenv('AWS_BATCH_JQ_NAME')}"
            },
            {
                "timestamp": int(time.time() * 1000),
                "message": f"Error type: {sigevent_type}"
            },
            {
                "timestamp": int(time.time() * 1000),
                "message": f"Error description: {sigevent_description}"
            }
        ]
        if sigevent_data != "": log_events.append({
            "timestamp": int(time.time() * 1000),
            "message": f"Error data: {sigevent_data}"
        })
        put_response = logs.put_log_events(
            logGroupName=log_group_name,
            logStreamName=log_stream_name,
            logEvents=log_events
        )
        print(f"notify - INFO: Logged error message to: {log_group_name}{log_stream_name}")    
    except botocore.exceptions.ClientError as e:
        print(f"notify - INFO: Failed to log to CloudWatch.")
        print(f"notify - ERROR: Error - {e}")
        exit(1)

def get_ecs_task_metadata():
    """Return log group and log stream if available from ECS task endpoint."""
    
    ecs_endpoint = os.getenv("ECS_CONTAINER_METADATA_URI_V4")
    if ecs_endpoint:
        response = requests.get(ecs_endpoint)
        print(f"notify - INFO: ECS endpoint response: {response}.")
        response_json = response.json()
        log_group = response_json["LogOptions"]["awslogs-group"]
        log_stream = response_json["LogOptions"]["awslogs-stream"]
        log = f"Log Group: {log_group}\nLog Stream: {log_stream}\n\n"
    else:
        log = ""
    return log
    
def publish_event(sigevent_type, sigevent_description, sigevent_data, log_metadata):
    """Publish event to SNS Topic."""
    
    sns = boto3.client("sns")
    
    # Get topic ARN
    try:
        topics = sns.list_topics()
    except botocore.exceptions.ClientError as e:
        print("notify - INFO: Failed to list SNS Topics.")
        print(f"notify - ERROR: Error - {e}")
        exit(1)
    for topic in topics["Topics"]:
        if TOPIC_STRING in topic["TopicArn"]:
            topic_arn = topic["TopicArn"]
            
    # Publish to topic
    subject = f"Generate Batch Job Failure: {os.getenv('SIGEVENT_SOURCE')}"        
    message = f"Generate AWS Batch downloader job has encountered an error.\n\n" \
        + "JOB INFORMATION:\n" \
        + f"Job Identifier: {os.getenv('AWS_BATCH_JOB_ID')}.\n" \
        + f"Job Queue: {os.getenv('AWS_BATCH_JQ_NAME')}.\n"
    
    if log_metadata:
        message += log_metadata
        
    message += "\nERROR INFORMATION:\n" \
        + f"Error type: {sigevent_type}.\n" \
        + f"Error description: {sigevent_description}\n\n" \
        + "Please follow these steps to diagnose the error: https://wiki.jpl.nasa.gov/pages/viewpage.action?pageId=771470900#GenerateCloudErrorDetection&Recovery-AWSBatchJobFailures\n\n\n"  
    if sigevent_data != "": message += f"Error data: {sigevent_data}"
    try:
        response = sns.publish(
            TopicArn = topic_arn,
            Message = message,
            Subject = subject
        )
    except botocore.exceptions.ClientError as e:
        print(f"notify - INFO: Failed to publish to SNS Topic: {topic_arn}.")
        print(f"notify - ERROR: Error - {e}")
        exit(1)
    
    print(f"notify - INFO: Message published to SNS Topic: {topic_arn}.")
    
if __name__ == "__main__":
    
    sigevent_type = "ERROR"
    sigevent_description = "Test problem occured."
    # sigevent_data = ""
    sigevent_data = "Test problem data details."
    notify(sigevent_type, sigevent_description, sigevent_data)
    
    sigevent_type = "WARN"
    sigevent_description = "Test warning occured."
    # sigevent_data = ""
    sigevent_data = "Test warning data details."
    notify(sigevent_type, sigevent_description, sigevent_data)