# This functions works based on an S3 notification event.
# When triggered by an S3 file creation event it reads the file
# and rewrites its content to a specified CloudWatch log group stream.
# Use case: get Amazon WAF logs into CloudWatch

import boto3
import codecs
import time

def s3_logs_stripper(event, context):
    if event:
        print('[DEBUG] Event payload:', event)
        s3FileObject = event['Records'][0]

        s3BucketName = str(s3FileObject['s3']['bucket']['name'])
        print('[INFO] Event received from S3:', s3BucketName)

        s3FileName = str(s3FileObject['s3']['object']['key'])
        print('[INFO] Log file to be read:', s3FileName)

        s3 = boto3.client('s3')
        print('[INFO] Obtaining the log file from S3...')
        s3File = s3.get_object(Bucket = s3BucketName, Key = s3FileName)
        s3FileContent = s3File['Body']

        cwlogs = boto3.client('logs')
        cwLogGroup = '/aws/lambda/s3LogsStripperOutput'
        cwLogStream = s3FileName
        print('[INFO] Creating log stream:', cwLogStream, 'in log group:', cwLogGroup)
        cwlogs.create_log_stream(
            logGroupName=cwLogGroup,
            logStreamName=cwLogStream
        )

        print('[INFO] Reading log events and sending to CloudWatch logs...')
        iterator = 0

        for s3FileLine in codecs.getreader('utf-8')(s3FileContent):
            logEvent = {
                'logGroupName': cwLogGroup,
                'logStreamName': cwLogStream,
                'logEvents': [
                    {
                        'timestamp': int(round(time.time() * 1000)),
                        'message': s3FileLine
                    }
                ]
            }

            if iterator != 0:
                logEvent.update({ 'sequenceToken': response['nextSequenceToken'] })

            response = cwlogs.put_log_events(**logEvent)
            iterator += 1

    print('[INFO] All done.')
