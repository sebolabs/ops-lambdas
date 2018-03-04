import os
import sys
import boto3
from botocore.exceptions import ClientError

def get_latest_ami(ec2_conn, ami_filter, nodetype):
  try:
    latest = None
    response = ec2_conn.describe_images(Filters=ami_filter)
    if len(response['Images']) != 0:
      print('[INFO] AMIs found:')
      for image in response['Images']:
        print('ImageId: '+image['ImageId']+' | Name: '+image['Name']+' | CreationDate: '+image['CreationDate'])
        if not latest:
          latest = image
          continue
        if parser.parse(image['CreationDate']) > parser.parse(latest['CreationDate']):
          latest = image
      return latest
    else:
      print('[ERR] No AMIs found!')
      sys.exit('[ERR] No AMIs found!')
  except ClientError as error:
    print('[ERR] '+str(error))
    raise

def run_ec2(ec2_conn, aws_region, image_id, instance_type, subnet_id, security_group_id, instance_profile_arn, assessment_template_arn, timeout_s, tags):
  try:
    response = ec2_conn.run_instances(
      ImageId=image_id, 
      InstanceType=instance_type, 
      MaxCount=1, 
      MinCount=1, 
      TagSpecifications=[{'ResourceType': 'instance', 'Tags': tags}], 
      SubnetId=subnet_id,
      SecurityGroupIds=[security_group_id],
      UserData="\n".join([ 
        "#!/bin/bash",
        "curl -O https://d1wk0tztpsntt1.cloudfront.net/linux/latest/install",
        "/bin/bash install",
        "/etc/init.d/awsagent start",
        "echo -e '\n[INFO] Starting assessment...'",
        "assessment_run_arn=`aws inspector start-assessment-run --assessment-template-arn {} --region {} | jq -r .assessmentRunArn`",
        "status='unknown'",
        "timeout_s={}",
        "sleep_s=60",
        "repeates=$((timeout_s/sleep_s))",
        "echo -e '\n[INFO] Monitoring assessment status...'",
        "while [[ $status != 'COMPLETED' && $status != 'FAILED' && $repeates -gt 1 ]]; do",
        "let repeates=$repeates-1",
        "status=`aws inspector describe-assessment-runs --assessment-run $assessment_run_arn --region {} | jq -r .assessmentRuns[].state`",
        "echo `date` '| STATUS:' $status",
        "sleep $sleep_s",
        "done",
        "echo -e '\n[INFO] Assessment finished.'",
        "echo '[INFO] Shutting down...'",
        "/sbin/shutdown -h now" 
      ]).format(assessment_template_arn, aws_region, timeout_s, aws_region), 
      IamInstanceProfile={'Arn': instance_profile_arn},
      InstanceInitiatedShutdownBehavior='terminate'
    )
    instance_id = response.get("Instances")[0].get("InstanceId")
    return instance_id
  except ClientError as error:
    print('[ERR] '+str(error))
    raise

def handler(event, context):
  # event received
  print('[INFO] Event received: '+str(event))

  # prep
  session    = boto3.session.Session()
  aws_region = session.region_name
  nodetype   = event.get('nodetype')

  # environment variables capture
  instance_type           = os.environ['instance_type']
  subnet_id               = os.environ['subnet_id']
  security_group_id       = os.environ['security_group_id']
  instance_profile_arn    = os.environ['instance_profile_arn']
  assessment_template_arn = os.environ['assessment_template_arn']
  timeout_s               = os.environ['timeout_s']
  
  # create ec2 service client
  ec2_conn = boto3.client('ec2', region_name=aws_region)
  
  # get latest AMI ID based on filters
  ami_filter = [
    { 'Name': 'tag:nodetype', 'Values': [nodetype] },
    { 'Name': 'state', 'Values': ['available'] }
  ]
  latest_ami = get_latest_ami(ec2_conn, ami_filter, nodetype)
  print('[INFO] The latest AMI:')
  print('ImageId: '+latest_ami['ImageId']+' | Name: '+latest_ami['Name']+' | CreationDate: '+latest_ami['CreationDate'])
  
  # run ec2 instance and start AWS Inspector assessment
  tags = [
    { 'Key': 'Name', 'Value': 'aws-inspector-test-'+nodetype },
    { 'Key': 'AWSInspectorScan', 'Value': 'true' },
    { 'Key': 'Nodetype', 'Value': nodetype },
    { 'Key': 'ImageId', 'Value': latest_ami['ImageId'] }
  ]
  test_instance_id = run_ec2(
    ec2_conn,
    aws_region,
    latest_ami['ImageId'],
    instance_type,
    subnet_id,
    security_group_id,
    instance_profile_arn,
    assessment_template_arn,
    timeout_s,
    tags
  )
  print('[INFO] Test Instance started: '+test_instance_id)

  # the end
  print('[INFO] All good! Quitting...')
