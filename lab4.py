import boto3
import botocore.session
import os
session = botocore.session.get_session()
ACCESS_KEY = session.get_credentials().access_key
SECRET_KEY = session.get_credentials().secret_key
REGION = session.get_config_variable('region')
def create_key_pair(name):
    ec2_client = boto3.client('ec2', region_name=REGION, aws_access_key_id = ACCESS_KEY, aws_secret_access_key = SECRET_KEY)
    try:
        key_pair = ec2_client.create_key_pair(KeyName=name)
        private_key = key_pair["KeyMaterial"]
        with os.fdopen(os.open(f"/tmp/{name}.pem", os.O_WRONLY | os.O_CREAT, 0o400), "w+") as handle:
            handle.write(private_key)
    except botocore.exceptions.ClientError:
        print("Error! Key pair with such name already exists. Please try another one.")

#create_key_pair('t')
img_vars = {"amazon_linux": "ami-0577c11149d377ab7", "ubuntu_64-bit(x86)":"ami-064087b8d355e9051", "windows_64-bit (x86)":"ami-0d273da0d944870e5"}
#print(img_vars)
def create_instance(keyname, img, instance_type="t3.micro", mincount=1, maxcount=1):
    ec2_client = boto3.client('ec2', region_name=REGION, aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
    if img_vars.__contains__(img):
        try:
            instances = ec2_client.run_instances(
                ImageId=img_vars[img],
                MinCount=mincount,
                MaxCount=maxcount,
                InstanceType=instance_type,
                KeyName=keyname
            )
            print(instances["Instances"][0]["InstanceId"])
        except botocore.exceptions.ClientError as e:
            print("Error! Check your credentials.", e)
    else:
        print("Error! Check your image type.")


#create_instance('t', 'amazon_linux')
def get_running_instances():
    ec2_client = boto3.client('ec2', region_name=REGION, aws_access_key_id = ACCESS_KEY, aws_secret_access_key = SECRET_KEY)
    reservations = ec2_client.describe_instances(Filters=[
        {
            "Name": "instance-state-name",
            "Values": ["running"],
        },
        {
            "Name": "instance-type",
            "Values": ["t3.micro"]
        }
    ]).get("Reservations")
    for reservation in reservations:
        for instance in reservation["Instances"]:
            instance_id = instance["InstanceId"]
            instance_type = instance["InstanceType"]
            public_ip = instance["PublicIpAddress"]
            private_ip = instance["PrivateIpAddress"]
            print(f"The instance {instance_id}, {instance_type}, {public_ip}, {private_ip} is running now.")

#get_running_instances()
def stop_instance(instance_id):
    ec2_client = boto3.client('ec2', region_name=REGION, aws_access_key_id = ACCESS_KEY, aws_secret_access_key = SECRET_KEY)
    try:
        resp = ec2_client.stop_instances(InstanceIds=[instance_id])
        print(f"The instance {instance_id} is stopped now.")
    except botocore.exceptions.ClientError as e:
        print("Error!", e)
#stop_instance("i-040199f6a646405ee")
def start_instance(instance_id):
    ec2_client = boto3.client('ec2', region_name=REGION, aws_access_key_id = ACCESS_KEY, aws_secret_access_key = SECRET_KEY)
    try:
        ec2_client.start_instances(InstanceIds=[instance_id])
        print(f"The instance {instance_id} was started.")
    except botocore.exceptions.ClientError as e:
        print("Error!: ", e)
#start_instance("i-040199f6a646405ee")
def terminate_instance(instance_id):
    ec2_client = boto3.client('ec2', region_name=REGION, aws_access_key_id = ACCESS_KEY, aws_secret_access_key = SECRET_KEY)
    try:
        ec2_client.terminate_instances(InstanceIds=[instance_id])
        print(f"The instance {instance_id} is terminated now.")
    except botocore.exceptions.ClientError as e:
        print("Error!: ", e)
#terminate_instance("i-040199f6a646405ee")
def create_bucket(bucket_name):
    s3_client = boto3.client('s3', region_name=REGION, aws_access_key_id = ACCESS_KEY, aws_secret_access_key = SECRET_KEY)
    location = {'LocationConstraint': REGION}
    try:
        s3_client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration=location)
    except botocore.exceptions.ClientError as e:
        print("Error!", e)

#create_bucket('bucketformylab4')
def list_buckets():
    s3_client = boto3.client('s3', region_name=REGION, aws_access_key_id = ACCESS_KEY, aws_secret_access_key = SECRET_KEY)
    print('Existing buckets:')
    # Output the bucket names
    try:
        response = s3_client.list_buckets()
        for bucket in response['Buckets']:
            print(f'  {bucket["Name"]}')
    except botocore.exceptions.ClientError as e:
        print("Error!", e)
#list_buckets()
import pandas
def get_files(bucket_name, file_name):
    s3_client = boto3.client('s3', region_name=REGION, aws_access_key_id = ACCESS_KEY, aws_secret_access_key = SECRET_KEY)
    try:
        obj = s3_client.get_object(Bucket = bucket_name, Key = file_name)
        data = pandas.read_csv(obj['Body'])
        # Print the data frame
        print('Printing the data frame...')
        print(data.head())
    except botocore.exceptions.ClientError as e:
        print("Error!", e)
#get_files('buketforlabs','curr1_usd.csv')
def upload_files(file_name, bucket_name):
    object_name = os.path.basename(file_name)
    s3_client = boto3.client('s3', region_name=REGION, aws_access_key_id = ACCESS_KEY, aws_secret_access_key = SECRET_KEY)
    try:
        response = s3_client.upload_file(file_name, bucket_name, object_name)
    except botocore.exceptions.ClientError as e:
        print('Error!',e)
        return False
    return True
#upload_files("curr1_usd.csv","buketforlabs")
def del_bucket(bucket_name):
    s3_client = boto3.client('s3', region_name=REGION, aws_access_key_id = ACCESS_KEY, aws_secret_access_key = SECRET_KEY)
    location = {'LocationConstraint': REGION}
    try:
        s3_client.delete_bucket(Bucket=bucket_name)
    except botocore.exceptions.ClientError as e:
        print("Error!", e)
#del_bucket('bucketformylab4')