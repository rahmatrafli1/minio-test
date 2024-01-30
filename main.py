from minio import Minio
from minio.error import S3Error
import os
from dotenv import load_dotenv

load_dotenv()

def main():
    client = Minio(os.environ.get('LOCAL_MINIO'), 
                   access_key=os.environ.get('ACCESS_KEY_MINIO'),
                   secret_key=os.environ.get('SECRET_KEY_MINIO'),
                   secure=False)
    
    source_file = "/home/diginsight/Documents/Minio/Test/tmp/minio/my-test.txt"

    bucket_name = "python-test-bucket"

    destination_file = "my-test.txt"

    found = client.bucket_exists(bucket_name)
    if not found:
        client.make_bucket(bucket_name)
        print(f"Bucket {bucket_name} created")
    else:
        print(f"Bucket {bucket_name} already exists")

    client.fput_object(bucket_name, destination_file, source_file)
    print(f"File {destination_file} uploaded to bucket {bucket_name}")

if __name__ == "__main__":
    try:
        main()
    except S3Error as e:
        print(e)