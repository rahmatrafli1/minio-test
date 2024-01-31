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
    
    file_multi = ['python/hai.py', 'python/ifelse.py', 'python/hello.py', 'text/my-test.txt']
    
    for files in file_multi:
        source_file = f"/home/diginsight/Documents/Minio/Test/tmp/minio/{files}"
        
        bucket_name = "test-bucket"
        
        destination_file = files

        found = client.bucket_exists(bucket_name)
        if not found:
            client.make_bucket(bucket_name)
            print(f"Bucket {bucket_name} created")
        else:
            print(f"Bucket {bucket_name} already exists")

        client.fput_object(bucket_name, destination_file, source_file)
        print(f"File {destination_file} uploaded to bucket {bucket_name}")

# ganti nama bucket
def rename_bucket(old_bucket_name, new_bucket_name):
    # Inisialisasi koneksi ke MinIO
    client = Minio(os.environ.get('LOCAL_MINIO'),
                   access_key=os.environ.get('ACCESS_KEY_MINIO'),
                   secret_key=os.environ.get('SECRET_KEY_MINIO'),
                   secure=False)

    try:
        # Periksa apakah bucket lama ada
        found_old_bucket = client.bucket_exists(old_bucket_name)
        if not found_old_bucket:
            print(f"Bucket {old_bucket_name} tidak ditemukan.")
            return

        # Buat bucket baru dengan nama baru
        found_new_bucket = client.bucket_exists(new_bucket_name)
        if not found_new_bucket:
            client.make_bucket(new_bucket_name)
            print(f"Bucket {new_bucket_name} dibuat.")

        # Pindahkan objek dari bucket lama ke bucket baru
        objects = client.list_objects(old_bucket_name)
        for obj in objects:
            source = f'/{old_bucket_name}/{obj.object_name}'
            destination = f'/{new_bucket_name}/{obj.object_name}'
            client.copy_object(new_bucket_name, obj.object_name, source, metadata=obj.metadata)
            print(f"Objek {obj.object_name} dipindahkan dari {old_bucket_name} ke {new_bucket_name}.")

        # Hapus bucket lama jika perlu
        client.remove_bucket(old_bucket_name)
        print(f"Bucket {old_bucket_name} dihapus.")

    except S3Error as e:
        print(f"Terjadi kesalahan MinIO: {e}")

# list buckets
def list_buckets():
    client = Minio(os.environ.get('LOCAL_MINIO'), 
                   access_key=os.environ.get('ACCESS_KEY_MINIO'),
                   secret_key=os.environ.get('SECRET_KEY_MINIO'),
                   secure=False)
    
    buckets = client.list_buckets()

    for bucket in buckets:
        print("Name:", bucket.name,"Creation time:", bucket.creation_date)

# delete object
def delete_object_on_bucket():
    client = Minio(os.environ.get('LOCAL_MINIO'), 
                   access_key=os.environ.get('ACCESS_KEY_MINIO'),
                   secret_key=os.environ.get('SECRET_KEY_MINIO'),
                   secure=False)
    
    b_name = "test-bucket"
    o_name = "python/hello.py"

    client.remove_object(b_name, o_name)

def list_objects_on_bucket():
    client = Minio(os.environ.get('LOCAL_MINIO'), 
                   access_key=os.environ.get('ACCESS_KEY_MINIO'),
                   secret_key=os.environ.get('SECRET_KEY_MINIO'),
                   secure=False)
    
    b_name = "test-bucket"

    objects = client.list_objects(b_name, include_version=True, recursive=True)

    for obj in objects:
        print(obj)

# download a stream
def download_stream_minio():
    client = Minio(os.environ.get('LOCAL_MINIO'), 
                   access_key=os.environ.get('ACCESS_KEY_MINIO'),
                   secret_key=os.environ.get('SECRET_KEY_MINIO'),
                   secure=False)
    
    b_name = "test-bucket"
    o_name = "text/my-test.txt"

    try:
        response = client.get_object(b_name, o_name, 0, 11)
        print(response.read())
    finally:
        response.close()
        response.release_conn()

# upload a file
def upload_file_minio():
    client = Minio(os.environ.get('LOCAL_MINIO'), 
                   access_key=os.environ.get('ACCESS_KEY_MINIO'),
                   secret_key=os.environ.get('SECRET_KEY_MINIO'),
                   secure=False)
    
    b_name = "test-bucket"
    o_name = "text/lorem.txt"
    source_file = f"/home/diginsight/Documents/Minio/Test/tmp/minio/{o_name}"
    c_type = "text/plain"

    client.fput_object(b_name, o_name, source_file, c_type)
    print(f"File {o_name} uploaded to bucket {b_name}")

if __name__ == "__main__":
    try:
        # main()
        # rename_bucket("python-test-bucket", "test-bucket")
        # list_buckets()
        # delete_object_on_bucket()
        # list_objects_on_bucket()
        # download_stream_minio()
        upload_file_minio()

    except S3Error as e:
        print(e)