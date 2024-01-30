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

if __name__ == "__main__":
    try:
        main()
        # rename_bucket("python-test-bucket", "test-bucket")
    except S3Error as e:
        print(e)