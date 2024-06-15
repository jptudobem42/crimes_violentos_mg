#%%
import boto3
from botocore.exceptions import ClientError
import os
import logging
from dotenv import load_dotenv
from tqdm import tqdm
import shutil
#%%
load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
REGION = os.getenv("REGION")
#%%
logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(asctime)s - %(message)s')
#%%
def upload_file(data_dir, raw_dir, bucket, s3_directory):
    s3_client = boto3.client('s3', region_name=REGION,
                            aws_access_key_id=AWS_ACCESS_KEY_ID,
                            aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

    if not os.path.exists(data_dir):
        raise ValueError(f"Diretório {data_dir} não existe.")

    files = []
    for root, dirs, filenames in os.walk(data_dir):
        for filename in filenames:
            file_path = os.path.join(root, filename)
            files.append(file_path)

    if files:
        for file_path in tqdm(files, desc="Fazendo upload para o S3"):
            try:
                relative_path = os.path.relpath(file_path, data_dir)
                s3_path = os.path.join(s3_directory, relative_path).replace("\\", "/")
                s3_client.upload_file(file_path, bucket, s3_path)
            except ClientError as e:
                logging.error(f"Erro ao fazer upload do {file_path} para S3: {e}")
        
        shutil.rmtree(raw_dir)
    else:
        logging.info(f"Nenhum arquivo encontrado em '{data_dir}'.")
#%%
data_dir="../data/"
raw_dir = "../data/raw/"
bucket = "credencial-datalake"
s3_directory = "despesas-mg"
upload_file(data_dir, raw_dir, bucket, s3_directory)