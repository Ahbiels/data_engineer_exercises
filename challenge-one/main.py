import requests
import os 
from pydantic import BaseModel
from fastapi import FastAPI, Path
from google.cloud import storage

app = FastAPI()

class Params(BaseModel):
    url: str
    bucket_name: str
    output_file_prefix: str

# url = "https://static.poder360.com.br/2022/01/flycar6-848x477.jpg"
# bucket = "upload_objects_data_engineer_challenger"

directory_path = "tmp_directory"

def upload_object(bucket_name, source_file_name, destination_blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    generation_match_precondition = 0

    blob.upload_from_filename(source_file_name, if_generation_match=generation_match_precondition)
    return f"File {source_file_name} uploaded to {destination_blob_name}."

@app.get("/")
def index():
    return {"Response":"Server is works"}

@app.post("/add-object")
def create_object(params: Params = Path(description = "object to be stored in the bucket")):
    if not os.path.exists(directory_path):
        os.mkdir(directory_path)
    response = requests.get(params.url)
    suffix = os.path.splitext(params.url)[1] #pega a extesão do arquivo

    object_params = f"{params.output_file_prefix}{suffix}"
    
    with open(f"{directory_path}/{object_params}","wb") as content:
        content.write(response.content)
        upload_object(params.bucket_name, f"{directory_path}/{object_params}", object_params) 
    return 200

"""
Comandos utilizados para instalacao:
- pip3 install fastapi uvicorn 
- pip install --upgrade google-cloud-storage

Comandos utilizados para configuracao
- virtualenv first-challenger
- source first-challenger/bin/activate
- gcloud auth list
- gcloud config set project [project ID]
- gcloud config list
- uvicorn main:app --reload

Acessar a api
- http://127.0.0.1:8000/docs -> localhost
"""