import json
from google.cloud import storage

def gcs_trigger(event):
    data = event.data
    data_str = data.decode('utf-8')

    # Converter a string para um dicionário
    data_dict = json.loads(data_str)

    bucket_name = 'challenger-data-enginner-logs'
    storage_client = storage.Client() 
    bucket = storage_client.bucket(bucket_name)
    blobs = storage_client.list_blobs(bucket_name)

    for blob in blobs:
        extensao = blob.name.split(".")[1]
        print(extensao)
        if extensao == "json":
            blob_name = blob.name
        else:
            return "error"

    # Extrair o ID e o nome da bucket
    object_id = data_dict['id']
    object_name = data_dict['name']
    object_type = data_dict['contentType']
    object_updated = data_dict['updated']
    bucket_name_output = data_dict['bucket']
    bucket_storage = data_dict['storageClass']

    blob = bucket.get_blob(blob_name)
    contents = blob.download_as_text()

    attributs = [object_name, object_id, object_type, object_updated, bucket_name_output, bucket_storage]
    referencia = [
        {
            "name": str,
            "id":int,
            "object type": str,
            "Object update": str,
            "bucket name": str,
            "bucket storage": str
        }
    ]

    if len(contents) == 0:
        log = []
    else:
        log = json.loads(contents)

    for index, values in enumerate(referencia[0]):
        referencia[0][values] = attributs[index]
    
    log.append(referencia[0])
    json_save = json.dumps(log,indent=4)
    blob.upload_from_string(json_save)

    print('-----------------------------//------------------------------')
    print(f"ID do objeto: {object_id}")
    print(f"Nome do objeto: {object_name}")
    print(f"Tipo do objeto: {object_type}")
    print(f"Atualização do objeto: {object_updated}")
    print(f"Nome da Bucket: {bucket_name_output}")
    print(f"Classe de armazenamento: {bucket_storage}")

