import json
import os
import boto3
from datetime import datetime
from dotenv import load_dotenv

#Carregando variáveis de ambiente
load_dotenv()

def save_recently_played_raw_to_s3(data: dict) -> str:
    """
    Envia os dados brutos (JSON) da API do Spotify diretamente para o S3
    """
    # 1. Configuração do Client S3
    s3_client = boto3.client(
        's3',
        aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name = os.getenv('AWS_REGION')
    )

    bucket_name = os.getenv('S3_BUCKET_NAME')

    #2. Definir a estrutura de "pastas" (prefixos)
    #Mantendo o seu padrão de particionamento
    
    extraction_date = datetime.now().strftime("%Y-%m-%d")
    extraction_time = datetime.now().strftime("%Y%m%dT%H%M%S")


    #3. Caminho dentro do S3
    s3_key = f"raw/spotify/recently_played/extraction_date={extraction_date}/recently_played_{extraction_time}.json"

    #4. Fazendo upload
    try:
        s3_client.put_object(
            Bucket = bucket_name,
            Key = s3_key,
            Body = json.dumps(data, ensure_ascii = False, indent = 2),
            ContentType = 'application/json'
        )
    except Exception as e:
        print(f"❌ Erro ao salvar no S3: {e}")
        raise
