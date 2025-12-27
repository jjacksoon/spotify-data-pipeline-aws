import json
import os
import boto3
import pandas as pd 
from datetime import datetime
from dotenv import load_dotenv
from io import StringIO, BytesIO
from sqlalchemy import create_engine # Importação necessária para conectar ao banco

#Carregando variáveis de ambiente
load_dotenv()

#Configurações AWS
BUCKET_NAME = os.getenv('S3_BUCKET_NAME')
s3_client = boto3.client('s3')

#Leitura da camada bronze
def read_raw_files_from_s3():
    all_items = []
    
    # list_objects_v2: Pede à AWS uma lista de tudo que está no bucket dentro da pasta 'raw/...'
    response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix="raw/spotify/recently_played/")
    
    # Se a chave 'Contents' não existir, significa que a pasta está vazia ou não existe
    if 'Contents' not in response:
        print("⚠️ Nenhum arquivo RAW encontrado no S3.")
        return []

    # O 'response['Contents']' é uma lista de dicionários com metadados de cada arquivo encontrado
    for obj in response['Contents']:
        key = obj['Key'] # Pega o "caminho" do arquivo (ex: raw/spotify/.../arquivo.json)
        
        # Filtramos para garantir que estamos lendo apenas arquivos JSON
        if key.endswith(".json"):
            print(f"📄 Lendo RAW do S3: {key}")
            
            # get_object: Realmente "baixa" o conteúdo do arquivo do S3 para a memória do Python
            content = s3_client.get_object(Bucket=BUCKET_NAME, Key=key)
            
            # content['Body'].read(): Lê os bytes do arquivo
            # .decode('utf-8'): Transforma bytes em texto (string)
            # json.loads(): Transforma o texto JSON em um dicionário Python
            data = json.loads(content['Body'].read().decode('utf-8'))
            
            # Pega a lista de músicas (items) e adiciona na nossa lista geral
            items = data.get("items", [])
            all_items.extend(items)
            
    print(f"✅ Fim da leitura. Total de itens na lista all_items: {len(all_items)}")
    return all_items

def transform_items(items: list) -> pd.DataFrame:
    rows = []

    for item in items:
        track = item.get("track", {})
        album = track.get("album", {})
        artists = track.get("artists", [])

        rows.append({
            "played_at": item.get("played_at"),
            "track_id": track.get("id"),
            "track_name": track.get("name"),
            "duration_ms": track.get("duration_ms"),
            "popularity": track.get("popularity"),
            "explicit": track.get("explicit"),
            "album_id": album.get("id"),
            "album_name": album.get("name"),
            "album_release_date": album.get("release_date"),
            "artist_id": artists[0].get("id") if artists else None,
            "artist_name": artists[0].get("name") if artists else None,
            "load_date": datetime.now().date()
        })

    df = pd.DataFrame(rows)
    

    if df.empty:
        print("⚠️ Silver: DataFrame vazio — nenhuma linha gerada")
        return df

    if "played_at" in df.columns:
        df["played_at"] = pd.to_datetime(df["played_at"], errors="coerce")

        '''coerce - se algum valor não puder ser convertido para datetime, 
        ele será transformado em NaT (Not a Time), evitando erros de execução.'''

    if "album_release_date" in df.columns:
        df["album_release_date"] = pd.to_datetime(
            df["album_release_date"], errors="coerce"
        ).dt.date

    return df


def save_silver_to_s3(df_new: pd.DataFrame):
    # Define o caminho (chave) onde o ficheiro consolidado será guardado no S3
    silver_key = "silver/recently_played.csv" 
    
    try:
        # Tenta ler o ficheiro CSV que já existe na camada Silver do S3
        response = s3_client.get_object(Bucket=BUCKET_NAME, Key=silver_key)
        
        # Converte o conteúdo do ficheiro lido num DataFrame Pandas para podermos comparar
        df_existing = pd.read_csv(response['Body'], parse_dates=["played_at"])
        print("📂 Silver existente carregada do S3.")
        
        # Faz um 'merge' (união) entre os dados novos e os existentes para identificar duplicados
        # Usamos track_id e played_at como chaves exclusivas de cada música tocada
        df_merged = df_new.merge(
            df_existing[["track_id", "played_at"]],
            on=["track_id", "played_at"],
            how="left",
            indicator=True
        )
        
        # Filtra apenas as linhas que existem apenas no 'df_new' (ou seja, músicas novas)
        df_to_insert = df_merged[df_merged["_merge"] == "left_only"].drop(columns="_merge")
        
        # Cria o DataFrame final juntando o histórico antigo com as novidades
        df_final = pd.concat([df_existing, df_to_insert], ignore_index=True)
        
    except s3_client.exceptions.NoSuchKey:
        # Se o ficheiro não existir no S3 (primeira execução), o 'df_final' será o próprio 'df_new'
        print("✨ Criando novo arquivo Silver no S3.")
        df_final = df_new
        df_to_insert = df_new

    # --- BLOCO DE SALVAMENTO NO S3 ---
    # Só fazemos o upload para o S3 se realmente houver músicas novas (para poupar recursos)
    if not df_to_insert.empty:
        # Cria um buffer de memória (ficheiro virtual) para não precisar de guardar no disco local
        csv_buffer = StringIO()
        # Converte o DataFrame para formato CSV e guarda-o nesse buffer
        df_final.to_csv(csv_buffer, index=False)
        
        # Faz o upload (sobrescreve) o ficheiro no S3 com os dados atualizados
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=silver_key,
            Body=csv_buffer.getvalue(),
            ContentType='text/csv'
        )
        print(f"✅ Silver atualizada no S3: {len(df_to_insert)} novos registros.")
    else:
        # Se não houver nada novo, apenas avisamos no log
        print("⚠️ Sem registros novos para adicionar ao arquivo S3.")

    # --- BLOCO DE SINCRONIZAÇÃO COM O RDS (DBEAVER) ---
    # Este bloco corre SEMPRE para garantir que o banco de dados tem o mesmo que o S3
    try:
        print("🚀 Sincronizando dados com o RDS...")
        
        # Obtém as credenciais das variáveis de ambiente (.env)
        user = os.getenv("DB_USER")
        password = os.getenv("DB_PASSWORD")
        host = os.getenv("DB_HOST")
        port = os.getenv("DB_PORT")
        database = os.getenv("DB_NAME")

        # Cria a conexão (engine) com o motor PostgreSQL do Amazon RDS
        engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')

        # Envia o 'df_final' (histórico completo) para a tabela no schema 'silver'
        # if_exists='replace': Apaga a tabela antiga e cria uma nova com os 80 registos atuais
        df_final.to_sql(
            'recently_played', 
            con=engine, 
            schema='silver', 
            if_exists='replace', 
            index=False
        )
        
        # Faz uma pequena consulta SQL para confirmar ao utilizador quantas linhas estão no banco
        check_df = pd.read_sql("SELECT count(*) FROM silver.recently_played", engine)
        print(f"📊 Confirmação do Banco: A tabela agora tem {check_df.iloc[0,0]} linhas.")
        print("💎 Dados carregados com sucesso no RDS (DBeaver atualizado)!")
    
    except Exception as e:
        # Captura qualquer erro de conexão ou de SQL para não travar o pipeline
        print(f"❌ Erro ao carregar dados no RDS: {e}")

def run_silver():
    items = read_raw_files_from_s3()
    print(f"🔎 Total de items lidos da Bronze: {len(items)}")
    df = transform_items(items)
    save_silver_to_s3(df)
