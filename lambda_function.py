import sys
import subprocess
subprocess.check_call([sys.executable, "-m", "pip", "install", "requests"])
import requests
import boto3
import json
import logging
import requests
import io
import os
import time
from urllib.parse import urlparse
from botocore.exceptions import ClientError

# Configurar logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    """
    Função Lambda para fazer download de URL HTTPS e salvar no S3

    Parâmetros esperados no event:
    - url: URL HTTPS do arquivo para download
    - bucket: nome do bucket S3 (opcional, pode usar variável de ambiente)
    - prefix: prefixo/pasta no S3 (opcional, pode usar variável de ambiente)
    - filename: nome do arquivo no S3 (opcional, extrai da URL se não fornecido)
    """

    # Obter parâmetros do evento ou variáveis de ambiente
    url = event.get('url')
    bucket = event.get('bucket') or os.environ.get('S3_BUCKET')
    prefix = event.get('prefix') or os.environ.get('S3_PREFIX', '')
    custom_filename = event.get('filename')

    # Validações
    if not url:
        return {
            'statusCode': 400,
            'body': json.dumps({
                'error': 'Parâmetro "url" é obrigatório',
                'example': {
                    'url': 'https://example.com/arquivo.zip',
                    'bucket': 'meu-bucket',
                    'prefix': 'dados/',
                    'filename': 'arquivo_customizado.zip'
                }
            })
        }

    if not bucket:
        return {
            'statusCode': 400,
            'body': json.dumps({
                'error': 'Parâmetro "bucket" é obrigatório (via evento ou variável de ambiente S3_BUCKET)'
            })
        }

    # Extrair nome do arquivo da URL se não fornecido
    if custom_filename:
        filename = custom_filename
    else:
        parsed_url = urlparse(url)
        filename = os.path.basename(parsed_url.path)
        if not filename:
            filename = 'downloaded_file'

    # Construir chave S3
    if prefix and not prefix.endswith('/'):
        prefix += '/'
    s3_key = f"{prefix}{filename}" if prefix else filename

    logger.info(f"Iniciando download de: {url}")
    logger.info(f"Destino S3: s3://{bucket}/{s3_key}")

    start_time = time.time()

    try:
        # Inicializar cliente S3
        s3_client = boto3.client('s3')

        # Verificar se arquivo já existe no S3
        try:
            s3_client.head_object(Bucket=bucket, Key=s3_key)
            logger.info(f"⚠️ Arquivo {filename} já existe no S3")
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': f'Arquivo {filename} já existe no S3',
                    'status': 'skipped',
                    's3_location': f's3://{bucket}/{s3_key}',
                    'url': url
                })
            }
        except ClientError as e:
            if e.response['Error']['Code'] != '404':
                raise
            # Arquivo não existe, pode prosseguir

        # Configurar headers para o download
        headers = {
            'User-Agent': 'AWS-Lambda-HTTPS-Downloader/1.0'
        }

        # Fazer requisição HEAD para obter informações do arquivo
        logger.info("🔍 Verificando informações do arquivo...")
        head_response = requests.head(url, headers=headers, timeout=30, allow_redirects=True)
        head_response.raise_for_status()

        # Obter tamanho do arquivo
        content_length = head_response.headers.get('content-length')
        file_size = int(content_length) if content_length else None

        if file_size:
            logger.info(f"📏 Tamanho do arquivo: {file_size / (1024 * 1024):.2f} MB")
        else:
            logger.info("📏 Tamanho do arquivo: Desconhecido")

        # Fazer download com streaming
        logger.info("📥 Iniciando download...")
        download_start = time.time()

        with requests.get(url, headers=headers, stream=True, timeout=60) as response:
            response.raise_for_status()

            # Verificar se o servidor suporta range requests
            accepts_ranges = response.headers.get('accept-ranges') == 'bytes'
            logger.info(f"🔄 Suporte a range requests: {accepts_ranges}")

            # Criar buffer em memória
            file_buffer = io.BytesIO()

            # Download com progresso
            downloaded_bytes = 0
            chunk_size = 8192  # 8KB chunks

            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:
                    file_buffer.write(chunk)
                    downloaded_bytes += len(chunk)

                    # Log de progresso a cada 10MB
                    if downloaded_bytes % (10 * 1024 * 1024) == 0:
                        mb_downloaded = downloaded_bytes / (1024 * 1024)
                        if file_size:
                            progress = (downloaded_bytes / file_size) * 100
                            logger.info(f"📥 Progresso: {mb_downloaded:.1f} MB ({progress:.1f}%)")
                        else:
                            logger.info(f"📥 Progresso: {mb_downloaded:.1f} MB baixados")

            # Finalizar download
            final_size = file_buffer.tell()
            file_buffer.seek(0)

            download_time = time.time() - download_start
            logger.info(f"✅ Download concluído: {final_size / (1024 * 1024):.2f} MB em {download_time:.2f}s")

            # Upload para S3
            logger.info("☁️ Iniciando upload para S3...")
            upload_start = time.time()

            # Preparar metadados
            metadata = {
                'source-url': url,
                'download-date': str(int(time.time())),
                'original-filename': filename,
                'file-size': str(final_size)
            }

            # Adicionar Content-Type se disponível
            content_type = response.headers.get('content-type', 'application/octet-stream')

            # Upload com configuração otimizada
            if final_size > 100 * 1024 * 1024:  # > 100MB
                logger.info("📤 Usando multipart upload para arquivo grande")
                s3_client.upload_fileobj(
                    file_buffer,
                    bucket,
                    s3_key,
                    Config=boto3.s3.transfer.TransferConfig(
                        multipart_threshold=1024 * 25,  # 25MB
                        max_concurrency=10,
                        multipart_chunksize=1024 * 25,
                        use_threads=True
                    ),
                    ExtraArgs={
                        'ServerSideEncryption': 'AES256',
                        'Metadata': metadata,
                        'ContentType': content_type
                    }
                )
            else:
                # Upload normal para arquivos menores
                s3_client.upload_fileobj(
                    file_buffer,
                    bucket,
                    s3_key,
                    ExtraArgs={
                        'ServerSideEncryption': 'AES256',
                        'Metadata': metadata,
                        'ContentType': content_type
                    }
                )

            upload_time = time.time() - upload_start
            total_time = time.time() - start_time

            logger.info("🎉 Upload completo!")
            logger.info(f"📊 Estatísticas:")
            logger.info(f"   - URL: {url}")
            logger.info(f"   - Arquivo: {filename}")
            logger.info(f"   - Tamanho: {final_size / (1024 * 1024):.2f} MB")
            logger.info(f"   - Tempo download: {download_time:.2f}s")
            logger.info(f"   - Tempo upload: {upload_time:.2f}s")
            logger.info(f"   - Tempo total: {total_time:.2f}s")
            logger.info(f"   - Velocidade média: {(final_size / (1024 * 1024)) / total_time:.2f} MB/s")
            logger.info(f"   - Localização S3: s3://{bucket}/{s3_key}")

            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': f'Arquivo {filename} transferido com sucesso',
                    'status': 'completed',
                    'stats': {
                        'url': url,
                        'filename': filename,
                        'size_mb': round(final_size / (1024 * 1024), 2),
                        'download_time_seconds': round(download_time, 2),
                        'upload_time_seconds': round(upload_time, 2),
                        'total_time_seconds': round(total_time, 2),
                        'average_speed_mbps': round((final_size / (1024 * 1024)) / total_time, 2),
                        'content_type': content_type
                    },
                    's3_location': f's3://{bucket}/{s3_key}'
                })
            }

    except requests.exceptions.RequestException as e:
        error_msg = f"Erro no download da URL {url}: {str(e)}"
        logger.error(error_msg)
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Falha no download',
                'url': url,
                'error': error_msg,
                'status': 'failed'
            })
        }

    except ClientError as e:
        error_msg = f"Erro no upload para S3: {str(e)}"
        logger.error(error_msg)
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Falha no upload para S3',
                'url': url,
                'bucket': bucket,
                'key': s3_key,
                'error': error_msg,
                'status': 'failed'
            })
        }

    except Exception as e:
        error_msg = f"Erro inesperado ao processar {url}: {str(e)}"
        logger.error(error_msg)
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Erro inesperado',
                'url': url,
                'error': error_msg,
                'status': 'failed'
            })
        }
