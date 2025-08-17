# Exemplo de configuração para bulk_run.py

# Configurações da Lambda
LAMBDA_FUNCTION_NAME = 'lambdownload-function'
MAX_CONCURRENT_EXECUTIONS = 2

# Configurações do S3
S3_BUCKET_NAME = 'meu-bucket-dados'
S3_PREFIX = 'dados-fluxo/'

# URL base dos arquivos
BASE_URL = 'https://opendatasus.saude.gov.br/dataset/covid-19-vacinacao/resource/'

# Lista de arquivos para download
FILES_TO_DOWNLOAD = [
    "Dados_Fluxo_Parte_1.zip",
    "Dados_Fluxo_Parte_2.zip",
    "Dados_Fluxo_Parte_3.zip",
    "Dados_Fluxo_Parte_4.zip",
    "Dados_Fluxo_Parte_5.zip"
]

# Exemplo para COVID-19 vacinação
COVID_VACCINATION_CONFIG = {
    'function_name': 'lambdownload-function',
    'max_concurrent': 3,
    'bucket': 'covid-data-bucket',
    'prefix': 'vacinacao/',
    'base_url': 'https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SIPNI/COVID/completo/',
    'files': [
        'part-00000-70dd7710-b64c-4a6e-a780-bf4ca7d0a1f7-c000.csv'
    ]
}