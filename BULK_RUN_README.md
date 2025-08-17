# Bulk Run - Execução em Lote da Lambda

Este módulo permite executar downloads em lote usando a função Lambda de download.

## Arquivos

- `bulk_run.py` - Versão original atualizada para funcionar com lambda_function.py
- `bulk_run_configurable.py` - Versão configurável e mais flexível
- `config_example.py` - Exemplos de configuração

## Como Usar

### 1. Configurar AWS

```bash
aws configure
```

### 2. Usar a Versão Configurável (Recomendado)

```python
from bulk_run_configurable import process_files_with_config

# Configuração para dados do COVID-19
config = {
    'function_name': 'lambdownload-function',
    'max_concurrent': 2,
    'bucket': 'meu-bucket-dados',
    'prefix': 'covid/',
    'base_url': 'https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SIPNI/COVID/completo/',
    'files': [
        'part-00000-70dd7710-b64c-4a6e-a780-bf4ca7d0a1f7-c000.csv'
    ]
}

process_files_with_config(config)
```

### 3. Executar Diretamente

```bash
python bulk_run_configurable.py
```

## Configuração

### Parâmetros Obrigatórios

- `function_name`: Nome da função Lambda
- `bucket`: Bucket S3 de destino
- `base_url`: URL base dos arquivos
- `files`: Lista de nomes de arquivos

### Parâmetros Opcionais

- `max_concurrent`: Execuções simultâneas (padrão: 2)
- `prefix`: Prefixo/pasta no S3 (padrão: '')

## Exemplo Completo

```python
# Configuração para múltiplos arquivos
config = {
    'function_name': 'lambdownload-function',
    'max_concurrent': 3,
    'bucket': 'dados-publicos',
    'prefix': 'saude/covid/',
    'base_url': 'https://opendatasus.saude.gov.br/dataset/covid-19-vacinacao/resource/',
    'files': [
        'Dados_Fluxo_Parte_1.zip',
        'Dados_Fluxo_Parte_2.zip',
        'Dados_Fluxo_Parte_3.zip',
        'Dados_Fluxo_Parte_4.zip',
        'Dados_Fluxo_Parte_5.zip'
    ]
}

process_files_with_config(config)
```

## Funcionalidades

- ✅ Execução paralela configurável
- ✅ Verificação de arquivos já existentes no S3
- ✅ Relatório detalhado de progresso
- ✅ Tratamento de erros robusto
- ✅ Salvamento de resultados em JSON
- ✅ Estatísticas de transferência

## Saída

O script gera:
- Log detalhado no console
- Arquivo `batch_results.json` com resultados completos
- Estatísticas de transferência e erros

## Requisitos

- boto3
- Credenciais AWS configuradas
- Função Lambda deployada e acessível