import boto3
import json
import time
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime


def check_aws_credentials():
    """Verifica se as credenciais AWS estão configuradas"""
    try:
        sts = boto3.client('sts')
        identity = sts.get_caller_identity()
        print(f"✅ AWS configurado para conta: {identity['Account']}")
        print(f"✅ Usuário/Role: {identity['Arn']}")
        return True
    except Exception as e:
        print(f"❌ Erro nas credenciais AWS: {str(e)}")
        print("💡 Configure com: aws configure")
        return False


def test_lambda_function_simple(lambda_client, function_name):
    """Testa a função Lambda fazendo uma invocação de teste simples"""
    try:
        print(f"🔍 Testando função Lambda: {function_name}")

        response = lambda_client.invoke(
            FunctionName=function_name,
            InvocationType='RequestResponse',
            Payload=json.dumps({})
        )

        print(f"✅ Função Lambda acessível: {function_name}")
        result = json.loads(response['Payload'].read())
        if result['statusCode'] == 400:
            print(f"✅ Função respondeu corretamente ao teste")
            return True
        else:
            print(f"✅ Função executou com sucesso")
            return True

    except Exception as e:
        error_str = str(e)
        if "does not exist" in error_str or "Function not found" in error_str:
            print(f"❌ Função Lambda não encontrada: {function_name}")
        elif "AccessDenied" in error_str:
            print(f"❌ Sem permissão para acessar função: {function_name}")
        else:
            print(f"❌ Erro ao testar função Lambda: {error_str}")
        return False


def invoke_lambda_for_file(lambda_client, function_name, file_config, index, total):
    """Invoca a Lambda para um arquivo específico"""
    try:
        filename = file_config['filename']
        url = file_config['url']
        print(f"[{index}/{total}] 🔄 Iniciando: {filename}")
        start_time = time.time()

        payload = {
            'url': url,
            'filename': filename
        }
        
        if 'bucket' in file_config:
            payload['bucket'] = file_config['bucket']
        if 'prefix' in file_config:
            payload['prefix'] = file_config['prefix']

        response = lambda_client.invoke(
            FunctionName=function_name,
            InvocationType='RequestResponse',
            Payload=json.dumps(payload)
        )

        result = json.loads(response['Payload'].read())
        execution_time = time.time() - start_time

        if result['statusCode'] == 200:
            body = json.loads(result['body'])
            if body.get('status') == 'skipped':
                print(f"[{index}/{total}] ⏭️  {filename} - Já existe no S3 ({execution_time:.1f}s)")
                return {'filename': filename, 'status': 'skipped', 'result': body, 'execution_time': execution_time}
            else:
                stats = body.get('stats', {})
                size_mb = stats.get('size_mb', 0)
                transfer_time = stats.get('total_time_seconds', 0)
                print(f"[{index}/{total}] ✅ {filename} - {size_mb}MB em {transfer_time:.1f}s (total: {execution_time:.1f}s)")
                return {'filename': filename, 'status': 'success', 'result': body, 'execution_time': execution_time}
        else:
            print(f"[{index}/{total}] ❌ {filename} - Erro Lambda: {result}")
            return {'filename': filename, 'status': 'error', 'result': result, 'execution_time': execution_time}

    except Exception as e:
        execution_time = time.time() - start_time if 'start_time' in locals() else 0
        print(f"[{index}/{total}] 💥 {filename} - Exceção: {str(e)}")
        return {'filename': filename, 'status': 'exception', 'error': str(e), 'execution_time': execution_time}


def save_results_to_file(results, filename="batch_results.json"):
    """Salva resultados em arquivo JSON"""
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump({
                'timestamp': datetime.now().isoformat(),
                'results': results
            }, f, indent=2, ensure_ascii=False)
        print(f"💾 Resultados salvos em: {filename}")
    except Exception as e:
        print(f"❌ Erro ao salvar resultados: {str(e)}")


def process_files_with_config(config):
    """Processa lista de arquivos usando configuração fornecida"""
    
    function_name = config['function_name']
    max_concurrent = config.get('max_concurrent', 2)
    base_url = config['base_url']
    bucket_name = config['bucket']
    s3_prefix = config.get('prefix', '')
    filenames = config['files']

    # Construir lista de configurações de arquivos
    files_to_download = []
    for filename in filenames:
        files_to_download.append({
            'filename': filename,
            'url': f"{base_url}{filename}",
            'bucket': bucket_name,
            'prefix': s3_prefix
        })

    print(f"🚀 Iniciando processamento em lote")
    print(f"📝 Configurações:")
    print(f"   - Função Lambda: {function_name}")
    print(f"   - Bucket S3: {bucket_name}")
    print(f"   - Prefixo S3: {s3_prefix}")
    print(f"   - URL base: {base_url}")
    print(f"   - Total de arquivos: {len(files_to_download)}")
    print(f"   - Execuções simultâneas: {max_concurrent}")
    print()

    # Verificar credenciais AWS
    if not check_aws_credentials():
        return

    # Inicializar cliente Lambda
    try:
        lambda_client = boto3.client('lambda')
        print(f"✅ Cliente Lambda inicializado")
    except Exception as e:
        print(f"❌ Erro ao inicializar cliente Lambda: {str(e)}")
        return

    # Testar função Lambda
    if not test_lambda_function_simple(lambda_client, function_name):
        print(f"❌ Abortando execução devido a problemas com a função Lambda")
        return

    print()
    print(f"📈 Iniciando processamento de {len(files_to_download)} arquivos...")
    print()

    # Processar arquivos com ThreadPoolExecutor
    results = []
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=max_concurrent) as executor:
        future_to_file = {
            executor.submit(
                invoke_lambda_for_file,
                lambda_client,
                function_name,
                file_config,
                i + 1,
                len(files_to_download)
            ): file_config for i, file_config in enumerate(files_to_download)
        }

        for future in as_completed(future_to_file):
            file_config = future_to_file[future]
            try:
                result = future.result()
                results.append(result)
            except Exception as exc:
                filename = file_config['filename']
                print(f"💥 {filename} - Exceção na thread: {exc}")
                results.append({
                    'filename': filename,
                    'status': 'thread_exception',
                    'error': str(exc),
                    'execution_time': 0
                })

    total_time = time.time() - start_time

    # Relatório final
    print()
    print("=" * 60)
    print("📈 RELATÓRIO FINAL")
    print("=" * 60)

    success_count = len([r for r in results if r['status'] == 'success'])
    skipped_count = len([r for r in results if r['status'] == 'skipped'])
    error_count = len([r for r in results if r['status'] in ['error', 'exception', 'thread_exception']])

    print(f"📁 Total de arquivos: {len(results)}")
    print(f"✅ Sucessos: {success_count}")
    print(f"⏭️ Ignorados (já existem): {skipped_count}")
    print(f"❌ Erros: {error_count}")
    print(f"⏱️ Tempo total: {total_time:.1f}s")
    print()

    # Estatísticas de transferência
    successful_results = [r for r in results if r['status'] == 'success' and 'result' in r]
    if successful_results:
        total_mb = sum([r['result'].get('stats', {}).get('size_mb', 0) for r in successful_results])
        avg_speed = total_mb / total_time if total_time > 0 else 0
        print(f"📈 Estatísticas de transferência:")
        print(f"   - Total transferido: {total_mb:.1f} MB")
        print(f"   - Velocidade média: {avg_speed:.1f} MB/s")
        print()

    # Mostrar erros se houver
    if error_count > 0:
        print("❌ ERROS ENCONTRADOS:")
        for result in results:
            if result['status'] in ['error', 'exception', 'thread_exception']:
                filename = result['filename']
                error = result.get('error', result.get('result', 'Erro desconhecido'))
                print(f"   - {filename}: {error}")
        print()

    # Salvar resultados
    save_results_to_file(results)
    print("🎉 Processamento concluído!")


def main():
    """Função principal com exemplo de uso"""
    
    # Exemplo de configuração para dados do COVID-19
    covid_config = {
        'function_name': 'lambdownload',
        'max_concurrent': 2,
        'bucket': 'administrativoticlab',
        'prefix': 'landing-zone/',
        'base_url': 'https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SIPNI/COVID/completo/',
        'files': [
            'part-00000-70dd7710-b64c-4a6e-a780-bf4ca7d0a1f7-c000.csv'
        ]
    }
    
    # Processar com a configuração
    process_files_with_config(covid_config)


if __name__ == "__main__":
    main()