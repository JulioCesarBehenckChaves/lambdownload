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

        # Fazer uma invocação de teste simples (sem URL para gerar erro esperado)
        response = lambda_client.invoke(
            FunctionName=function_name,
            InvocationType='RequestResponse',
            Payload=json.dumps({})
        )

        # Se chegou até aqui, a função existe e é acessível
        print(f"✅ Função Lambda acessível: {function_name}")

        # Verificar se foi erro de parâmetro obrigatório (esperado)
        result = json.loads(response['Payload'].read())
        if result['statusCode'] == 400:  # Erro esperado para falta de URL
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
        
        # Adicionar bucket e prefix se fornecidos
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
                print(
                    f"[{index}/{total}] ✅ {filename} - {size_mb}MB em {transfer_time:.1f}s (total: {execution_time:.1f}s)")
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


def process_files_batch():
    """Processa lista de arquivos em lote"""

    # ==========================================
    # CONFIGURAÇÕES - AJUSTE AQUI
    # ==========================================
    function_name = 'lambdownload-function'  # Nome da sua função Lambda
    max_concurrent = 2  # Execuções simultâneas
    base_url = 'https://example.com/path/'  # URL base dos arquivos
    bucket_name = 'meu-bucket-dados'  # Bucket S3 de destino
    s3_prefix = 'dados-fluxo/'  # Prefixo/pasta no S3

    # LISTA DE ARQUIVOS - COLOQUE SEUS ARQUIVOS AQUI
    filenames = [
        "Dados_Fluxo_Parte_10.zip",
        "Dados_Fluxo_Parte_100.zip",
        "Dados_Fluxo_Parte_101.zip",
        "Dados_Fluxo_Parte_102.zip",
        "Dados_Fluxo_Parte_103.zip"
        # Adicione mais arquivos conforme necessário
    ]

    # Construir lista de configurações de arquivos
    files_to_download = []
    for filename in filenames:
        files_to_download.append({
            'filename': filename,
            'url': f"{base_url}{filename}",
            'bucket': bucket_name,
            'prefix': s3_prefix
        })

    # ==========================================
    # EXECUÇÃO
    # ==========================================

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
        # Submeter todas as tarefas
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

        # Coletar resultados conforme completam
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

    # ==========================================
    # RELATÓRIO FINAL
    # ==========================================
    print()
    print("=" * 60)
    print("📈 RELATÓRIO FINAL")
    print("=" * 60)

    # Contar resultados por status
    success_count = len([r for r in results if r['status'] == 'success'])
    skipped_count = len([r for r in results if r['status'] == 'skipped'])
    error_count = len([r for r in results if r['status'] in ['error', 'exception', 'thread_exception']])

    print(f"📁 Total de arquivos: {len(results)}")
    print(f"✅ Sucessos: {success_count}")
    print(f"⏭️ Ignorados (já existem): {skipped_count}")
    print(f"❌ Erros: {error_count}")
    print(f"⏱️ Tempo total: {total_time:.1f}s")
    print()

    # Mostrar estatísticas de transferência
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

    # Salvar resultados em arquivo
    save_results_to_file(results)

    print("🎉 Processamento concluído!")


if __name__ == "__main__":
    process_files_batch()92.zip",
        "Dados_Fluxo_Parte_93.zip",
        "Dados_Fluxo_Parte_94.zip",
        "Dados_Fluxo_Parte_95.zip",
        "Dados_Fluxo_Parte_96.zip",
        "Dados_Fluxo_Parte_97.zip",
        "Dados_Fluxo_Parte_98.zip",
        "Dados_Fluxo_Parte_99.zip",
        "Tabela_Locais.txt"
        # ... adicione todos os arquivos aqui

        # Adicione todos os seus arquivos aqui
    ]

    print("🚀 AWS FTP to S3 Batch Processor")
    print("=" * 60)
    print(f"📋 Função Lambda: {function_name}")
    print(f"📁 Total de arquivos: {len(files_to_download)}")
    print(f"🔄 Concorrência máxima: {max_concurrent}")
    print(f"⏰ Iniciado em: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    # Verificar credenciais AWS
    if not check_aws_credentials():
        return None

    # Inicializar cliente Lambda
    try:
        lambda_client = boto3.client('lambda')
        print("✅ Cliente Lambda inicializado")
    except Exception as e:
        print(f"❌ Erro ao inicializar cliente Lambda: {str(e)}")
        return None

    # Testar função Lambda (sem GetFunction)
    if not test_lambda_function_simple(lambda_client, function_name):
        print("⚠️  Continuando mesmo com erro no teste...")
        # Não retornar None aqui, continuar tentando

    print("-" * 60)

    results = []
    start_time = time.time()

    # Processar arquivos com concorrência limitada
    with ThreadPoolExecutor(max_workers=max_concurrent) as executor:
        # Submeter todas as tarefas
        future_to_info = {
            executor.submit(invoke_lambda_for_file, lambda_client, function_name, filename, i + 1,
                            len(files_to_download)):
                {'filename': filename, 'index': i + 1}
            for i, filename in enumerate(files_to_download)
        }

        # Processar resultados conforme completam
        completed = 0
        for future in as_completed(future_to_info):
            result = future.result()
            results.append(result)
            completed += 1

            # Mostrar progresso
            progress = (completed / len(files_to_download)) * 100
            print(f"📊 Progresso: {completed}/{len(files_to_download)} ({progress:.1f}%)")

    # Relatório final
    total_time = time.time() - start_time
    successful = len([r for r in results if r['status'] == 'success'])
    skipped = len([r for r in results if r['status'] == 'skipped'])
    failed = len([r for r in results if r['status'] in ['error', 'exception']])

    print("\n" + "=" * 60)
    print("📊 RELATÓRIO FINAL")
    print("=" * 60)
    print(f"✅ Sucessos: {successful}")
    print(f"⏭️  Pulados (já existiam): {skipped}")
    print(f"❌ Falhas: {failed}")
    print(f"📁 Total: {len(files_to_download)}")
    print(f"⏱️  Tempo total: {total_time:.2f}s")
    if len(files_to_download) > 0:
        print(f"⚡ Média por arquivo: {total_time / len(files_to_download):.2f}s")
    print("=" * 60)

    # Mostrar falhas detalhadas
    failures = [r for r in results if r['status'] in ['error', 'exception']]
    if failures:
        print("\n❌ ARQUIVOS COM FALHA:")
        for failure in failures:
            error_msg = failure.get('error', failure.get('result', 'Erro desconhecido'))
            print(f"   - {failure['filename']}: {error_msg}")

    # Salvar resultados
    save_results_to_file(results)

    return results


def main():
    """Função principal"""
    try:
        results = process_files_batch()
        if results:
            print(f"\n🎉 Processamento concluído! Verifique o arquivo batch_results.json")
        else:
            print(f"\n💥 Processamento falhou na configuração inicial")
    except KeyboardInterrupt:
        print(f"\n⏹️  Processamento interrompido pelo usuário")
    except Exception as e:
        print(f"\n💥 Erro inesperado: {str(e)}")


if __name__ == "__main__":
    main()
