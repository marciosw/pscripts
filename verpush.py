#!/usr/bin/env python3
"""
Script para varrer pacientes no Firestore e identificar aqueles com collection e-tib-notificacoes.
Processa em paralelo para lidar com grandes volumes de dados.
"""

import argparse
import json
import csv
import sys
import queue
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, Tuple, List
import time

try:
    import firebase_admin
    from firebase_admin import credentials, firestore, auth
except ImportError:
    print("Erro: firebase-admin não está instalado. Execute: pip install firebase-admin")
    sys.exit(1)


class PatientScanner:
    def __init__(self, key_file: str, max_workers: int = 50, batch_size: int = 1000):
        """
        Inicializa o scanner de pacientes.
        
        Args:
            key_file: Caminho para o arquivo JSON com credenciais do Firebase
            max_workers: Número máximo de threads para processamento paralelo
            batch_size: Tamanho do lote para listar usuários do Auth
        """
        self.key_file = key_file
        self.max_workers = max_workers
        self.batch_size = batch_size
        self.tenant_path = "Tenant/vWxkIvCXIJUSIlhBSiIl"
        self.patient_path = f"{self.tenant_path}/Patient"
        self.db = None
        self._initialize_firebase()
    
    def _initialize_firebase(self):
        """Inicializa a conexão com o Firebase."""
        try:
            # Carrega as credenciais diretamente do arquivo JSON
            # Isso é mais seguro e evita problemas de formatação
            cred = credentials.Certificate(self.key_file)
            
            # Inicializa o Firebase Admin (ou usa a instância existente)
            try:
                app = firebase_admin.initialize_app(cred)
            except ValueError:
                # App já inicializado, usa a instância existente
                app = firebase_admin.get_app()
            
            self.db = firestore.client()
            
            # Obtém o project_id do arquivo para exibição
            with open(self.key_file, 'r') as f:
                key_data = json.load(f)
            
            print(f"✓ Firebase inicializado com sucesso (projeto: {key_data.get('project_id')})")
            
        except FileNotFoundError:
            print(f"Erro: Arquivo de chave não encontrado: {self.key_file}")
            sys.exit(1)
        except Exception as e:
            print(f"Erro ao inicializar Firebase: {e}")
            print(f"\nVerifique se o arquivo {self.key_file} contém uma chave privada válida e completa.")
            print("A chave privada deve começar com '-----BEGIN PRIVATE KEY-----'")
            print("e terminar com '-----END PRIVATE KEY-----'")
            
            # Verifica se a chave está truncada
            try:
                with open(self.key_file, 'r') as f:
                    key_data = json.load(f)
                    private_key = key_data.get('private_key', '')
                    if private_key and '-----END PRIVATE KEY-----' not in private_key:
                        print("\n⚠️  ATENÇÃO: A chave privada no arquivo parece estar truncada!")
                        print("   É necessário obter o arquivo JSON completo do Firebase Console.")
            except:
                pass
            
            sys.exit(1)
    
    def load_uids_to_queue(self, uid_queue: queue.Queue, stop_event: threading.Event) -> int:
        """
        Carrega UIDs do Firebase Authentication e os coloca na fila.
        Executa em uma thread separada para permitir processamento paralelo.
        
        Args:
            uid_queue: Fila thread-safe para receber os UIDs
            stop_event: Evento para sinalizar parada em caso de erro
            
        Returns:
            Total de UIDs carregados
        """
        page_token = None
        total = 0
        
        try:
            print("Carregando UIDs do Firebase Authentication...")
            while True:
                # Lista usuários em lotes
                result = auth.list_users(max_results=self.batch_size, page_token=page_token)
                
                batch_uids = [user.uid for user in result.users]
                total += len(batch_uids)
                
                # Coloca cada UID na fila
                for uid in batch_uids:
                    uid_queue.put(uid)
                
                print(f"  Carregados {total} UIDs... (fila: {uid_queue.qsize()})", end='\r')
                
                # Verifica se há mais páginas
                if result.has_next_page:
                    page_token = result.next_page_token
                else:
                    break
            
            print(f"\n✓ Total de {total} UIDs carregados")
            return total
            
        except Exception as e:
            print(f"\nErro ao listar usuários: {e}")
            stop_event.set()  # Sinaliza para parar os workers
            return total
    
    def check_notifications_exists(self, uid: str) -> bool:
        """
        Verifica se existe o documento "regras" na collection e-tib-notificacoes.
        Função otimizada para verificação rápida.
        
        Args:
            uid: UID do paciente
            
        Returns:
            True se o documento "regras" existe, False caso contrário
        """
        try:
            notifications_path = f"{self.patient_path}/{uid}/e-tib-notificacoes"
            # Verifica especificamente se o documento "regras" existe
            regras_ref = self.db.document(f"{notifications_path}/regras")
            regras_doc = regras_ref.get()
            return regras_doc.exists
        except Exception:
            return False
    
    def get_patient_cpf(self, uid: str) -> Optional[str]:
        """
        Obtém o CPF do documento do paciente.
        
        Args:
            uid: UID do paciente
            
        Returns:
            CPF do paciente ou None se não encontrado
        """
        try:
            patient_ref = self.db.document(f"{self.patient_path}/{uid}")
            patient_doc = patient_ref.get()
            
            if patient_doc.exists:
                patient_data = patient_doc.to_dict()
                # Tenta diferentes campos possíveis para CPF
                cpf = (
                    patient_data.get('cpf') or
                    patient_data.get('CPF') or
                    patient_data.get('documento') or
                    patient_data.get('Documento') or
                    patient_data.get('document') or
                    ''
                )
                # Remove formatação do CPF se houver
                cpf = str(cpf).replace('.', '').replace('-', '').replace(' ', '')
                return cpf if cpf else None
            return None
        except Exception:
            return None
    
    def check_patient_has_notifications(self, uid: str) -> Optional[Tuple[str, str]]:
        """
        Verifica se um paciente tem o documento "regras" na collection e-tib-notificacoes.
        Otimizado para fazer verificações em paralelo quando possível.
        
        Args:
            uid: UID do paciente
            
        Returns:
            Tupla (uid, cpf) se o paciente tem o documento "regras", None caso contrário
        """
        try:
            # Primeiro verifica se a collection existe (mais rápido)
            if not self.check_notifications_exists(uid):
                return None
            
            # Se existe, busca o CPF
            cpf = self.get_patient_cpf(uid)
            if cpf:
                return (uid, cpf)
            
            return None
            
        except Exception as e:
            # Log do erro mas não interrompe o processamento
            # Reduzido para não poluir o output
            return None
    
    def scan_patients(self, output_file: str):
        """
        Escaneia todos os pacientes e gera o arquivo CSV.
        Processa UIDs conforme são carregados, sem esperar carregar todos primeiro.
        Escreve no CSV incrementalmente conforme encontra resultados.
        
        Args:
            output_file: Nome do arquivo CSV de saída
        """
        print(f"\nIniciando varredura de pacientes...")
        print(f"Processamento paralelo com {self.max_workers} workers")
        print("(Carregando e processando UIDs simultaneamente)")
        print(f"CSV sendo escrito incrementalmente em: {output_file}\n")
        
        # Filas thread-safe com buffers maiores para melhor throughput
        uid_queue = queue.Queue(maxsize=self.max_workers * 5)  # Buffer maior para evitar esperas
        # Fila thread-safe para resultados (apenas pacientes encontrados)
        result_queue = queue.Queue(maxsize=1000)  # Buffer para resultados
        stop_event = threading.Event()
        processed = 0
        found = 0  # Contador de resultados encontrados (incrementado quando encontrado, não quando escrito)
        total_uids = 0
        start_time = time.time()
        
        # Abre o arquivo CSV e escreve o cabeçalho
        csv_file = open(output_file, 'w', newline='', encoding='utf-8')
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(['uid', 'cpf'])  # Cabeçalho
        csv_file.flush()  # Garante que o cabeçalho seja escrito imediatamente
        
        # Thread para escrever resultados no CSV (otimizada para batch writing)
        def write_results():
            written_count = 0
            batch = []
            batch_size = 10  # Escreve em lotes para melhor performance
            last_flush = time.time()
            
            while True:
                try:
                    # Tenta pegar múltiplos resultados de uma vez
                    result = result_queue.get(timeout=0.5)
                    if result is None:  # Sinal de fim
                        # Escreve batch restante
                        if batch:
                            for uid, cpf in batch:
                                csv_writer.writerow([uid, cpf])
                            csv_file.flush()
                            batch = []
                        break
                    
                    uid, cpf = result
                    batch.append((uid, cpf))
                    written_count += 1
                    result_queue.task_done()
                    
                    # Escreve em lote ou força flush a cada segundo
                    current_time = time.time()
                    if len(batch) >= batch_size or (current_time - last_flush) >= 1.0:
                        for uid, cpf in batch:
                            csv_writer.writerow([uid, cpf])
                        csv_file.flush()
                        batch = []
                        last_flush = current_time
                        
                except queue.Empty:
                    # Se há batch pendente, escreve
                    if batch:
                        for uid, cpf in batch:
                            csv_writer.writerow([uid, cpf])
                        csv_file.flush()
                        batch = []
                        last_flush = time.time()
                    continue
                except Exception as e:
                    print(f"\n  Erro ao escrever no CSV: {e}")
        
        writer_thread = threading.Thread(target=write_results, daemon=False)
        writer_thread.start()
        
        # Thread para carregar UIDs
        def load_uids():
            nonlocal total_uids
            total_uids = self.load_uids_to_queue(uid_queue, stop_event)
            # Sinaliza fim colocando None na fila para cada worker
            for _ in range(self.max_workers):
                uid_queue.put(None)
        
        loader_thread = threading.Thread(target=load_uids, daemon=True)
        loader_thread.start()
        
        # Processa em paralelo enquanto UIDs são carregados
        # Usa um executor maior para melhor paralelismo
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = set()
            loading_complete = False
            last_print_time = time.time()
            
            while True:
                # Submete novas tarefas enquanto há espaço e UIDs disponíveis
                # Processa em lote para reduzir overhead
                batch_size = min(10, self.max_workers - len(futures))
                for _ in range(batch_size):
                    if len(futures) >= self.max_workers or stop_event.is_set():
                        break
                    
                    try:
                        uid = uid_queue.get(timeout=0.1)
                        
                        if uid is None:
                            # Sinal de fim do carregamento
                            loading_complete = True
                            break
                        
                        # Submete tarefa
                        future = executor.submit(self.check_patient_has_notifications, uid)
                        futures.add(future)
                        
                    except queue.Empty:
                        # Fila vazia temporariamente
                        if not loader_thread.is_alive() and uid_queue.empty():
                            loading_complete = True
                        break
                
                # Processa resultados completos em lote
                completed = [f for f in futures if f.done()]
                for future in completed:
                    futures.discard(future)
                    processed += 1
                    
                    try:
                        result = future.result()
                        if result:
                            # Coloca resultado na fila para escrita no CSV (não bloqueia)
                            try:
                                result_queue.put_nowait(result)
                            except queue.Full:
                                # Se a fila estiver cheia, espera um pouco
                                result_queue.put(result, timeout=1)
                            found += 1
                            print(f"  [{processed}] ✓ Encontrado: {result[0]} (CPF: {result[1]})")
                        else:
                            # Print de progresso a cada 100 ou a cada 2 segundos
                            current_time = time.time()
                            if processed % 100 == 0 or (current_time - last_print_time) >= 2.0:
                                queue_size = uid_queue.qsize()
                                result_queue_size = result_queue.qsize()
                                print(f"  [{processed}] Processados... (encontrados: {found}, fila UIDs: {queue_size}, fila resultados: {result_queue_size}, ativos: {len(futures)})")
                                last_print_time = current_time
                    except Exception as e:
                        # Erros silenciosos para não poluir output
                        pass
                
                # Se carregamento terminou e não há mais tarefas, sai do loop
                if loading_complete and len(futures) == 0:
                    break
                
                # Pausa mínima apenas se não houver trabalho
                if not completed and len(futures) >= self.max_workers:
                    time.sleep(0.01)  # Reduzido de 0.05 para 0.01
            
            # Processa resultados restantes (caso algum ainda esteja pendente)
            if futures:
                print(f"\nAguardando conclusão de {len(futures)} tarefas restantes...")
                for future in as_completed(futures):
                    processed += 1
                    try:
                        result = future.result()
                        if result:
                            # Coloca resultado na fila para escrita no CSV
                            result_queue.put(result)
                            print(f"  [{processed}] ✓ Encontrado: {result[0]} (CPF: {result[1]})")
                    except Exception as e:
                        print(f"\n  Erro ao processar: {e}")
        
        # Aguarda thread de carregamento terminar
        loader_thread.join(timeout=5)
        
        # Sinaliza fim para a thread de escrita
        result_queue.put(None)
        
        # Aguarda thread de escrita terminar e processar todos os resultados pendentes
        writer_thread.join(timeout=30)
        
        # Garante que todos os dados foram escritos
        csv_file.flush()
        
        # Fecha o arquivo CSV
        csv_file.close()
        
        elapsed_time = time.time() - start_time
        
        # Exibe estatísticas finais
        print(f"\n✓ Processamento concluído!")
        print(f"  Total de UIDs disponíveis: {total_uids}")
        print(f"  Total processado: {processed}")
        print(f"  Pacientes com e-tib-notificacoes: {found}")
        print(f"  Tempo total: {elapsed_time:.2f} segundos")
        print(f"\n✓ Arquivo CSV gerado com sucesso: {output_file}")
        print(f"  Total de registros escritos: {found}")


def main():
    parser = argparse.ArgumentParser(
        description='Varre pacientes no Firestore e identifica aqueles com collection e-tib-notificacoes',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos:
  python verpush.py key-prod.json output.csv
  python verpush.py --key-file key-prod.json --output output.csv --workers 100
        """
    )
    
    parser.add_argument(
        'key_file',
        nargs='?',
        help='Caminho para o arquivo JSON com credenciais do Firebase'
    )
    
    parser.add_argument(
        'output_file',
        nargs='?',
        help='Nome do arquivo CSV de saída'
    )
    
    parser.add_argument(
        '--key-file',
        dest='key_file_opt',
        help='Caminho para o arquivo JSON com credenciais do Firebase (alternativa)'
    )
    
    parser.add_argument(
        '--output',
        '--output-file',
        dest='output_file_opt',
        help='Nome do arquivo CSV de saída (alternativa)'
    )
    
    parser.add_argument(
        '--workers',
        type=int,
        default=50,
        help='Número de workers para processamento paralelo (padrão: 50)'
    )
    
    parser.add_argument(
        '--batch-size',
        type=int,
        default=1000,
        help='Tamanho do lote para listar usuários do Auth (padrão: 1000)'
    )
    
    args = parser.parse_args()
    
    # Resolve argumentos (suporta posicional e opcional)
    key_file = args.key_file_opt or args.key_file
    output_file = args.output_file_opt or args.output_file
    
    if not key_file:
        parser.error("É necessário fornecer o caminho do arquivo de chave (key_file)")
    
    if not output_file:
        parser.error("É necessário fornecer o nome do arquivo de saída (output_file)")
    
    # Cria e executa o scanner
    scanner = PatientScanner(
        key_file=key_file,
        max_workers=args.workers,
        batch_size=args.batch_size
    )
    
    scanner.scan_patients(output_file)


if __name__ == '__main__':
    main()

