#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Consumidor Kafka para processar mensagens de vendas e ingerir no Apache Pinot.
"""

import json
import time
import signal
import logging
import requests
import os
import socket
from confluent_kafka import Consumer, KafkaError, KafkaException
from datetime import datetime

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Detectar ambiente
def is_running_in_docker():
    try:
        with open('/proc/1/cgroup', 'r') as f:
            return 'docker' in f.read()
    except:
        return False

# Configurar endereços baseados no ambiente
IS_DOCKER = is_running_in_docker()
logger.info(f"Detectado ambiente: {'Docker' if IS_DOCKER else 'Local'}")

# Funções para configurar endereços baseados no ambiente
def get_environment_config():
    """Retorna a configuração adequada baseada no ambiente de execução"""
    if IS_DOCKER:
        # Executando dentro de um container Docker
        return {
            'kafka_bootstrap': 'kafka:9092',
            'kafka_for_pinot': 'kafka:9092',  # Pinot container -> Kafka container
            'pinot_controller': 'http://pinot-controller:9000',
            'pinot_broker': 'http://pinot-broker:8099'
        }
    else:
        # Executando localmente
        return {
            'kafka_bootstrap': 'localhost:29092',  # Porto mapeado para o host
            'kafka_for_pinot': 'kafka:9092',       # Nome do serviço para uso interno do Pinot
            'pinot_controller': 'http://localhost:9000',
            'pinot_broker': 'http://localhost:8099'
        }

# Obter configuração do ambiente
env_config = get_environment_config()

# Configurações Kafka
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', env_config['kafka_bootstrap'])
KAFKA_TOPIC = os.environ.get('KAFKA_TOPIC', 'vendas-tempo-real')
KAFKA_GROUP_ID = os.environ.get('KAFKA_GROUP_ID', 'grupo-consumidor-pinot')
KAFKA_AUTO_OFFSET_RESET = os.environ.get('KAFKA_AUTO_OFFSET_RESET', 'earliest')

# Configurações Pinot
PINOT_CONTROLLER_URL = os.environ.get('PINOT_CONTROLLER_URL', env_config['pinot_controller'])
PINOT_BROKER_URL = os.environ.get('PINOT_BROKER_URL', env_config['pinot_broker'])
PINOT_TABLE = os.environ.get('PINOT_TABLE', 'vendas')

# Configurações para uso interno do Pinot (como tabela conecta com Kafka)
KAFKA_CONNECT_URL = os.environ.get('KAFKA_CONNECT_URL', env_config['kafka_for_pinot'])

# Exibir informações de configuração
logger.info(f"Kafka Bootstrap Servers: {KAFKA_BOOTSTRAP_SERVERS}")
logger.info(f"Kafka Connect URL (para Pinot): {KAFKA_CONNECT_URL}")
logger.info(f"Pinot Controller URL: {PINOT_CONTROLLER_URL}")
logger.info(f"Pinot Broker URL: {PINOT_BROKER_URL}")

# Controle para interrupções
running = True

def criar_consumidor():
    """Cria e retorna uma instância do consumidor Kafka"""
    config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': KAFKA_GROUP_ID,
        'auto.offset.reset': KAFKA_AUTO_OFFSET_RESET,
        'enable.auto.commit': True,
        'auto.commit.interval.ms': 5000,  # 5 segundos
        'max.poll.interval.ms': 300000,   # 5 minutos
        'session.timeout.ms': 30000       # 30 segundos
    }
    return Consumer(config)

def handle_signal(sig, frame):
    """Manipulador de sinal para encerrar o consumidor graciosamente"""
    global running
    logger.info(f"Sinal recebido: {sig}. Encerrando o consumidor...")
    running = False

def verificar_pinot():
    """Verifica se o controlador Pinot está disponível"""
    logger.info(f"Verificando conexão com o Pinot Controller em {PINOT_CONTROLLER_URL}...")
    
    endpoints_to_try = [
        "/health", 
        "/", 
        "/tables", 
        "/schemas"
    ]
    
    is_available = False
    
    for endpoint in endpoints_to_try:
        try:
            url = f"{PINOT_CONTROLLER_URL}{endpoint}"
            logger.info(f"Tentando acessar: {url}")
            
            response = requests.get(url, timeout=5)
            status = response.status_code
            
            logger.info(f"Resposta de {url}: {status}")
            
            if status >= 200 and status < 300:
                logger.info(f"Conexão com Pinot verificada com sucesso via {endpoint}")
                is_available = True
                break
            else:
                logger.warning(f"Resposta inesperada do endpoint {endpoint}: {status}")
        except requests.exceptions.RequestException as e:
            logger.warning(f"Erro ao conectar com {endpoint}: {e}")
    
    if is_available:
        return True
    else:
        logger.error("Não foi possível conectar ao Apache Pinot em nenhum endpoint conhecido.")
        logger.error("Verifique se o serviço Pinot está rodando e as portas estão mapeadas corretamente.")
        return False

def criar_schema_pinot():
    """
    Cria o schema no Pinot para a tabela de vendas
    Nota: Em produção, isso normalmente seria gerenciado separadamente
    """
    schema = {
        "schemaName": PINOT_TABLE,
        "dimensionFieldSpecs": [
            {"name": "id_venda", "dataType": "STRING"},
            {"name": "id_cliente", "dataType": "STRING"},
            {"name": "nome_cliente", "dataType": "STRING"},
            {"name": "email_cliente", "dataType": "STRING"},
            {"name": "produto", "dataType": "STRING"},
            {"name": "categoria", "dataType": "STRING"},
            {"name": "forma_pagamento", "dataType": "STRING"},
            {"name": "loja", "dataType": "STRING"},
            {"name": "cidade", "dataType": "STRING"},
            {"name": "estado", "dataType": "STRING"}
        ],
        "metricFieldSpecs": [
            {"name": "preco", "dataType": "DOUBLE"},
            {"name": "quantidade", "dataType": "INT"},
            {"name": "valor_total", "dataType": "DOUBLE"}
        ],
        "dateTimeFieldSpecs": [
            {
                "name": "timestamp",
                "dataType": "LONG",
                "format": "1:MILLISECONDS:EPOCH",
                "granularity": "1:MILLISECONDS"
            },
            {
                "name": "data_hora",
                "dataType": "STRING",
                "format": "1:DAYS:SIMPLE_DATE_FORMAT:yyyy-MM-dd'T'HH:mm:ss",
                "granularity": "1:DAYS"
            }
        ]
    }
    
    # Verificar se o schema já existe
    logger.info(f"Verificando se o schema {PINOT_TABLE} já existe...")
    schema_exists = False
    
    try:
        response = requests.get(f"{PINOT_CONTROLLER_URL}/schemas")
        if response.status_code == 200:
            schemas = response.json()
            if PINOT_TABLE in schemas:
                logger.info(f"Schema {PINOT_TABLE} já existe")
                schema_exists = True
        else:
            logger.warning(f"Erro ao listar schemas: {response.status_code}, {response.text}")
    except requests.exceptions.RequestException as e:
        logger.warning(f"Exceção ao listar schemas: {str(e)}")
    
    # Criar schema se não existir
    if not schema_exists:
        logger.info(f"Criando schema {PINOT_TABLE}...")
        try:
            response = requests.post(
                f"{PINOT_CONTROLLER_URL}/schemas",
                headers={"Content-Type": "application/json"},
                json=schema
            )
            
            if response.status_code == 200:
                logger.info(f"Schema {PINOT_TABLE} criado com sucesso: {response.text}")
                return True
            else:
                logger.error(f"Erro ao criar schema: {response.status_code}, {response.text}")
                return False
        except requests.exceptions.RequestException as e:
            logger.error(f"Exceção ao criar schema: {str(e)}")
            return False
    
    return True

def criar_tabela_pinot():
    """
    Cria a tabela no Pinot para ingestão de dados
    """
    logger.info("Preparando para criar a tabela no Pinot...")
    
    # Definindo a configuração da tabela
    table_config = {
        "tableName": PINOT_TABLE,
        "tableType": "REALTIME",
        "segmentsConfig": {
            "timeColumnName": "timestamp",
            "timeType": "MILLISECONDS",
            "replication": "1",
            "schemaName": PINOT_TABLE
        },
        "tenants": {},
        "tableIndexConfig": {
            "loadMode": "MMAP",
            "streamConfigs": {
                "streamType": "kafka",
                "stream.kafka.consumer.type": "lowlevel",
                "stream.kafka.topic.name": KAFKA_TOPIC,
                "stream.kafka.decoder.class.name": "org.apache.pinot.plugin.stream.kafka.KafkaJSONMessageDecoder",
                "stream.kafka.consumer.factory.class.name": "org.apache.pinot.plugin.stream.kafka20.KafkaConsumerFactory",
                # Usar nome do serviço Docker para comunicação interna do Pinot com Kafka
                "stream.kafka.broker.list": KAFKA_CONNECT_URL,
                "realtime.segment.flush.threshold.time": "3600000",
                "realtime.segment.flush.threshold.size": "500000",
                "stream.kafka.consumer.prop.auto.offset.reset": "smallest"
            }
        },
        "metadata": {
            "customConfigs": {}
        }
    }
    
    # Verificar se a tabela já existe
    logger.info(f"Verificando se a tabela {PINOT_TABLE} já existe...")
    table_exists = False
    
    try:
        # Listar todas as tabelas
        response = requests.get(f"{PINOT_CONTROLLER_URL}/tables")
        if response.status_code == 200:
            tables = response.json().get("tables", [])
            if f"{PINOT_TABLE}_REALTIME" in tables:
                logger.info(f"Tabela {PINOT_TABLE}_REALTIME já existe")
                table_exists = True
            else:
                logger.info(f"Tabela {PINOT_TABLE}_REALTIME não encontrada entre as tabelas existentes: {tables}")
        else:
            logger.warning(f"Erro ao listar tabelas: {response.status_code}, {response.text}")
    except requests.exceptions.RequestException as e:
        logger.warning(f"Exceção ao listar tabelas: {str(e)}")
    
    # Criar tabela se não existir
    if not table_exists:
        logger.info(f"Criando tabela {PINOT_TABLE}...")
        
        try:
            # Fazendo um log detalhado da configuração da tabela
            logger.info(f"Configuração da tabela a ser enviada: {json.dumps(table_config, indent=2)}")
            
            # Enviando a requisição para criar a tabela
            response = requests.post(
                f"{PINOT_CONTROLLER_URL}/tables",
                headers={"Content-Type": "application/json"},
                json=table_config
            )
            
            # Avaliando a resposta
            if response.status_code == 200:
                logger.info(f"Tabela {PINOT_TABLE} criada com sucesso: {response.text}")
                # Aguardar alguns segundos para que a tabela seja processada
                logger.info("Aguardando 5 segundos para processamento da tabela...")
                time.sleep(5)
                return True
            else:
                logger.error(f"Erro ao criar tabela: {response.status_code}, {response.text}")
                # Tentar verificar se houve algum problema específico
                try:
                    error_info = response.json()
                    logger.error(f"Detalhes do erro: {json.dumps(error_info, indent=2)}")
                except:
                    pass
                return False
        except requests.exceptions.RequestException as e:
            logger.error(f"Exceção ao criar tabela: {str(e)}")
            return False
    
    return True

def testar_consulta_pinot():
    """Testa uma consulta simples ao Pinot para verificar a ingestão de dados"""
    logger.info("Executando consulta de teste no Pinot...")
    
    # Consulta básica para contar registros
    count_query = f"SELECT COUNT(*) FROM {PINOT_TABLE}"
    
    try:
        # Usar o Broker para consultas ao invés do Controller
        logger.info(f"Enviando consulta para {PINOT_BROKER_URL}/query/sql: {count_query}")
        response = requests.post(
            f"{PINOT_BROKER_URL}/query/sql",
            headers={"Content-Type": "application/json"},
            json={"sql": count_query}
        )
        
        logger.info(f"Resposta da consulta: {response.status_code}")
        
        if response.status_code == 200:
            result = response.json()
            
            # Verificar se a resposta contém erro
            if "exceptions" in result and result["exceptions"]:
                logger.warning(f"Erro na consulta: {result['exceptions']}")
                return False
            
            # Extrair a contagem de registros
            if "resultTable" in result and "rows" in result["resultTable"] and result["resultTable"]["rows"]:
                count = result["resultTable"]["rows"][0][0]
                logger.info(f"Consulta ao Pinot - Total de registros: {count}")
                
                # Tentar uma consulta mais detalhada se houver registros
                if count > 0:
                    try:
                        detail_query = f"SELECT * FROM {PINOT_TABLE} LIMIT 1"
                        detail_response = requests.post(
                            f"{PINOT_BROKER_URL}/query/sql",
                            headers={"Content-Type": "application/json"},
                            json={"sql": detail_query}
                        )
                        
                        if detail_response.status_code == 200:
                            detail_result = detail_response.json()
                            if "resultTable" in detail_result and "rows" in detail_result["resultTable"]:
                                logger.info(f"Exemplo de registro: {detail_result['resultTable']['rows'][0]}")
                    except Exception as e:
                        logger.warning(f"Erro ao tentar consulta detalhada: {str(e)}")
                
                return True
            else:
                logger.warning("Resposta da consulta não contém resultados esperados")
                logger.debug(f"Resposta completa: {json.dumps(result)}")
                return False
        else:
            logger.warning(f"Erro ao consultar Pinot: {response.status_code}")
            try:
                error_text = response.text
                logger.warning(f"Detalhes do erro: {error_text}")
            except:
                pass
            return False
    except requests.exceptions.RequestException as e:
        logger.warning(f"Erro de conexão ao consultar Pinot: {e}")
        return False

def verificar_tabela_e_corrigir():
    """Verifica se a tabela está disponível e funcionando, corrigindo se necessário"""
    logger.info("Verificando e garantindo que a tabela esteja disponível...")
    
    # Verificar se podemos consultar a tabela
    query_ok = False
    try:
        logger.info(f"Tentando consultar tabela {PINOT_TABLE}...")
        response = requests.post(
            f"{PINOT_BROKER_URL}/query/sql",
            headers={"Content-Type": "application/json"},
            json={"sql": f"SELECT COUNT(*) FROM {PINOT_TABLE}"}
        )
        
        if response.status_code == 200:
            result = response.json()
            if "exceptions" not in result or not result["exceptions"]:
                logger.info("Consulta bem-sucedida, tabela está funcionando!")
                query_ok = True
            else:
                logger.warning(f"Consulta retornou exceções: {result.get('exceptions')}")
        else:
            logger.warning(f"Erro ao consultar tabela: {response.status_code}, {response.text}")
    except requests.exceptions.RequestException as e:
        logger.warning(f"Exceção ao consultar tabela: {str(e)}")
    
    if not query_ok:
        logger.warning("Tabela não está disponível para consultas, tentando corrigir...")
        
        # Tentar forçar a criação da tabela novamente
        try:
            # Primeiro tentar remover a tabela se ela existe
            logger.info("Tentando remover tabela se existir...")
            requests.delete(f"{PINOT_CONTROLLER_URL}/tables/{PINOT_TABLE}")
            
            # Aguardar um momento para que a remoção seja processada
            time.sleep(3)
            
            # Criar a tabela novamente
            return criar_tabela_pinot()
        except Exception as e:
            logger.error(f"Erro ao tentar corrigir a tabela: {str(e)}")
            return False
    
    return True

def verificar_kafka():
    """Verifica se o servidor Kafka está acessível"""
    logger.info(f"Verificando conexão com o Kafka em {KAFKA_BOOTSTRAP_SERVERS}...")
    
    try:
        # Criar um consumidor Kafka temporário para verificar conexão
        config = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'group.id': f"{KAFKA_GROUP_ID}-test",
            'auto.offset.reset': 'earliest',
            'session.timeout.ms': 6000
        }
        
        consumer = Consumer(config)
        
        # Listar tópicos para verificar a conexão
        metadata = consumer.list_topics(timeout=10)
        topics = metadata.topics
        
        # Fechar consumidor
        consumer.close()
        
        logger.info(f"Conexão com Kafka bem-sucedida! Tópicos disponíveis: {', '.join(topics.keys())}")
        
        # Verificar se o tópico de vendas existe
        if KAFKA_TOPIC in topics:
            logger.info(f"Tópico {KAFKA_TOPIC} encontrado!")
        else:
            logger.warning(f"Tópico {KAFKA_TOPIC} não encontrado! A tabela Pinot pode não funcionar.")
            logger.warning(f"Tópicos disponíveis: {', '.join(topics.keys())}")
        
        # Importante: Kafka funcionando para o script local não garante que o Pinot conseguirá acessá-lo
        if not IS_DOCKER:
            logger.warning(f"Executando em modo local. Pinot precisará acessar o Kafka em: {KAFKA_CONNECT_URL}")
            logger.warning("Certifique-se que este endereço é acessível do container do Pinot!")
        
        return True
    except Exception as e:
        logger.error(f"Erro ao conectar ao Kafka: {str(e)}")
        return False

def main():
    """Função principal para consumo e ingestão no Pinot"""
    # Configurar manipuladores de sinal para encerramento adequado
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)
    
    logger.info(f"Iniciando consumidor para o tópico: {KAFKA_TOPIC}")
    logger.info(f"Usando servidor Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"Grupo de consumidores: {KAFKA_GROUP_ID}")
    
    # Verificar conexão com Pinot
    if not verificar_pinot():
        logger.error("Não foi possível conectar ao Apache Pinot. Verifique se o serviço está em execução.")
        return
    
    # Verificar conexão com Kafka
    if not verificar_kafka():
        logger.error("Não foi possível conectar ao Kafka. Verifique se o serviço está em execução.")
        return
    
    # Criar schema
    logger.info("Etapa 1: Criação do schema...")
    if not criar_schema_pinot():
        logger.error("Falha na criação do schema. Abortando.")
        return
    
    # Criar tabela
    logger.info("Etapa 2: Criação da tabela...")
    if not criar_tabela_pinot():
        logger.error("Falha na criação da tabela. Abortando.")
        return
    
    # Verificar se a tabela está funcionando
    logger.info("Etapa 3: Verificação final da tabela...")
    if not verificar_tabela_e_corrigir():
        logger.error("Falha na verificação final da tabela. Abortando.")
        return
    
    # Realizar verificação manual para confirmar que a tabela está disponível para consulta
    logger.info("Etapa 4: Testando disponibilidade da tabela com consulta explícita...")
    try:
        test_query = f"SELECT COUNT(*) FROM {PINOT_TABLE}"
        logger.info(f"Enviando consulta: {test_query}")
        
        response = requests.post(
            f"{PINOT_BROKER_URL}/query/sql",
            headers={"Content-Type": "application/json"},
            json={"sql": test_query}
        )
        
        if response.status_code == 200:
            result = response.json()
            logger.info(f"Resposta da consulta: {json.dumps(result, indent=2)}")
            
            if "exceptions" in result and result["exceptions"]:
                logger.error(f"Consulta retornou exceções: {result['exceptions']}")
                logger.error("ERRO: A tabela foi criada mas não está respondendo corretamente às consultas.")
                logger.error("Por favor, verifique a configuração no Console do Pinot em http://localhost:9000")
                return
            else:
                logger.info("SUCESSO! A tabela está respondendo corretamente às consultas.")
                logger.info("Você pode consultar a tabela pelo Query Console do Pinot em http://localhost:9000")
        else:
            logger.error(f"Erro na consulta de teste: {response.status_code}, {response.text}")
            logger.error("A configuração pode não estar correta. Verifique os logs para mais detalhes.")
            return
    except Exception as e:
        logger.error(f"Erro durante teste de consulta: {str(e)}")
        return
    
    logger.info("Configuração do Pinot concluída com sucesso. Iniciando consumo de Kafka...")
    
    # Criar e configurar consumidor
    consumidor = criar_consumidor()
    
    try:
        # Inscrever nos tópicos
        consumidor.subscribe([KAFKA_TOPIC])
        logger.info(f"Inscrito no tópico: {KAFKA_TOPIC}")
        
        # Mensagens processadas
        contador = 0
        ultima_consulta = time.time()
        
        # Loop principal de consumo
        while running:
            # Tentar receber mensagem com timeout de 1 segundo
            msg = consumidor.poll(timeout=1.0)
            
            # Realizar consulta de teste a cada 30 segundos
            if time.time() - ultima_consulta > 30:
                testar_consulta_pinot()
                ultima_consulta = time.time()
            
            if msg is None:
                continue
            
            if msg.error():
                # Erro fatal no consumidor
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.info(f"Fim da partição alcançado {msg.topic()}/{msg.partition()}")
                elif msg.error().code() == KafkaError._TRANSPORT:
                    logger.error(f"Erro de transporte: {msg.error()}")
                else:
                    logger.error(f"Erro do consumidor: {msg.error()}")
                continue
            
            # Processar mensagem recebida
            try:
                # Kafka já está enviando para o Pinot baseado na configuração da tabela.
                # Isso é apenas para logar que as mensagens estão sendo processadas
                contador += 1
                if contador % 10 == 0:
                    logger.info(f"Processadas {contador} mensagens até o momento")
                    
                # Em um cenário real poderíamos adicionar validação ou transformação
                # antes da ingestão no Pinot, mas neste caso o Pinot usa o Kafka diretamente
                
            except Exception as e:
                logger.error(f"Erro ao processar mensagem: {str(e)}")
    
    except KafkaException as e:
        logger.error(f"Erro Kafka: {e}")
    finally:
        # Fechar o consumidor apropriadamente
        logger.info("Fechando consumidor Kafka...")
        consumidor.close()
        logger.info("Consumidor fechado. Saindo...")

if __name__ == "__main__":
    main() 