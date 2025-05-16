#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Consumidor Kafka para ler e processar mensagens de vendas em tempo real.
"""

import json
import time
import signal
import sys
import logging
from confluent_kafka import Consumer, KafkaError, KafkaException
import os

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configurações Kafka
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:29092')
KAFKA_TOPIC = os.environ.get('KAFKA_TOPIC', 'vendas-tempo-real')
KAFKA_GROUP_ID = os.environ.get('KAFKA_GROUP_ID', 'grupo-consumidor-vendas')
KAFKA_AUTO_OFFSET_RESET = os.environ.get('KAFKA_AUTO_OFFSET_RESET', 'earliest')

# Controle para interrupções
running = True

def processar_mensagem(msg_value):
    """
    Processa uma mensagem de venda.
    Esta função pode ser adaptada para realizar transformações, validações,
    armazenamento em banco de dados, envio para outros sistemas, etc.
    
    Args:
        msg_value (dict): Dados da venda
    """
    # Exemplo de processamento simples
    categoria = msg_value.get('categoria')
    valor = msg_value.get('valor_total', 0)
    data_hora = msg_value.get('data_hora')
    
    logger.info(f"Processando venda: {data_hora} - {categoria} - R$ {valor:.2f}")
    
    # Aqui pode ser adicionada a lógica para:
    # - Validar os dados
    # - Transformar os dados (ETL)
    # - Armazenar em bancos de dados
    # - Realizar cálculos/agregações
    # - Chamar outros serviços
    
    # Exemplo de validação simples
    if valor <= 0:
        logger.warning(f"Valor inválido: {valor}")
    
    # Simular pequeno atraso para representar processamento
    time.sleep(0.01)
    
    return True

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

def main():
    """Função principal para consumo de mensagens do Kafka"""
    # Configurar manipuladores de sinal para encerramento adequado
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)
    
    logger.info(f"Iniciando consumidor para o tópico: {KAFKA_TOPIC}")
    logger.info(f"Usando servidor Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"Grupo de consumidores: {KAFKA_GROUP_ID}")
    
    # Criar e configurar consumidor
    consumidor = criar_consumidor()
    
    try:
        # Inscrever nos tópicos
        consumidor.subscribe([KAFKA_TOPIC])
        logger.info(f"Inscrito no tópico: {KAFKA_TOPIC}")
        
        # Mensagens processadas
        contador = 0
        
        # Loop principal de consumo
        while running:
            # Tentar receber mensagem com timeout de 1 segundo
            msg = consumidor.poll(timeout=1.0)
            
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
                # Decodificar mensagem JSON
                valor = json.loads(msg.value().decode('utf-8'))
                
                # Processar mensagem
                if processar_mensagem(valor):
                    contador += 1
                    if contador % 10 == 0:
                        logger.info(f"Processadas {contador} mensagens até o momento")
                
            except json.JSONDecodeError:
                logger.error(f"Erro ao decodificar JSON: {msg.value().decode('utf-8')}")
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