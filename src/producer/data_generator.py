#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Gerador de dados simulados de vendas para envio ao Kafka.
"""

import json
import time
import uuid
import random
from datetime import datetime
from confluent_kafka import Producer
from faker import Faker
import os
import logging

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Inicializar Faker para gerar dados realistas
fake = Faker('pt_BR')

# Configurações Kafka
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:29092')
KAFKA_TOPIC = os.environ.get('KAFKA_TOPIC', 'vendas-tempo-real')

# Configurações de simulação
INTERVALO_MIN_MS = 500  # Intervalo mínimo entre mensagens (ms)
INTERVALO_MAX_MS = 2000  # Intervalo máximo entre mensagens (ms)

# Constantes para simulação de dados
CATEGORIAS_PRODUTOS = [
    "Eletrônicos", "Roupas", "Alimentos", "Livros", "Móveis", 
    "Esportes", "Beleza", "Brinquedos", "Ferramentas", "Jóias"
]

PRODUTOS = {
    "Eletrônicos": ["Smartphone", "Notebook", "Tablet", "Smart TV", "Fones de Ouvido"],
    "Roupas": ["Camisa", "Calça", "Vestido", "Casaco", "Tênis"],
    "Alimentos": ["Café", "Chocolate", "Pão", "Arroz", "Feijão"],
    "Livros": ["Romance", "Técnico", "Biografia", "Ficção", "Auto-ajuda"],
    "Móveis": ["Sofá", "Mesa", "Cadeira", "Armário", "Cama"],
    "Esportes": ["Bola", "Raquete", "Tênis Esportivo", "Luvas", "Bicicleta"],
    "Beleza": ["Perfume", "Shampoo", "Maquiagem", "Creme", "Batom"],
    "Brinquedos": ["Boneca", "Carrinho", "Jogo de Tabuleiro", "Quebra-cabeça", "Pelúcia"],
    "Ferramentas": ["Martelo", "Furadeira", "Alicate", "Serra", "Chave de Fenda"],
    "Jóias": ["Anel", "Colar", "Brinco", "Pulseira", "Relógio"]
}

PRECOS = {
    "Eletrônicos": (500, 5000),
    "Roupas": (50, 500),
    "Alimentos": (5, 100),
    "Livros": (30, 200),
    "Móveis": (300, 3000),
    "Esportes": (50, 1000),
    "Beleza": (20, 300),
    "Brinquedos": (30, 500),
    "Ferramentas": (40, 800),
    "Jóias": (100, 5000)
}

FORMAS_PAGAMENTO = ["Crédito", "Débito", "Dinheiro", "PIX", "Boleto"]

ESTADOS_LOJAS = {
    "SP": ["São Paulo", "Campinas", "Santos", "Guarulhos"],
    "RJ": ["Rio de Janeiro", "Niterói", "Petrópolis"],
    "MG": ["Belo Horizonte", "Contagem", "Juiz de Fora"],
    "RS": ["Porto Alegre", "Caxias do Sul", "Pelotas"],
    "PR": ["Curitiba", "Londrina", "Maringá"]
}

# Função para gerar registro de venda
def gerar_venda():
    """Gera um registro simulado de venda"""
    # Dados temporais
    agora = datetime.now()
    timestamp = int(agora.timestamp() * 1000)
    data_hora = agora.isoformat()
    
    # Cliente
    id_cliente = str(uuid.uuid4())
    nome_cliente = fake.name()
    email_cliente = fake.email()
    
    # Produto e preço
    categoria = random.choice(CATEGORIAS_PRODUTOS)
    produto = random.choice(PRODUTOS[categoria])
    preco_min, preco_max = PRECOS[categoria]
    preco = round(random.uniform(preco_min, preco_max), 2)
    quantidade = random.randint(1, 5)
    valor_total = round(preco * quantidade, 2)
    
    # Pagamento
    forma_pagamento = random.choice(FORMAS_PAGAMENTO)
    
    # Localização
    estado = random.choice(list(ESTADOS_LOJAS.keys()))
    cidade = random.choice(ESTADOS_LOJAS[estado])
    loja = f"{cidade}-{random.randint(1, 10)}"
    
    # Montar registro
    return {
        "id_venda": str(uuid.uuid4()),
        "timestamp": timestamp,
        "data_hora": data_hora,
        "id_cliente": id_cliente,
        "nome_cliente": nome_cliente,
        "email_cliente": email_cliente,
        "produto": produto,
        "categoria": categoria,
        "preco": preco,
        "quantidade": quantidade,
        "valor_total": valor_total,
        "forma_pagamento": forma_pagamento,
        "loja": loja,
        "cidade": cidade,
        "estado": estado
    }

# Callback para confirmação de entrega
def delivery_report(err, msg):
    """Callback invocado quando a mensagem é entregue (ou falha)"""
    if err is not None:
        logger.error(f"Erro na entrega da mensagem: {err}")
    else:
        logger.info(f"Mensagem entregue ao tópico {msg.topic()} [partição {msg.partition()}] em {msg.offset()}")

# Configuração do produtor Kafka
def criar_produtor():
    """Cria e retorna uma instância do produtor Kafka"""
    config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'client.id': 'gerador-vendas'
    }
    return Producer(config)

def main():
    """Função principal que gera e envia dados para o Kafka"""
    logger.info(f"Iniciando gerador de dados para o tópico: {KAFKA_TOPIC}")
    logger.info(f"Usando servidor Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    
    produtor = criar_produtor()
    
    try:
        contador = 0
        while True:  # Loop infinito para geração contínua
            # Gerar venda
            venda = gerar_venda()
            
            # Serializar para JSON
            mensagem = json.dumps(venda).encode('utf-8')
            
            # Enviar para o Kafka
            produtor.produce(
                KAFKA_TOPIC,
                key=venda['id_venda'].encode('utf-8'),
                value=mensagem,
                callback=delivery_report
            )
            
            # Garantir entrega
            produtor.poll(0)
            
            # Incrementar contador
            contador += 1
            if contador % 10 == 0:
                logger.info(f"Geradas {contador} mensagens até o momento")
            
            # Aguardar intervalo aleatório para simular fluxo real
            intervalo = random.randint(INTERVALO_MIN_MS, INTERVALO_MAX_MS) / 1000.0
            time.sleep(intervalo)
            
    except KeyboardInterrupt:
        logger.info("Interrompido pelo usuário. Finalizando...")
    finally:
        # Garantir que todas as mensagens pendentes sejam enviadas
        logger.info("Aguardando entrega das mensagens pendentes...")
        produtor.flush()
        logger.info("Finalizado com sucesso!")

if __name__ == "__main__":
    main() 