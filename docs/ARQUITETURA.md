# Arquitetura do Pipeline de Dados

Este documento descreve a arquitetura do pipeline de dados em tempo real implementado neste projeto.

## Visão Geral

O pipeline é composto por:

1. **Geração de Dados**: Simulação de dados de vendas em Python
2. **Apache Kafka**: Plataforma de streaming para ingestão de dados
3. **Apache Pinot**: Banco de dados OLAP para análise em tempo real
4. **Consumidores Python**: Processamento e transformação de dados
5. **Jupyter Notebook**: Análise e visualização de dados

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│              │     │              │     │              │     │              │
│  Gerador     ├────►│    Kafka     ├────►│   Pinot      ├────►│  Visualização│
│  de Dados    │     │  (Streaming) │     │   (OLAP)     │     │  (Notebook)  │
│              │     │              │     │              │     │              │
└──────────────┘     └─────┬────────┘     └──────────────┘     └──────────────┘
                           │
                           │
                      ┌────▼────────┐
                      │             │
                      │ Consumidores│
                      │    Python   │
                      │             │
                      └─────────────┘
```

## Componentes

### 1. Gerador de Dados (Produtor Kafka)

- Implementado em Python (`src/producer/data_generator.py`)
- Simula dados de vendas com características realistas usando a biblioteca Faker
- Produz mensagens JSON para o tópico Kafka `vendas-tempo-real`
- Configura campos com timestamps, valores, categorias, formas de pagamento, etc.

### 2. Apache Kafka

- Plataforma de streaming distribuído
- Tópico: `vendas-tempo-real`
- Recebe mensagens dos produtores e as disponibiliza para consumidores
- Garante armazenamento temporário, replicação e tolerância a falhas
- Interface de administração disponível em `http://localhost:8080`

### 3. Consumidores Kafka

- **Consumidor Básico** (`src/consumer/kafka_consumer.py`): 
  - Lê mensagens do Kafka e pode realizar processamento personalizado
  - Demonstra como eventos podem ser transformados, filtrados ou enriquecidos

- **Consumidor para Pinot** (`src/consumer/pinot_consumer.py`):
  - Configura a integração entre Kafka e Apache Pinot
  - Gerencia a criação de schemas e tabelas no Pinot
  - Monitora a ingestão de dados no Pinot

### 4. Apache Pinot

- Banco de dados OLAP para análise em tempo real
- Recebe dados diretamente do Kafka via streaming
- Fornece capacidades de consulta SQL para análises
- Componentes:
  - **Controller**: Gerencia metadados e coordena o cluster
  - **Broker**: Processa consultas e retorna resultados
  - **Server**: Armazena e indexa dados para consulta rápida
- Interface web disponível em `http://localhost:9000`

### 5. Análise e Visualização

- Jupyter Notebook (`notebooks/analise_vendas.ipynb`):
  - Executa consultas SQL no Pinot
  - Realiza análises usando Pandas
  - Cria visualizações com Matplotlib e Seaborn
  - Demonstra análise em tempo real (atualização a cada poucos segundos)

## Fluxo de Dados

1. O gerador de dados simula transações de vendas
2. As transações são serializadas em JSON e enviadas ao Kafka
3. O Apache Pinot consome mensagens do Kafka através de sua configuração de streaming
4. O script de consumidor do Pinot monitora e gerencia a ingestão de dados
5. Os dados ingeridos são indexados no Pinot para consultas rápidas
6. O notebook Jupyter executa consultas SQL no Pinot para análise e visualização

## Persistência de Dados

- Kafka mantém dados temporariamente conforme as configurações de retenção
- Pinot armazena dados segmentados para consulta rápida
- O sistema é projetado para processamento em tempo real, com opções para análise histórica

## Escalabilidade

O pipeline pode ser escalonado de várias formas:

- **Kafka**: Adicionando mais brokers e partições para aumentar throughput
- **Pinot**: Adicionando mais servidores para escalonamento horizontal
- **Produtores/Consumidores**: Instanciando múltiplas cópias para processamento paralelo

## Monitoramento

- Kafka UI: `http://localhost:8080`
- Pinot Controller UI: `http://localhost:9000`
- Logs de cada componente disponíveis nos containers Docker

## Configuração

- Schema do Pinot: `config/pinot_schema.json`
- Configuração da tabela Pinot: `config/pinot_table.json`
- Docker Compose: `docker-compose.yml` 