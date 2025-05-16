# Como Executar o Pipeline de Dados

Este guia fornece instruções passo a passo para configurar e executar o pipeline de dados em tempo real com Kafka e Pinot.

## Pré-requisitos

- Docker e Docker Compose instalados
- Python 3.8 ou superior
- Git (opcional, mas recomendado)

## Instalação

### 1. Clone o repositório (opcional)

```bash
git clone https://github.com/wesleyasantos/kafka_and_pinot_bpkedu.git
cd ["NOME DA PASTA"]
```

### 2. Instale as dependências Python

```bash
pip install -r requirements.txt
```

## Executando os serviços

### 1. Inicie os containers usando Docker Compose

```bash
docker-compose up -d
```

Este comando iniciará os seguintes serviços:
- ZooKeeper
- Kafka
- Kafka UI (interface web para Kafka)
- Pinot Controller
- Pinot Broker
- Pinot Server

### 2. Verifique se os serviços estão em execução

```bash
docker-compose ps
```

Todos os serviços devem estar no estado "Up".

### 3. Acesse as interfaces web

- **Kafka UI**: http://localhost:8080
- **Pinot UI**: http://localhost:9000

## Gerando e processando dados

### 1. Iniciar o produtor de dados

O produtor de dados gerará dados simulados de vendas e os enviará para o Kafka:

```bash
python src/producer/data_generator.py
```

### 2. Iniciar o consumidor Kafka (opcional, para visualizar processamento)

Este consumidor lerá e processará os dados do tópico Kafka:

```bash
python src/consumer/kafka_consumer.py
```

### 3. Iniciar o consumidor Pinot (para configurar integração com Pinot)

Este consumidor gerenciará a conexão entre Kafka e Pinot:

```bash
python src/consumer/pinot_consumer.py
```

## Análise dos dados

### 1. Iniciar Jupyter Notebook

```bash
jupyter notebook
```

### 2. Abrir o notebook de análise

Navegue até `notebooks/analise_vendas.ipynb` e abra o notebook.

### 3. Executar as células do notebook

Execute as células sequencialmente para realizar análises nos dados em tempo real.

## Monitoramento

Você pode monitorar o pipeline de diversas formas:

1. **Logs dos produtores/consumidores**:
   Os logs mostrarão a atividade de produção e consumo em tempo real.

2. **Kafka UI** (http://localhost:8080):
   - Visualize tópicos, partições e mensagens
   - Monitore produtores e consumidores conectados
   - Verifique métricas de throughput

3. **Pinot UI** (http://localhost:9000):
   - Consulte dados usando a interface SQL
   - Monitore tabelas e segmentos
   - Verifique a ingestão de dados

## Experimentos sugeridos

1. **Modificar o gerador de dados**:
   - Altere `src/producer/data_generator.py` para gerar dados diferentes
   - Experimente com diferentes frequências de mensagens
   - Adicione novos campos ou categorias

2. **Criar consultas personalizadas**:
   - No notebook, adicione novas consultas SQL para extrair insights diferentes
   - Experimente com agregações, filtros e janelas temporais

3. **Adicionar transformações aos dados**:
   - Modifique `src/consumer/kafka_consumer.py` para realizar transformações nos dados
   - Implemente cálculos em tempo real (médias móveis, detecção de anomalias, etc.)

## Parando os serviços

Para parar todos os serviços:

```bash
# Parar os scripts Python (Ctrl+C nos terminais em execução)
# Parar os containers Docker
docker-compose down
```

## Solução de problemas

### Problemas com Kafka

1. **Tópico não foi criado automaticamente**:
   ```bash
   docker-compose exec kafka kafka-topics --create --topic vendas-tempo-real --partitions 1 --replication-factor 1 --bootstrap-server kafka:9092
   ```

2. **Verificar conteúdo do tópico**:
   ```bash
   docker-compose exec kafka kafka-console-consumer --topic vendas-tempo-real --from-beginning --bootstrap-server kafka:9092
   ```

### Problemas com Pinot

1. **Verificar status do controlador**:
   ```bash
   curl http://localhost:9000/health
   ```

2. **Recriar manualmente o schema e tabela**:
   ```bash
   curl -X POST -H "Content-Type: application/json" -d @config/pinot_schema.json http://localhost:9000/schemas
   curl -X POST -H "Content-Type: application/json" -d @config/pinot_table.json http://localhost:9000/tables
   ```

### Verificar registros ingeridos

Execute a seguinte consulta na interface SQL do Pinot (http://localhost:9000):

```sql
SELECT COUNT(*) FROM vendas
``` 