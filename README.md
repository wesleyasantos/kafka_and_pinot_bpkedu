# Pipeline de Dados em Tempo Real: Kafka, Pinot e Python

Este projeto demonstra um pipeline de dados ETL em tempo real usando:
- **Apache Kafka**: Plataforma de streaming distribuído
- **Apache Pinot**: Banco de dados OLAP para análise em tempo real
- **Python**: Linguagem de programação para produtores e consumidores Kafka

## Estrutura do Projeto

```
.
├── config/              # Arquivos de configuração para Kafka e Pinot
├── data/                # Dados de exemplo
├── docs/                # Documentação adicional
├── notebooks/           # Jupyter notebooks para análise
└── src/
    ├── producer/        # Código do produtor Kafka
    ├── consumer/        # Código do consumidor Kafka
    ├── connector/       # Conectores Kafka-Pinot
    └── schemas/         # Esquemas Avro/JSON
```

## Requisitos

- Docker e Docker Compose
- Python 3.8+
- Apache Kafka
- Apache Pinot

## Como executar

1. Iniciar os serviços:
   ```
   docker-compose up -d
   ```

2. Executar o produtor:
   ```
   python src/producer/data_generator.py
   ```

3. Verificar dados no Pinot:
   ```
   http://localhost:9000
   ```

## Fluxo de Dados

1. Geração de dados simulados (Python)
2. Ingestão no Kafka (Produtor Python)
3. Consumo dos dados (Consumidor Python)
4. Armazenamento no Apache Pinot
5. Visualização e análise em tempo real

## Referências

- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Apache Pinot Documentation](https://docs.pinot.apache.org/) 