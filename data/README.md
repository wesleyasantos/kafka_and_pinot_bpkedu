# Diretório de Dados

Este diretório é destinado ao armazenamento de arquivos de dados utilizados ou gerados pelo pipeline.

## Propósito

- Armazenar dados de exemplo que podem ser carregados pelo pipeline
- Manter cópias locais de dados processados
- Armazenar resultados de análises ou extrações

## Estrutura

- `raw/` - Dados brutos antes do processamento
- `processed/` - Dados após processamento
- `output/` - Relatórios e resultados de análises

## Uso

Em um ambiente de produção, este diretório seria substituído por soluções de armazenamento adequadas como:

- Bancos de dados
- Object storage (S3, GCS, etc.)
- HDFS
- Sistemas de arquivos distribuídos

Para fins de demonstração e aprendizado, utilizamos o sistema de arquivos local. 