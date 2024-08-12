# SCDManager

`SCDManager` é uma classe Python projetada para facilitar a criação e gerenciamento de tabelas de dimensão do tipo Slowly Changing Dimension (SCD) no Spark, utilizando o Delta Lake. Esta classe oferece suporte para SCD do Tipo 1 e Tipo 2, permitindo a criação, inserção e atualização de dimensões em um DataFrame Spark dentro do ambiente Databricks.

## Índice

- [Uso no Databricks](#uso-no-databricks)
- [Funcionalidades](#funcionalidades)
  - [generate_change_date](#generate_change_date)
  - [table_exists](#table_exists)
  - [create_dimension_scd2](#create_dimension_scd2)
  - [upsert_scd_2](#upsert_scd_2)
  - [create_dimension_scd1](#create_dimension_scd1)
  - [upsert_scd_1](#upsert_scd_1)
  - [manage_scd_table](#manage_scd_table)
- [Exemplo de Uso no Databricks](#exemplo-de-uso-no-databricks)
- [Contribuição](#contribuição)
- [Licença](#licença)

## Uso no Databricks

Esta classe foi projetada para ser utilizada diretamente em notebooks do Databricks. Certifique-se de que o ambiente está configurado corretamente, com o Delta Lake habilitado.

Você pode copiar e colar o código da classe `SCDManager` diretamente em uma célula de código Python no Databricks ou adicioná-lo como uma biblioteca em um repositório de código compartilhado no Databricks.

## Funcionalidades

### `generate_change_date`

Retorna o timestamp atual ajustado para o fuso horário especificado.

**Parâmetros:**

- `time_zone (str)`: O fuso horário para ajustar o timestamp.

**Retorno:**

- `datetime`: O timestamp atual ajustado para o fuso horário especificado.

### `table_exists`

Verifica se uma tabela Delta existe em um determinado esquema.

**Parâmetros:**

- `schema_name (str)`: Nome do esquema onde a tabela está localizada.
- `table_name (str)`: Nome da tabela.

**Retorno:**

- `bool`: `True` se a tabela existe, `False` caso contrário.

### `create_dimension_scd2`

Cria uma dimensão do tipo SCD-2.

**Parâmetros:**

- `df (DataFrame)`: DataFrame contendo os dados a serem inseridos na dimensão.
- `schema_name (str)`: Nome do esquema onde a tabela será criada.
- `table_name (str)`: Nome da tabela a ser criada.
- `sk_name (str)`: Nome da coluna da chave substituta (SK).
- `columns_to_hash (list)`: Lista de colunas utilizadas para gerar a chave substituta (SK).

**Retorno:**

- `DataFrame`: DataFrame final com a dimensão criada.

### `upsert_scd_2`

Realiza um upsert em uma dimensão do tipo SCD-2.

**Parâmetros:**

- `df_origem (DataFrame)`: DataFrame contendo os dados de origem a serem mesclados.
- `schema_name (str)`: Nome do esquema onde a tabela está localizada.
- `table_name (str)`: Nome da tabela onde o upsert será realizado.
- `sk_name (str)`: Nome da coluna da chave substituta (SK).
- `pk_name (str)`: Nome da coluna da chave primária (PK).
- `partition_columns (list)`: Lista de colunas de partição.
- `matched_columns (list)`: Lista de colunas usadas para correspondência no merge.

### `create_dimension_scd1`

Cria uma dimensão do tipo SCD-1.

**Parâmetros:**

- `df (DataFrame)`: DataFrame contendo os dados a serem inseridos na dimensão.
- `schema_name (str)`: Nome do esquema onde a tabela será criada.
- `table_name (str)`: Nome da tabela a ser criada.
- `sk_name (str)`: Nome da coluna da chave substituta (SK).
- `columns_to_hash (list)`: Lista de colunas utilizadas para gerar a chave substituta (SK).

**Retorno:**

- `DataFrame`: DataFrame final com a dimensão criada.

### `upsert_scd_1`

Realiza um upsert em uma dimensão do tipo SCD-1.

**Parâmetros:**

- `df_origem (DataFrame)`: DataFrame contendo os dados de origem a serem mesclados.
- `schema_name (str)`: Nome do esquema onde a tabela está localizada.
- `table_name (str)`: Nome da tabela onde o upsert será realizado.
- `sk_name (str)`: Nome da coluna da chave substituta (SK).
- `pk_name (str)`: Nome da coluna da chave primária (PK).
- `partition_columns (list)`: Lista de colunas de partição.
- `matched_columns (list)`: Lista de colunas usadas para correspondência no merge.

### `manage_scd_table`

Gerencia a criação e o upsert de uma tabela de dimensão SCD do tipo 1 ou 2.

**Parâmetros:**

- `scd_type (int)`: Tipo de SCD (1 ou 2).
- `df (DataFrame)`: DataFrame contendo os dados a serem inseridos ou atualizados.
- `schema_name (str)`: Nome do esquema onde a tabela está localizada.
- `table_name (str)`: Nome da tabela onde o upsert será realizado.
- `sk_name (str)`: Nome da coluna da chave substituta (SK).
- `pk_name (str)`: Nome da coluna da chave primária (PK).
- `partition_columns (list)`: Lista de colunas de partição.
- `matched_columns (list)`: Lista de colunas usadas para correspondência no merge.

## Exemplo de Uso no Databricks

Aqui está um exemplo básico de como utilizar a classe `SCDManager` em um notebook do Databricks:

```python
# Certifique-se de que o código da classe SCDManager esteja em uma célula anterior ou em um módulo importado

# Instanciando o SCDManager
scd_manager = SCDManager()

# Suponha que você tenha um DataFrame chamado df que precisa ser gerenciado como uma dimensão SCD-2
df = ...  # Seu DataFrame aqui

# Gerenciando a tabela SCD-2
scd_manager.manage_scd_table(
    scd_type=2,
    df=df,
    schema_name="meu_esquema",
    table_name="minha_tabela_scd2",
    sk_name="SK",
    pk_name="ID",
    partition_columns=["coluna1", "coluna2"],
    matched_columns=["coluna1", "coluna2"]
)
```

## Contribuição

Contribuições são bem-vindas! Se você encontrar algum problema ou tiver sugestões de melhorias, sinta-se à vontade para abrir uma _issue_.


## Licença
Este projeto é licenciado sob os termos da licença MIT. Consulte o arquivo LICENSE para mais detalhes.


