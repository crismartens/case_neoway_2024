import pandas as pd
import logging
from validate_docbr import CPF, CNPJ
from psycopg2.extras import execute_batch
from . import conexao

logger = logging.getLogger(__name__)


def processar_dados(df):
    """
    Processa os dados de um DataFrame e os persiste no banco de dados PostgreSQL.

    Args:
        df (pd.DataFrame): DataFrame contendo os dados a serem persistidos.
    """

    connection = conexao.conectar_postgresql()
    if connection:
        try:
            criar_tabelas_postgresql(connection)
            inserir_dados_postgresql(df, connection)
            logger.info("Dados inseridos com sucesso!")
        except Exception as e:
            logger.error(f"Erro ao processar os dados: {e}")
        finally:
            connection.close()
    else:
        logger.error(CONNECTION_ERROR)


def criar_tabelas_postgresql(connection):
    """
    Cria as tabelas necessárias no banco de dados PostgreSQL.

    Args:
        connection (psycopg2.extensions.connection): Conexão com o banco de dados PostgreSQL.
    """
    try:
        with connection.cursor() as cursor:
            for query in [CREATE_CPF_TABLE, CREATE_CNPJ_TABLE, CREATE_PERFIL_PESSOAS_TABLE]:
                cursor.execute(query)
        connection.commit()
        logger.info("Tabelas criadas com sucesso!")
    except Exception as e:
        logger.error(f"Erro ao criar tabelas: {e}")


def inserir_dados_postgresql(df, connection):
    try:
        with connection.cursor() as cursor:
            inserir_cpfs(cursor, df)
            inserir_cnpjs(cursor, df)
            inserir_perfis(cursor, df)
        connection.commit()
        logger.info("Dados inseridos com sucesso!")
    except Exception as e:
        logger.error(f"Erro ao inserir dados: {e}")


def inserir_cpfs(cursor, df):
    """
    Insere os CPFs válidos e inválidos no banco de dados PostgreSQL.

    Args:
        cursor (psycopg2.extensions.cursor): Cursor para executar consultas SQL.
        df (pd.DataFrame): DataFrame contendo os dados a serem inseridos.
    """
    cpfs = df['cpf'].dropna().unique()
    cpfs_validados = []
    for cpf in cpfs:
        cpf_validado = validar_documento(cpf, tipo='CPF')
        if cpf_validado is not None:
            cpfs_validados.append((cpf, cpf_validado))
    cursor.executemany(INSERT_CPF_QUERY, cpfs_validados)


def inserir_cnpjs(cursor, df):
    """
    Insere os CNPJs válidos e inválidos no banco de dados PostgreSQL.

    Args:
        cursor (psycopg2.extensions.cursor): Cursor para executar consultas SQL.
        df (pd.DataFrame): DataFrame contendo os dados a serem inseridos.
    """
    cnpjs = pd.concat([df['loja_mais_frequente'], df['loja_ultima_compra']]).dropna().unique()
    cnpjs_validados = []
    for cnpj in cnpjs:
        cnpj_validado = validar_documento(cnpj, tipo='CNPJ')
        if cnpj_validado is not None:
            cnpjs_validados.append((cnpj, cnpj_validado))
    cursor.executemany(INSERT_CNPJ_QUERY, cnpjs_validados)


def inserir_perfis(cursor, df):
    """
    Insere os perfis de pessoas no banco de dados PostgreSQL.

    Args:
        cursor (psycopg2.extensions.cursor): Cursor para executar consultas SQL.
        df (pd.DataFrame): DataFrame contendo os dados a serem inseridos.
    """
    perfis = []
    for index, row in df.iterrows():

        cpf_id = get_id(cursor, 'cpf', row['cpf'])
        cnpj_mais_frequente_id = get_id(cursor, 'cnpj', row['loja_mais_frequente'])
        cnpj_ultima_compra_id = get_id(cursor, 'cnpj', row['loja_ultima_compra'])

        if cpf_id:
            perfis.append((
                cpf_id,
                bool(row['private']),
                bool(row['incompleto']),
                row['data_ultima_compra'],
                row['ticket_medio'],
                row['ticket_ultima_compra'],
                cnpj_mais_frequente_id,
                cnpj_ultima_compra_id
            ))
    cursor.executemany(INSERT_PERFIL_PESSOAS_QUERY, perfis)


def validar_documento(doc, tipo):
    """
    Valida um documento (CPF ou CNPJ).

    Args:
        doc (str): Documento a ser validado.
        tipo (str): Tipo de documento ('CPF' ou 'CNPJ').

    Returns:
        bool: Retorna True se o documento for válido, False caso contrário.
    """
    if doc and tipo in ['CPF', 'CNPJ'] and doc.isdigit():
        if tipo == 'CPF':
            return CPF().validate(doc)
        elif tipo == 'CNPJ':
            return CNPJ().validate(doc)
    return None


def get_id(cursor, table, value):
    """
    Obtém o ID correspondente ao valor na tabela especificada.

    Args:
        cursor (psycopg2.extensions.cursor): Cursor para executar consultas SQL.
        table (str): Nome da tabela.
        value (str): Valor a ser buscado na tabela.

    Returns:
        int: ID correspondente ao valor na tabela.
    """
    if value:
        cursor.execute(f"SELECT id FROM {table} WHERE {table} = %s", (str(value),))
        return cursor.fetchone()[0] if cursor.rowcount else None
    return None


CONNECTION_ERROR = "Erro: Falha ao conectar ao banco de dados."

CREATE_CPF_TABLE = """
    CREATE TABLE IF NOT EXISTS cpf (
        id SERIAL PRIMARY KEY,
        cpf VARCHAR(14) UNIQUE NOT NULL,
        valido BOOLEAN
    )
"""

CREATE_CNPJ_TABLE = """
    CREATE TABLE IF NOT EXISTS cnpj (
        id SERIAL PRIMARY KEY,
        cnpj VARCHAR(14) UNIQUE NOT NULL,
        valido BOOLEAN
    )
"""

CREATE_PERFIL_PESSOAS_TABLE = """
    CREATE TABLE IF NOT EXISTS perfil_pessoas (
        id SERIAL PRIMARY KEY,
        cpf_id INTEGER REFERENCES cpf(id),
        private BOOLEAN NOT NULL DEFAULT TRUE,
        incompleto BOOLEAN NOT NULL DEFAULT TRUE,
        data_ultima_compra DATE,
        ticket_medio NUMERIC(10, 2),
        ticket_ultima_compra NUMERIC(10, 2),
        cnpj_mais_frequente_id INTEGER,
        cnpj_ultima_compra_id INTEGER,
        FOREIGN KEY (cnpj_mais_frequente_id) REFERENCES cnpj(id),
        FOREIGN KEY (cnpj_ultima_compra_id) REFERENCES cnpj(id)
    )
"""

INSERT_CPF_QUERY = """
    INSERT INTO cpf (cpf, valido) VALUES (%s, %s) ON CONFLICT DO NOTHING
"""

INSERT_CNPJ_QUERY = """
    INSERT INTO cnpj (cnpj, valido) VALUES (%s, %s) ON CONFLICT DO NOTHING
"""

INSERT_PERFIL_PESSOAS_QUERY = """
    INSERT INTO perfil_pessoas 
    (cpf_id, private, incompleto, data_ultima_compra, ticket_medio, ticket_ultima_compra, cnpj_mais_frequente_id, cnpj_ultima_compra_id)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""
