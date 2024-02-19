import time
import logging
import psycopg2
from config import POSTGRES_CONFIG

logger = logging.getLogger(__name__)


def conectar_postgresql():
    """
    Tenta estabelecer uma conexão com o banco de dados PostgreSQL.

    Returns:
        psycopg2.extensions.connection: Conexão com o banco de dados PostgreSQL.
    """
    max_attempts = 15
    attempt = 0
    connection = None
    while attempt < max_attempts:
        try:
            connection = psycopg2.connect(**POSTGRES_CONFIG)
            break
        except psycopg2.Error as error:
            logger.error(CONNECTION_ERROR.format(attempt + 1, error))
            attempt += 1
            time.sleep(5)
    return connection


CONNECTION_ERROR = "Erro ao conectar ao banco de dados PostgreSQL - Tentativa {}: {}"
