# config.py
import os

POSTGRES_CONFIG = {
    'database': os.environ.get("POSTGRES_DB", "postgres"),
    'user': os.environ.get("POSTGRES_USER", "postgres"),
    'password': os.environ.get("POSTGRES_PASSWORD", "postgres"),
    'host': "postgres",
    'port': "5432"
}

BASE = [
    {
        'name': 'base_teste.txt',
        'name_columns': ['cpf', 'private', 'incompleto', 'data_ultima_compra', 'ticket_medio', 'ticket_ultima_compra', 'loja_mais_frequente', 'loja_ultima_compra'],
        'width_columns': [19, 12, 12, 22, 22, 24, 20, 22],
        'docbr_columns': ['cpf', 'loja_mais_frequente', 'loja_ultima_compra'],
        'decimal_columns': ['ticket_medio', 'ticket_ultima_compra']
    }
]
