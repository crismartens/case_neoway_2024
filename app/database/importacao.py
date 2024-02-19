import os
import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)


def importar_dados_arquivo(arquivo_config):
    """
    Importa dados de um arquivo base e realiza algumas transformações nos dados.

    Args:
        arquivo_config: Dados do arquivo com a base.

    Returns:
        pd.DataFrame: DataFrame contendo os dados importados.
    """
    try:
        caminho_arquivo = obter_caminho_arquivo(arquivo_config['name'])
        df_importado = pd.read_fwf(caminho_arquivo, widths=arquivo_config['width_columns'], header=None, skiprows=1)
        df_importado.columns = arquivo_config['name_columns']
        df_importado[arquivo_config['docbr_columns']] = df_importado[arquivo_config['docbr_columns']].apply(lambda x: x.str.replace('[^\w]', '', regex=True))
        df_importado[arquivo_config['decimal_columns']] = df_importado[arquivo_config['decimal_columns']].replace(',','.',regex=True).astype(float)
        df_importado = df_importado.replace({np.nan: None})
        df_importado[arquivo_config['decimal_columns']] = df_importado[arquivo_config['decimal_columns']].astype(float)
        return df_importado
    except Exception as error:
        logger.error(IMPORT_ERROR.format(arquivo_config['name'], error))
        return None


def obter_caminho_arquivo(nome):
    """
    Obtém o caminho completo de um arquivo no diretório 'base'.

    Args:
        nome (str): Nome do arquivo.

    Returns:
        str: Caminho completo do arquivo.
    """
    caminho_arquivo = os.path.abspath(os.path.join('base/', nome))
    return caminho_arquivo


IMPORT_ERROR = "Erro ao importar arquivo '{}': {}"