import os
import logging
from . import persistencia
from . import importacao
from config import BASE

logger = logging.getLogger(__name__)

def processar_arquivos():
    for arquivo_config in BASE:
        try:
            df = importacao.importar_dados_arquivo(arquivo_config)
            if df is not None:
                persistencia.processar_dados(df)
            else:
                logger.error(NO_DATAFRAME_ERROR)
        except Exception as e:
            logger.exception(PROCESSING_ERROR.format(arquivo_config['name'], e))

NO_DATAFRAME_ERROR = "Erro: Nenhum DataFrame importado."
PROCESSING_ERROR = "Ocorreu um erro durante o processamento do arquivo '{}': {}"
