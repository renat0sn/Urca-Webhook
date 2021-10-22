import os
import config
from shapely.geometry.point import Point
import excel as xf
import pandas as pd
from datetime import timedelta, date

from sqlalchemy import create_engine

CONN_MYSQL = create_engine('mysql://{0}:{1}@{2}/{3}'.format(config.USER,config.PASSWORD,config.SERVER,config.DATABASE))

'''
ipdo_to_mysql(file,connection) : Recebe um arquivo ipdo excel, o transforma em dataframe com os dados 
formatados e joga no banco de dados, através da conexão 'connection'

Dados capturados (por subsistema):
- Data do arquivo
- ENA, ENA percentual e ENA armazenável percentual
- EARM e EARM percentual
- Carga prevista e carga realizada
- Geração: eólica e solar

Parâmetros:
- file : caminho do arquivo IPDO, com nome e extensão
- connection : conexão com o banco de dados para realizar operações de inserção
'''
def ipdo_to_mysql(file):
    # Dados do banco de dados
    '''    table_name = 'ipdo'
    engine_str = CONN_MYSQL
    database = config.DATABASE

    # Pega arquivo IPDO do excel e o coloca em um DataFrame
    ipdo_data = xf.Excel_Ipdo(file)
    df = ipdo_data.get_IPDO_dataframe()
    # Trocar nomes de subsistema por seus id's
    df = df.replace({'Sudeste': 1, 'Sul': 2, 'Nordeste': 3, 'Norte': 4, 'SIN': 5})
    with CONN_MYSQL.connect() as connection:
        try:
            data_ipdo = str(df.data[0].strftime('%Y-%m-%d'))
            # Verifica se a data deste IPDO já existe no BD
            check = connection.execute('SELECT EXISTS(SELECT * FROM {0} WHERE data="{1}");'.format(table_name, data_ipdo)).first()[0]
            # Se existe, os remove do BD para colocar dados atuais
            if check == 1:
                sql_command = 'DELETE FROM {0} WHERE data="{1}";'.format(table_name, data_ipdo)
                connection.execute(sql_command)

            # Insere DataFrame no BD
            df.to_sql(table_name, con=engine_str, schema=database, if_exists='append', index=False)
            print('Inserção OK!')
            return True
        except:
            print('Inserção ERRO!')
            return False'''