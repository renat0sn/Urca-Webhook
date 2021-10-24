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
    table_name = 'ipdo'
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
            return False

'''
acomph_to_mysql(file,connection) : Recebe um arquivo acomph excel, o transforma em dataframe com os dados 
formatados e joga no banco de dados, através da conexão 'connection'

Dados capturados (por posto):
- Datas dos últimos 30 dias
- Vazão natural
- Vazão incremental
- ENA (calculado a partir da vazão natural * produtibilidade do posto)

Parâmetros:
- file : caminho do arquivo ACOMPH, com nome e extensão
- connection : conexão com o banco de dados para realizar operações de inserção
'''

from models.produt import Acomph

def acomph_to_mysql(file):
    # Dados do banco de dados
    table_name = 'acomph'
    engine_str = CONN_MYSQL
    database = config.DATABASE

    data = xf.Excel_Acomph(file)
    df = pd.DataFrame()

    # Percorre todas as 18 planilhas do Acomph e adiciona à df o dataframe referente à planilha
    for i in range (18):
        df = df.append(data.get_dataframe_by_sheet(sheet=i))

    with CONN_MYSQL.connect() as connection:
        # Cria um dataframe para calcular postos que não estão no acomph
        df = Acomph.get_others_acomph(df,connection).reset_index(drop=True).sort_values(by='data', ascending=True)

        # Armazena em cod_prod todas as produtibilidades disponíveis no banco de dados
        sql_command = 'SELECT cod_posto,produtibilidade FROM posto;'
        cod_prod = pd.DataFrame(connection.execute(sql_command).fetchall()).set_index(0)

        # Para cada codigo de posto no dataframe, calcula o ena
        for cod in df.cod_posto:
            produtibilidade = cod_prod.loc[cod,1]
            # Condição visando Belo Monte e Pimental, que já possuem ENA calculado
            if any(df[df.cod_posto == cod].ena.isna()):
                df.loc[df.cod_posto == cod, 'ena'] = (df[df.cod_posto==cod].vazao_natural * produtibilidade)
        
        try:
            data_acomph_min = str(min(df.data).strftime('%Y-%m-%d'))
            data_acomph_max = str(max(df.data).strftime('%Y-%m-%d'))
            # Remove dados dos últimos 30 dias do acomph para substituir pelos dados atuais
            sql_command = 'DELETE FROM {0} WHERE data>="{1}" AND data<="{2}";'.format(table_name, data_acomph_min, data_acomph_max)
            connection.execute(sql_command)    

            df.round(2).to_sql(table_name, con=engine_str, schema=database, if_exists='append', index=False)
            print('Inserção OK!')
            return True
        except:
            print('Inserção ERRO!')
            return False

'''
rdh_to_mysql(file,connection) : Recebe um arquivo rdh excel, o transforma em dataframe com os dados 
formatados e joga no banco de dados, através da conexão 'connection'

Dados capturados (por posto):
- Código do posto
- Data considerada
- Nível dos reservatórios em metros
- Volume útil armazenado (%)

Parâmetros:
- file : caminho do arquivo RDH, com nome e extensão
- connection : conexão com o banco de dados para realizar operações de inserção
'''
def rdh_to_mysql(file):
    # Dados do banco de dados
    table_name = 'rdh'
    engine_str = CONN_MYSQL
    database = config.DATABASE

    data = xf.Excel_RDH(file)
    df = pd.DataFrame(columns=['cod_posto', 'data', 'nivel', 'volume_perc'])

    df = data.get_rdh_dataframe()

    with CONN_MYSQL.connect() as connection:
        try:
            data_rdh = str(df.data[0].strftime('%Y-%m-%d'))
            # Verifica se a data deste RDH já existe no BD
            check = connection.execute('SELECT EXISTS(SELECT * FROM {0} WHERE data="{1}");'.format(table_name, data_rdh)).first()[0]
            # Se existe, os remove do BD para colocar dados atuais
            if check == 1:
                sql_command = 'DELETE FROM {0} WHERE data="{1}";'.format(table_name, data_rdh)
                connection.execute(sql_command)
    
            df.to_sql(table_name, con=engine_str, schema=database, if_exists='append', index=False)
            print('Inserção OK!')
            return True
        except:
            print('Inserção ERRO!')
            return False



def precipitacao_to_mysql():
    table_name = 'precipitacao'
    engine_str = CONN_MYSQL
    database = config.DATABASE

    # Leitura do banco de dados dos polígonos de cada bacia
    #bacias_poligono = pd.read_sql('SELECT id_bacia,nome, ST_AsText(poligono) AS poligono FROM poligono', engine_str, index_col='id_bacia')
    bacias_indices = pd.read_sql('SELECT id_bacia, indice, modelo FROM indice_poligono', engine_str, index_col='id_bacia')

    # Criação de dataframe global que será inserido no banco de dados
    df_precip = pd.DataFrame()

    #================================================
    #================== MODELO GEFS =================
    #================================================
    print()

    # Dataframe de referência (deve ser igual aos de hoje, para não haver diferenças de pontos)
    ref = pd.read_csv(config.INPUT_PATH_PRECIPITACAO_GEFS + 'referencia_gefs.csv', names=['long', 'lat', 'precipitacao'], sep="\s+").iloc[:,:-1]

    for filename in os.listdir(config.INPUT_PATH_PRECIPITACAO_GEFS):
        if filename.endswith('.dat'):
            # Armazena data do arquivo, data do estudo e a tabela de precipitação
            precip = xf.Precipitacao(config.INPUT_PATH_PRECIPITACAO_GEFS + filename)
            data_arquivo = precip.get_data_arquivo('gefs')
            data_precip = precip.get_data_precipitacao('gefs')
            df = precip.get_dataframe()

            if not ref.equals(df.iloc[:,:-1]):
                raise Exception('Erro! Arquivo de precipitação diferente do padrão!')

            print('GEFS - {}: '.format(date.strftime(data_precip, format='%d/%m/%Y')), end='')

            # Inicialização do dataframe local (para cada arquivo da pasta)
            file_dataframe = pd.DataFrame({'id_bacia': bacias_indices.index.unique()})
            file_dataframe['data_arquivo'] = data_arquivo
            file_dataframe['data_precip'] = data_precip

            try:
                # Percorre cada bacia e verifica a precipitação presente dentro dos limites
                for id_bacia in bacias_indices.index.unique():
                    '''poligono = wkt.loads(bacias_poligono[bacias_poligono.index == id_bacia].poligono.values[0])
                    quadrante = df[
                        (df.long > poligono.bounds[0]) & 
                        (df.long < poligono.bounds[2]) & 
                        (df.lat > poligono.bounds[1]) & 
                        (df.lat < poligono.bounds[3])
                    ]
                    file_dataframe.loc[file_dataframe.id_bacia == id_bacia, 'gefs'] = quadrante[quadrante.apply(lambda x: poligono.contains(Point(x.long,x.lat)),axis=1)].precipitacao.mean()'''

                    dentro_contorno = df.loc[bacias_indices[(bacias_indices.index == id_bacia) & (bacias_indices.modelo == 'gefs')].indice.values,:]
                    file_dataframe.loc[file_dataframe.id_bacia == id_bacia, 'gefs'] = dentro_contorno.precipitacao.mean()

                # Adiciona dataframe local ao global
                df_precip = df_precip.append(file_dataframe)
                df_precip['gefs'] = df_precip['gefs'].round(2)
                print('OK!')
            except:
                print('ERRO!')
    

    #================================================
    #================== MODELO ETA ==================
    #================================================
    print()

    ref = pd.read_csv(config.INPUT_PATH_PRECIPITACAO_ETA + 'referencia_eta.csv', names=['long', 'lat', 'precipitacao'], sep="\s+").iloc[:,:-1]

    for filename in os.listdir(config.INPUT_PATH_PRECIPITACAO_ETA):
        if filename.endswith('.dat'):
            # Armazena data do arquivo, data do estudo e a tabela de precipitação
            precip = xf.Precipitacao(config.INPUT_PATH_PRECIPITACAO_ETA + filename)
            data_arquivo = precip.get_data_arquivo('eta')
            data_precip = precip.get_data_precipitacao('eta')
            df = precip.get_dataframe()

            if not ref.equals(df.iloc[:,:-1]):
                raise Exception('Erro! Arquivo de precipitação diferente do padrão!')

            print('ETA - {}: '.format(date.strftime(data_precip, format='%d/%m/%Y')), end='')

            try:
                # Percorre cada bacia e verifica a precipitação presente dentro dos limites
                for id_bacia in bacias_indices.index.unique():
                    '''poligono = wkt.loads(bacias_indices[bacias_poligono.index == id_bacia].poligono.values[0])
                    quadrante = df[
                        (df.long > poligono.bounds[0]) & 
                        (df.long < poligono.bounds[2]) & 
                        (df.lat > poligono.bounds[1]) & 
                        (df.lat < poligono.bounds[3])
                    ]
                    '''

                    dentro_contorno = df.loc[bacias_indices[(bacias_indices.index == id_bacia) & (bacias_indices.modelo == 'eta')].indice.values,:]
                    # Busca no dataframe global a linha do id_bacia e a data_precip para adicionar à coluna 'eta'
                    df_precip.loc[(df_precip.id_bacia == id_bacia) & (df_precip.data_precip == data_precip), 'eta'] = dentro_contorno.precipitacao.mean()

                df_precip['eta'] = df_precip['eta'].round(2)
                
                print('OK!')
            except:
                print('ERRO!')

    #================================================
    #================= MODELO ECMWF =================
    #================================================
    print()

    ref = pd.read_csv(config.INPUT_PATH_PRECIPITACAO_ECMWF + 'referencia_ecmwf.csv', names=['long', 'lat', 'precipitacao'], sep="\s+").iloc[:,:-1]

    for filename in os.listdir(config.INPUT_PATH_PRECIPITACAO_ECMWF):
        if filename.endswith('.dat'):
            # Armazena data do arquivo, data do estudo e a tabela de precipitação
            precip = xf.Precipitacao(config.INPUT_PATH_PRECIPITACAO_ECMWF + filename)
            data_arquivo = precip.get_data_arquivo('ecmwf')
            data_precip = precip.get_data_precipitacao('ecmwf')
            df = precip.get_dataframe()

            if not ref.equals(df.iloc[:,:-1]):
                raise Exception('Erro! Arquivo de precipitação diferente do padrão!')
                
            print('ECMWF - {}: '.format(date.strftime(data_precip, format='%d/%m/%Y')), end='')

            try:
                # Percorre cada bacia e verifica a precipitação presente dentro dos limites
                for id_bacia in bacias_indices.index.unique():
                    '''poligono = wkt.loads(bacias_indices[bacias_indices.index == id_bacia].poligono.values[0])
                    quadrante = df[
                        (df.long > poligono.bounds[0]) & 
                        (df.long < poligono.bounds[2]) & 
                        (df.lat > poligono.bounds[1]) & 
                        (df.lat < poligono.bounds[3])
                    ]'''

                    dentro_contorno = df.loc[bacias_indices[(bacias_indices.index == id_bacia) & (bacias_indices.modelo == 'ecmwf')].indice.values,:]
                    # Busca no dataframe global a linha do id_bacia e a data_precip para adicionar à coluna 'eta'
                    df_precip.loc[(df_precip.id_bacia == id_bacia) & (df_precip.data_precip == data_precip), 'ecmwf'] = dentro_contorno.precipitacao.mean()

                df_precip['ecmwf'] = df_precip['ecmwf'].round(2)
                
                print('OK!')
            except:
                print('ERRO!')


    with CONN_MYSQL.connect() as connection:
        # Remover dados de 2 dias atrás
        sql_command = 'DELETE FROM {0} WHERE data_arquivo <= "{1}";'.format(table_name, date.strftime(data_arquivo - timedelta(2), format='%Y-%m-%d'))
        connection.execute(sql_command)

        try:
            sql_command = 'SELECT EXISTS(SELECT * FROM {0} WHERE data_arquivo="{1}");'.format(table_name, date.strftime(data_arquivo,format='%Y-%m-%d'))
            check = connection.execute(sql_command).first()[0]
            if check == 1:
                sql_command = 'DELETE FROM {0} WHERE data_arquivo="{1}";'.format(table_name, date.strftime(data_arquivo,format='%Y-%m-%d'))
                connection.execute(sql_command)
            df_precip.to_sql(table_name, con=engine_str, schema=database, if_exists='append', index=False)
            print('Inserção OK!')
        except:
            print('Inserção ERRO!')