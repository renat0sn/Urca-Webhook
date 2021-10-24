from flask import Flask, jsonify, request
from urllib.request import urlretrieve
from openpyxl import load_workbook
import zipfile
import datetime

import database
import config

import os

from werkzeug.exceptions import SecurityError

application = app = Flask(__name__)

def check_auth(password):
    if not password == 'hPD!DhJuyEIF':
        raise SecurityError

def extract_zip_precip(file_name, modelo):
    path = config.INPUT_PATH_PRECIPITACAO + modelo + '/'

    # Extração do zip GEFS
    files_in_directory = [file for file in os.listdir(path) if file.endswith('.dat')]
    for file in files_in_directory:
        os.remove(path + file)
    with zipfile.ZipFile(file_name, 'r') as zf:
        for filename in zf.namelist():
            if filename.split('_')[1][0] == 'p':
                zf.extract(filename, path)

def get_request(request):
    print(os.getcwd())
    print('--------------------')
    # Autenticação
    check_auth(request.headers['Authorization'])

    # Dados de url e nome do arquivo extraídos
    url = request.json['url']
    file_name = os.getcwd() + '/' + request.json['nome']

    print(request.json['nome'] + ' publicado!')
    print(request.json['dataProduto'] + '  ' + datetime.datetime.now().strftime('%H:%M:%S'))

    # Jogar arquivo no banco de dados
    name = request.path[1:]

    if name == 'ipdo':
        file_name += '.xlsm'
        urlretrieve(url, file_name)
        database.ipdo_to_mysql(file_name)

    elif name == 'acomph':
        file_name += '.xls'
        urlretrieve(url, file_name)
        database.acomph_to_mysql(file_name)

    elif name == 'rdh':
        file_name += '.xlsx'
        urlretrieve(url, file_name)
        database.rdh_to_mysql(file_name)

    elif name == 'precipitacao':
        file_name = config.INPUT_PATH_PRECIPITACAO + request.json['nome'] + ' {}.zip'.format(request.json['dataProduto'].replace('/','-'))
        urlretrieve(url, file_name)
        
        files_zip = [file for file in os.listdir(config.INPUT_PATH_PRECIPITACAO) if file.endswith('.zip')]
        modelos = [file.split(' ')[1] for file in files_zip]
        datas = [file.split(' ')[2][:-4] for file in files_zip]
        hoje = datetime.datetime.now().strftime('%d-%m-%Y')

        # Remove arquivos de dias diferentes
        for file in files_zip:
            if file.split(' ')[2][:-4] != hoje:
                os.remove(config.INPUT_PATH_PRECIPITACAO + file)
        
        files_zip = [file for file in os.listdir(config.INPUT_PATH_PRECIPITACAO) if file.endswith('.zip')]
        modelos = [file.split(' ')[1] for file in files_zip]
        datas = [file.split(' ')[2][:-4] for file in files_zip]

        # Verifica se os 3 modelos foram publicados para rodar o script
        if (len(files_zip) == 3) & (set(modelos) == set(['ECMWF', 'ETA', 'GEFS'])) & (set(datas) == set([hoje])):
            for filename in files_zip:
                extract_zip_precip(config.INPUT_PATH_PRECIPITACAO + filename, filename.split(' ')[1])
            database.precipitacao_to_mysql()

    print('--------------------')

@app.route("/ipdo", methods=['POST'])
@app.route("/acomph", methods=['POST'])
@app.route("/rdh", methods=['POST'])
@app.route("/precipitacao", methods=['POST'])
def index():
    get_request(request)
    return ''

if __name__ == '__main__':
    app.run(debug=True)