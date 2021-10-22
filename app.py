from flask import Flask, jsonify, request
from urllib.request import urlretrieve
from openpyxl import load_workbook

import database

import os

from werkzeug.exceptions import SecurityError

app = Flask(__name__)

def check_auth(password):
    if not password == 'hPD!DhJuyEIF':
        raise SecurityError

@app.route("/ipdo", methods=['POST'])
def index():
    '''print('--------------------')
    # Autenticação
    check_auth(request.headers['Authorization'])

    # Dados de url e nome do arquivo extraídos
    url_ipdo = request.json['url']
    file_name = os.getcwd() + '\\' + request.json['nome'] + '.xlsm'

    print(request.json['nome'] + ' publicado!')
    print('Dia ' + request.json['dataProduto'])

    # Obter arquivo da url e transferir para diretório atual
    urlretrieve(url_ipdo, file_name)

    # Jogar arquivo no banco de dados
    database.ipdo_to_mysql(file_name)

    os.remove(file_name)
    print('--------------------')'''
    return 'OK'

if __name__ == '__main__':
    app.run(debug=True)