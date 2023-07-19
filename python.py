# Importar as bibliotecas necessárias
import pymongo
import pandas as pd
import json

# Função para conectar ao MongoDB


# Função para conectar ao MongoDB
def connect_to_mongodb():
    # cluster = pymongo.MongoClient('mongodb+srv://hallanmiranda23:Hallan-10@cluster0.nh0u06w.mongodb.net//test')
    cluster = pymongo.MongoClient(
        'mongodb+srv://hallanmiranda23:Hallan@cluster3.fgy2dag.mongodb.net/dbdatabase')
    db = cluster.get_database('test')
    return db


# Função para inserir dados no MongoDB


def insert_data_to_collections(db, col_cars, col_assembler):
    carros_collection = db.get_collection('Carros')
    montadoras_collection = db.get_collection('Montadora')
    carros_collection.insert_many(col_cars)
    montadoras_collection.insert_many(col_assembler)

# Função para realizar a agregação e atualização das coleções


def perform_aggregation_and_update():
    # Conectar ao MongoDB
    db = connect_to_mongodb()

    # Inserir dados no MongoDB
    col_cars = [
        {'Carro': 'Onix', 'Cor': 'Prata', 'Montadora': 'Chevrolet'},
        {'Carro': 'Polo', 'Cor': 'Branco', 'Montadora': 'Volkswagen'},
        {'Carro': 'Sandero', 'Cor': 'Prata', 'Montadora': 'Renault'},
        {'Carro': 'Fiesta', 'Cor': 'Vermelho', 'Montadora': 'Ford'},
        {'Carro': 'City', 'Cor': 'Preto', 'Montadora': 'Honda'}
    ]
    col_assembler = [
        {'Montadora': 'Chevrolet', 'Pais': 'EUA'},
        {'Montadora': 'Volkswagen', 'Pais': 'Alemanha'},
        {'Montadora': 'Renault', 'Pais': 'França'},
        {'Montadora': 'Ford', 'Pais': 'EUA'},
        {'Montadora': 'Honda', 'Pais': 'Japão'}
    ]
    insert_data_to_collections(db, col_cars, col_assembler)

    # Definir a chave de correspondência
    chave_correspondencia = 'Montadora'

    # Criar o pipeline de agregação
    pipeline = [
        {
            '$lookup': {
                'from': 'Montadora',
                'localField': chave_correspondencia,
                'foreignField': chave_correspondencia,
                'as': 'Montadora_info'
            }
        },
        {
            '$unwind': {
                'path': '$Montadora_info',
                'preserveNullAndEmptyArrays': True
            }
        },
        {
            '$addFields': {
                '_Montadora': ''
            }
        },
        {
            '$project': {
                'Carro': 1,
                'Cor': 1,
                'Montadora': 1,
                '_Montadora': 1,
                'Pais': '$Montadora_info.Pais'
            }
        }
    ]

# Executar a agregação e criar a nova coleção "Carros_Agregado"
carros_collection = db.get_collection('Carros')
resultado_agregacao = list(carros_collection.aggregate(pipeline))
db.create_collection('Carros_Agregado')
carros_agregado_collection = db.get_collection('Carros_Agregado')
carros_agregado_collection.insert_many(resultado_agregacao)

# Renomear a coleção original para backup
carros_collection.rename('Carros_BKP')

# Renomear a nova coleção para substituir a coleção original
carros_agregado_collection.rename('Carros')

# Excluir a coleção backup
db.drop_collection('Carros_BKP')

# Define o nome do arquivo
carros_json = 'resultado_agregacao.js'

# Escreve o resultado da agregação em um arquivo
with open(carros_json, 'w') as file:
    file.write(res_agreg_json)

# Sucesso da operação
print("ETL executado com sucesso! Coleções atualizadas.")
