# -*- coding: utf-8 -*-
"""
@author: Luiz Gustavo de Oliveira

ITAÚ Asset management
"""

import numpy as np
import pandas as pd
import scipy
import cmath #para número complexos
import csv
import matplotlib.pyplot as plt
import requests
import sqlite3
from sqlite3 import Error
import time
import os

from urllib.request import urlretrieve
from zipfile import ZipFile

import P0201funcoes_tabelas    # Import created functions for database management
import P0201funcoes_db         # Import created functions for database management

#-----------------------------------------------------------------------------#
#---------------------------------# RESUMO  #---------------------------------#

### P02 - base de dados da ANS

## Nível 01 - Busca os dados da ANS de forma automática, cria tabelas com os IDs, seleciona algumas colunas
#             e, em seguida, cria uma tabela com todos os dados de várias empresas de interesse

## Nível 02 - Dos dados da tabelona do nível 1, faz-se a união das quantidades de beneficiários
#             para colunas iguais criando uma nova base de dados

## Nível 03 - Dos dados da tabelona do nível 2, faz-se a união das quantidades de interesse para
#             para aplicação futura para o analista

#-----------------------------------------------------------------------------#
#------------------------------# FUNÇÕES AQUI  #------------------------------#

def baixa_arquivo(url, endereco):
    # faz a requisição ao servidor
    resposta = requests.get(url)
    if resposta.status_code == requests.codes.OK:
        with open(endereco, 'wb') as novo_arquivo:
            novo_arquivo.write(resposta.content)
        print("Dowload finalizado. Salvo em: {}".format(endereco))
    else:
        resposta.raise_for_status()

UF = ('AC','AL','AM','AP','BA','CE','DF','ES','GO','MA','MG','MS','MT','PA','PB', \
      'PE','PI','PR','RJ','RN','RO','RR','SC','SE','SP','TO','XX')

#-----------------------------------------------------------------------------#
#--------------------------# Empresas selecionadas #--------------------------#
    
excelFileName = 'Auxiliar_nomes.csv'
fhand = open(excelFileName)
fhand.readline()    # skip first row
dict_cd_empresas = {}
lista = list()
p = 0
for row in fhand:
    rs = row.split(';')
    dict_cd_empresas[rs[0]] = rs[1]

#-----------------------------------------------------------------------------#
#----------------------------------# MAIN  #----------------------------------#

# Auxiliar de tempo
start = time.time()

### Abre a conexao
conn = sqlite3.connect("P0201_basecompleta.db")
# conn = sqlite3.connect("base_de_teste.db")
cur = conn.cursor()
P0201funcoes_tabelas.createDBTable(cur)

# Aqui faremos o dowload de cada arquivo
from datetime import datetime
hoje = datetime.now()
ano_final = hoje.year
if hoje.month < 3:
    ano_final = hoje.year - 1
    mes_final = 10 + hoje.month

base_url = "http://ftp.dadosabertos.ans.gov.br/FTP/PDA/informacoes_consolidadas_de_beneficiarios/"

ano = 2020  #ALTERAR AQUI
while ano < ano_final+1:
    mes = 3  #ALTERAR AQUI
    while mes < 13:
        if mes < 10:
            arq = "ben" + str(ano) + str(0) + str(mes)
            site_url = base_url + str(ano) + str(0) + str(mes) + "/" + arq
        else:
            arq = "ben" + str(ano) + str(mes)
            site_url = base_url + str(ano) + str(mes) + "/" + arq
        #Para cada dowload
       
        p = 0
        while p<27:
            nome = UF[p] + ".zip"
            url_final = site_url + "_{}".format(nome)
            baixa_arquivo(url_final, nome)
            with ZipFile(nome, "r") as extra:
                extra.extractall()
            #aqui tem que add na base
            if mes < 10:
                excelFile = "ben" + str(ano) + str(0) + str(mes) + "_" + UF[p] + ".csv"
            else:
                excelFile = "ben" + str(ano) + str(mes) + "_" + UF[p] + ".csv"
            end1 = time.time()
            P0201funcoes_db.incluir_dados_planilha(cur, conn, excelFile, dict_cd_empresas)
            end2 = time.time()
            print("DADOS de ",UF[p],ano,mes,)
            tempo_total = end2 - start
            tempo_parcial = end2 - end1
            print("Total de execução: ", tempo_total)
            print("Tempo pra baixar: ", tempo_parcial)
            arq_csv = arq + "_" + UF[p] + ".csv"
            os.remove(arq_csv)
            os.remove(nome)
            p=p+1
       
        # print(url_final)
        print("dowload feito com sucesso")
        # mes = mes + 1
        mes =  mes + 3 #ALTERAR AQUI
        if ano == ano_final and mes > mes_final:
            mes = mes + 12
    ano = ano + 1


conn.commit()
conn.close()
    
# Tem várias colunas que não servem, substitui ou não ou joga fora????
    

# ano = 2020  #ALTERAR AQUI
# while ano < ano_final+1:
#     mes = 11  #ALTERAR AQUI
#     while mes < 13:
#         if mes < 10:
#             arq = "ben" + str(ano) + str(0) + str(mes)
#             site_url = base_url + str(ano) + str(0) + str(mes) + "/" + arq
#         else:
#             arq = "ben" + str(ano) + str(mes)
#             site_url = base_url + str(ano) + str(mes) + "/" + arq
#         #Para cada dowload
       
       
#         # for estado in UF:         ################ ver aqui depois UF[p] == estado
#         for estado in UF:
#             nome = estado + ".zip"
#             url_final = site_url + "_{}".format(nome)
#             baixa_arquivo(url_final, nome)
#             with ZipFile(nome, "r") as extra:
#                 extra.extractall()
#             #aqui tem que add na base
#             if mes < 10:
#                 excelFile = "ben" + str(ano) + str(0) + str(mes) + "_" + estado + ".csv"
#             else:
#                 excelFile = "ben" + str(ano) + str(mes) + "_" + estado + ".csv"
#             end1 = time.time()
#             funcoes_db.incluir_dados_planilha(cur, conn, excelFile)
#             end2 = time.time()
#             print("DADOS de ",estado,ano,mes,)
#             tempo_total = end2 - start
#             tempo_parcial = end2 - end1
#             print("Total de execução: ", tempo_total)
#             print("Tempo pra baixar: ", tempo_parcial)
#             arq_csv = arq + "_" + estado + ".csv"
#             os.remove(arq_csv)
#             os.remove(nome)
       
#         # print(url_final)
#         print("dowload feito com sucesso")
#         mes = mes + 1
#         if ano == ano_final and mes > mes_final:
#             mes = mes + 12
#     ano = ano + 1


# conn.commit()
# conn.close()


