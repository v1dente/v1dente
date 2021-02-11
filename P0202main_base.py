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

from sqlalchemy import create_engine
from urllib.request import urlretrieve
from zipfile import ZipFile

import P0201funcoes_tabelas    # Import created functions for database management
import P0201funcoes_db         # Import created functions for database management
import P0202funcoes_tabelas    # Import created functions for database management

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
#-------------------------------# AUXILIARES  #-------------------------------#

UF = ('AC','AL','AM','AP','BA','CE','DF','ES','GO','MA','MG','MS','MT','PA','PB', \
      'PE','PI','PR','RJ','RN','RO','RR','SC','SE','SP','TO','XX')

#-----------------------------------------------------------------------------#
#--------------------------------# CONEXÕES #---------------------------------#

# Auxiliar de tempo
start = time.time()

########## PARA FAZER AS CONEXÕES COM AS PLANILHAS
###### Índice 1 -> base completa crua
###### Índice 2 -> base filtrada
conn1 = sqlite3.connect("P0201_basecompleta.db")
cur1 = conn1.cursor()
conn2 = sqlite3.connect("P0202_basefiltrada.db")
cur2 = conn2.cursor()

########## Cria a tabela agrupada
P0202funcoes_tabelas.createDBTable(cur2)

#-----------------------------------------------------------------------------#
#--------------------------# Empresas selecionadas #--------------------------#

### Dicionário com o nome das empresas  
excelFileName = 'Auxiliar_nomes.csv'
fhand = open(excelFileName)
fhand.readline()    # skip first row
dict_cd_empresas = {}
lista = list()
p = 0
for row in fhand:
    rs = row.split(';')
    dict_cd_empresas[rs[0]] = rs[1]  ## cria dicionário com empresas filtrada tais que chave: cd_operadora para nome da empresa

### Dicionário com os id das empresas  
# conn1 = sqlite3.connect("P0201_basecompleta.db")
# cur1 = conn1.cursor()
# conn2 = sqlite3.connect("P0202_basefiltrada.db")
# cur2 = conn2.cursor()

########## Cria a tabela agrupada

dict_id_cd_empresas = {}

aux = cur1.execute('''SELECT * FROM Empresas ''',)
tableColumns = [description[0] for description in cur1.description]
df = pd.DataFrame(aux.fetchall(),  columns=tableColumns)
for i, row in df.iterrows():
    linha = row.tolist()
    if str(linha[1]) in dict_cd_empresas: ## se o cd_operadora está no dicionário filtrado
        dict_id_cd_empresas[linha[0]] = linha[1]  ## cria dicionário com empresas filtrada tais que chave: id_cd_operadora para cd_operadora
        fhand = open(excelFileName)
        fhand.readline()    # skip first row
        for row2 in fhand:
            rs = row2.split(';')
            if linha[1] == int(rs[0]):
                aux2 = [linha[0],linha[1],linha[2],linha[3], rs[2], rs[3]]
                P0202funcoes_tabelas.insertEmpresa_filtrado(cur2, aux2)


conn2.commit()
conn1.close()
conn2.close()

#-----------------------------------------------------------------------------#
#------------------------------# MAIN CODE #----------------------------------#

# Auxiliar de tempo
start = time.time()

########## PARA FAZER AS CONEXÕES COM AS PLANILHAS
###### Índice 1 -> base completa crua
###### Índice 2 -> base filtrada
conn1 = sqlite3.connect("P0201_basecompleta.db")
cur1 = conn1.cursor()
conn2 = sqlite3.connect("P0202_basefiltrada.db")
cur2 = conn2.cursor()


aux = cur1.execute('''SELECT  Dados.id_ID_CMPT_MOVEL,
                		Dados.id_CD_OPERADORA,
                		Dados.id_MODALIDADE_OPERADORA,
                		Dados.id_CD_MUNICIPIO,
                		Dados.id_DE_CONTRATACAO_PLANO,
                		Dados.id_DE_SEGMENTACAO_PLANO,
                		Dados.id_COBERTURA_ASSIST_PLAN,
                		sum(Dados.QT_BENEFICIARIO_ATIVO) as BenefAtivo, 
                		sum(Dados.QT_BENEFICIARIO_ADERIDO) as BenefAderido,
                		sum(dados.QT_BENEFICIARIO_CANCELADO) as BeneCancel
                from Dados
                group by Dados.id_ID_CMPT_MOVEL,
             			Dados.id_CD_OPERADORA,
             			Dados.id_MODALIDADE_OPERADORA,
             			Dados.id_CD_MUNICIPIO,
             			Dados.id_DE_CONTRATACAO_PLANO,
             			Dados.id_DE_SEGMENTACAO_PLANO,
                        Dados.id_COBERTURA_ASSIST_PLAN   
                ''',)

# aux = cur1.execute('''SELECT  Dados.id_ID_CMPT_MOVEL,
#                 		Dados.id_CD_OPERADORA,
#                 		Dados.id_MODALIDADE_OPERADORA,
#                 		Dados.id_CD_MUNICIPIO,
#                 		Dados.id_DE_CONTRATACAO_PLANO,
#                 		Dados.id_DE_SEGMENTACAO_PLANO,
#                 		Dados.id_COBERTURA_ASSIST_PLAN,
#                 		sum(Dados.QT_BENEFICIARIO_ATIVO) as BenefAtivo, 
#                 		sum(Dados.QT_BENEFICIARIO_ADERIDO) as BenefAderido,
#                 		sum(dados.QT_BENEFICIARIO_CANCELADO) as BeneCancel
#                 from Dados, Data_mes, Empresas, Localidade
# 				group by Data_mes.ID_CMPT_MOVEL,
#              			Empresas.CD_OPERADORA,
#              			Dados.id_MODALIDADE_OPERADORA,
#              			Localidade.SG_UF,
#              			Dados.id_DE_CONTRATACAO_PLANO,
#              			Dados.id_DE_SEGMENTACAO_PLANO
#                 ''',)



# order by Dados.id_CD_OPERADORA
print('executado')

# m = 0
# while m<60:
#     tableColumns = [description[0] for description in cur1.description]
#     df = pd.DataFrame(aux.fetchmany(5000),  columns=tableColumns)
#     for i, row in df.iterrows():
#     # Filtrar se o nome está no código row.tolist[1] que é um int
#         if row.tolist()[1] in dict_id_cd_empresas:
#             P0202funcoes_tabelas.insertDados(cur2, row.tolist())
#     print(m)
#     m = m+1

tableColumns = [description[0] for description in cur1.description]
df = pd.DataFrame(aux.fetchall(),  columns=tableColumns)


end1 = time.time()
tempo_parcial_1 = end1 - start
print("Tempo pra baixar: ", tempo_parcial_1)

for i, row in df.iterrows():
    # Filtrar se o nome está no código row.tolist[1] que é um int
    if row.tolist()[1] in dict_id_cd_empresas:
        P0202funcoes_tabelas.insertDados(cur2, row.tolist())

conn2.commit()
end2 = time.time()
tempo_total = end2 - start
print("Total de execução: ", tempo_total)


########## CARREGAR MAIS TABELAS (as com os ID)

aux = cur1.execute('''SELECT * FROM Cobertura_plano ''',)
tableColumns = [description[0] for description in cur1.description]
df = pd.DataFrame(aux.fetchall(),  columns=tableColumns)
for i, row in df.iterrows():
      P0202funcoes_tabelas.insertCobertura_plano(cur2, row.tolist())
      
aux = cur1.execute('''SELECT * FROM Cont_plano ''',)
tableColumns = [description[0] for description in cur1.description]
df = pd.DataFrame(aux.fetchall(),  columns=tableColumns)
for i, row in df.iterrows():
      P0202funcoes_tabelas.insertCont_plano(cur2, row.tolist())

aux = cur1.execute('''SELECT * FROM Data_mes ''',)
tableColumns = [description[0] for description in cur1.description]
df = pd.DataFrame(aux.fetchall(),  columns=tableColumns)
for i, row in df.iterrows():
      P0202funcoes_tabelas.insertData_mes(cur2, row.tolist())

aux = cur1.execute('''SELECT * FROM Empresas ''',)
tableColumns = [description[0] for description in cur1.description]
df = pd.DataFrame(aux.fetchall(),  columns=tableColumns)
for i, row in df.iterrows():
      P0202funcoes_tabelas.insertEmpresa(cur2, row.tolist())

aux = cur1.execute('''SELECT * FROM Faixa_etaria ''',)
tableColumns = [description[0] for description in cur1.description]
df = pd.DataFrame(aux.fetchall(),  columns=tableColumns)
for i, row in df.iterrows():
      P0202funcoes_tabelas.insertFaixa_etaria(cur2, row.tolist())

aux = cur1.execute('''SELECT * FROM Localidade ''',)
tableColumns = [description[0] for description in cur1.description]
df = pd.DataFrame(aux.fetchall(),  columns=tableColumns)
for i, row in df.iterrows():
      P0202funcoes_tabelas.insertLocalidade(cur2, row.tolist())

aux = cur1.execute('''SELECT * FROM Modalidade_plano ''',)
tableColumns = [description[0] for description in cur1.description]
df = pd.DataFrame(aux.fetchall(),  columns=tableColumns)
for i, row in df.iterrows():
      P0202funcoes_tabelas.insertModalidade_plano(cur2, row.tolist())
    
aux = cur1.execute('''SELECT * FROM Real_faixa ''',)
tableColumns = [description[0] for description in cur1.description]
df = pd.DataFrame(aux.fetchall(),  columns=tableColumns)
for i, row in df.iterrows():
      P0202funcoes_tabelas.insertReal_faixa(cur2, row.tolist())

aux = cur1.execute('''SELECT * FROM Vinculo_plano ''',)
tableColumns = [description[0] for description in cur1.description]
df = pd.DataFrame(aux.fetchall(),  columns=tableColumns)
for i, row in df.iterrows():
      P0202funcoes_tabelas.insertVinculo_plano(cur2, row.tolist())

aux = cur1.execute('''SELECT * FROM Seg_plano ''',)
tableColumns = [description[0] for description in cur1.description]
df = pd.DataFrame(aux.fetchall(),  columns=tableColumns)
for i, row in df.iterrows():  
    P0202funcoes_tabelas.insertSeg_plano(cur2, row.tolist())
    
# Criando a tabela com as info atualizadas de empresa

conn2.commit()
conn1.close()
conn2.close()


# SELECT NM_RAZAO_SOCIAL, MODALIDADE_OPERADORA, COBERTURA_ASSIST_PLAN, DE_CONTRATACAO_PLANO, DE_SEGMENTACAO_PLANO,
# 		sum(qt_beneficiario_ativo) as BenefAtivo, sum(qt_beneficiario_aderido) as BenefAderido, sum(qt_beneficiario_cancelado) as BeneCancel

# from ben201405_AC_copy
# group by CD_OPERADORA, MODALIDADE_OPERADORA, COBERTURA_ASSIST_PLAN, DE_CONTRATACAO_PLANO, DE_SEGMENTACAO_PLANO
# order by NM_RAZAO_SOCIAL