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
import P0203funcoes_tabelas    # Import created functions for database management

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
###### Índice 2 -> base filtrada nível 2
###### Índice 3 -> base de dados de interesse nível 3
conn2 = sqlite3.connect("P0202_basefiltrada.db")
cur2 = conn2.cursor()
conn3 = sqlite3.connect("P0203_baseinteresse.db")
cur3 = conn3.cursor()

####### Cria a tabela:
### Data / Nome / Qt beneficiários ativos
# P0203funcoes_tabelas.criartabelaA01(cur3)
# P0203funcoes_tabelas.preenchertabelaA01(cur2, cur3)

# P0203funcoes_tabelas.criartabelaA02(cur3)
# P0203funcoes_tabelas.preenchertabelaA02(cur2, cur3)

# P0203funcoes_tabelas.criartabelaA03(cur3)
# P0203funcoes_tabelas.preenchertabelaA03(cur2, cur3)

# P0203funcoes_tabelas.criartabelaA04(cur3)
# P0203funcoes_tabelas.preenchertabelaA04(cur2, cur3)

# P0203funcoes_tabelas.criartabelaA05(cur3)
# P0203funcoes_tabelas.preenchertabelaA05(cur2, cur3)

# ###
# P0203funcoes_tabelas.criartabelaB01(cur3)
# P0203funcoes_tabelas.preenchertabelaB01(cur2, cur3)

# P0203funcoes_tabelas.criartabelaB02(cur3)
# P0203funcoes_tabelas.preenchertabelaB02(cur2, cur3)

# P0203funcoes_tabelas.criartabelaB03(cur3)
# P0203funcoes_tabelas.preenchertabelaB03(cur2, cur3)

P0203funcoes_tabelas.criartabelaB04(cur3)
P0203funcoes_tabelas.preenchertabelaB04(cur2, cur3)
  
# P0203funcoes_tabelas.criartabelaB05(cur3)
# P0203funcoes_tabelas.preenchertabelaB05(cur2, cur3)

end2 = time.time()
tempo_total = end2 - start
print("Total de execução: ", tempo_total)
print('todas tabelas de interesse estão finalizadas')

conn3.commit()
conn3.close()
conn2.close()


# SELECT Data_mes.ID_CMPT_MOVEL as Data, Empresas.CD_OPERADORA, Empresas.NM_RAZAO_SOCIAL as Empresa, Localidade.SG_UF as UF, sum(Dados.QT_BENEFICIARIO_ATIVO) as BenefAtivo, sum(Dados.QT_BENEFICIARIO_ADERIDO) as BenefAderido, sum(Dados.QT_BENEFICIARIO_CANCELADO) as BenefCancelado, sum(Dados.QT_BENEFICIARIO_ADERIDO - Dados.QT_BENEFICIARIO_CANCELADO) as BenfNet
# FROM Empresas, Data_mes, Localidade JOIN Dados ON Dados.id_CD_OPERADORA = Empresas.ID AND Dados.id_ID_CMPT_MOVEL = Data_mes.ID AND Dados.id_CD_MUNICIPIO = Localidade.ID
# WHERE Data_mes.ID_CMPT_MOVEL = '202011' AND Empresas.CD_OPERADORA = '368253'
# group by  Dados.id_ID_CMPT_MOVEL, Dados.id_CD_OPERADORA, Localidade.SG_UF
# order by  Data_mes.ID_CMPT_MOVEL DESC, Empresas.NM_RAZAO_SOCIAL, Localidade.SG_UF, sum(Dados.QT_BENEFICIARIO_ATIVO) DESC, Empresas.NM_RAZAO_SOCIAL

# SELECT Data_mes.ID_CMPT_MOVEL as Data, Empresas.CD_OPERADORA, Empresas.NM_RAZAO_SOCIAL as Empresa, Cobertura_plano.COBERTURA_ASSIST_PLAN as Cobertura, Localidade.SG_UF, sum(Dados.QT_BENEFICIARIO_ATIVO) as BenefAtivo, sum(Dados.QT_BENEFICIARIO_ADERIDO) as BenefAderido, sum(Dados.QT_BENEFICIARIO_CANCELADO) as BenefCancelado, sum(Dados.QT_BENEFICIARIO_ADERIDO - Dados.QT_BENEFICIARIO_CANCELADO) as BenfNet,  sum(Dados.QT_BENEFICIARIO_ATIVO + Dados.QT_BENEFICIARIO_ADERIDO - Dados.QT_BENEFICIARIO_CANCELADO) as BenfNet
# FROM Empresas JOIN Data_mes JOIN Localidade JOIN Cobertura_plano JOIN Dados ON Dados.id_CD_OPERADORA = Empresas.ID AND Dados.id_ID_CMPT_MOVEL = Data_mes.ID AND Dados.id_COBERTURA_ASSIST_PLAN = Cobertura_plano.ID AND Dados.id_CD_MUNICIPIO = Localidade.ID
# WHERE (Data_mes.ID_CMPT_MOVEL = '202012' OR Data_mes.ID_CMPT_MOVEL = '202011') AND Empresas.CD_OPERADORA = '368253'
# group by Cobertura_plano.ID, Localidade.SG_UF, Dados.id_CD_OPERADORA,  Data_mes.ID_CMPT_MOVEL
# order by  Data_mes.ID_CMPT_MOVEL, Empresas.NM_RAZAO_SOCIAL, Localidade.SG_UF

# SELECT                  Data_mes.ID_CMPT_MOVEL,
#                 		Empresas.CD_OPERADORA,
# 						Empresas.NM_RAZAO_SOCIAL,
# 						Localidade.SG_UF,
# 						Cobertura_plano.COBERTURA_ASSIST_PLAN,
#                 		sum(Dados.QT_BENEFICIARIO_ATIVO) as BenefAtivo, 
#                 		sum(Dados.QT_BENEFICIARIO_ADERIDO) as BenefAderido,
#                 		sum(dados.QT_BENEFICIARIO_CANCELADO) as BeneCancel
#                 from Data_mes JOIN Empresas JOIN Localidade JOIN Cobertura_plano JOIN Dados 
# 				ON Dados.id_ID_CMPT_MOVEL = Data_mes.ID AND Dados.id_CD_OPERADORA = Empresas.ID AND Dados.id_CD_MUNICIPIO = Localidade.ID AND dados.id_COBERTURA_ASSIST_PLAN = Cobertura_plano.ID
#                 group by Dados.id_ID_CMPT_MOVEL,
#              			Dados.id_CD_OPERADORA,
#              			Dados.id_MODALIDADE_OPERADORA,
#              			Dados.id_CD_MUNICIPIO,
#              			Dados.id_DE_CONTRATACAO_PLANO,
#              			Dados.id_DE_SEGMENTACAO_PLANO,
#                         Dados.id_COBERTURA_ASSIST_PLAN 
# 				ORDER BY Data_mes.ID_CMPT_MOVEL DESC, Empresas.NM_RAZAO_SOCIAL, Localidade.SG_UF, Cobertura_plano.COBERTURA_ASSIST_PLAN , sum(Dados.QT_BENEFICIARIO_ATIVO) DESC



