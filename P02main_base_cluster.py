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


UF = ('AC','AL','AM','AP','BA','CE','DF','ES','GO','MA','MG','MS','MT','PA','PB', \
      'PE','PI','PR','RJ','RN','RO','RR','SC','SE','SP','TO','XX')

#-----------------------------------------------------------------------------#
#--------------------------# Empresas selecionadas #--------------------------#
    

#-----------------------------------------------------------------------------#
#----------------------------------# MAIN  #----------------------------------#


### Abre a conexao
conn = sqlite3.connect("Precificacao.db")
cur = conn.cursor()
n = 0
P0201funcoes_tabelas.createDBTable(cur)


n = n+1
excelFile = "rps_cadop.csv"
P0201funcoes_db.incluir_dados_planilhaRPS_CADOP(cur, excelFile)
print(str(n) + ") " + "dowload feito com sucesso")

n = n+1
excelFile = "nt_vc.csv"
P0201funcoes_db.incluir_dados_planilhaNT_VC(cur, excelFile)
print(str(n) + ") " + "dowload feito com sucesso")

n = n+1
excelFile = "nt_vc_mes.csv"
P0201funcoes_db.incluir_dados_planilhaNT_VC_MES(cur, excelFile)
print(str(n) + ") " + "dowload feito com sucesso")

n = n+1
excelFile = "caracteristicas_produtos_saude_suplementar.csv"
P0201funcoes_db.incluir_dados_planilhaSAUDE_SUPLEMENTAR(cur, excelFile)
print(str(n) + ") " + "dowload feito com sucesso")

n = n+1
P0201funcoes_db.incluir_dados_planilhaNOME_PLANO(cur)
print(str(n) + ") " + "dowload feito com sucesso")

n = n+1
for uf in UF:
    # EXCLUIR ESSA LINHA
    # if uf == 'SP':
    try:
        excelFile = 'ben202101_' + uf + '.csv'
        P0201funcoes_db.incluir_dados_planilhaben202101(cur, excelFile)
    except:
        print(uf + ' nao encontrado')
print(str(n) + ") " + "dowload feito com sucesso")

n = n+1
P0201funcoes_db.incluir_dados_planilhaben202101_ajustado(cur)
print(str(n) + ") " + "dowload feito com sucesso")

n = n+1
P0201funcoes_db.incluir_dados_planilhasula_planos(cur)
print(str(n) + ") " + "dowload feito com sucesso")

n = n+1
P0201funcoes_db.incluir_dados_planilhacluster_planos(cur)
print(str(n) + ") " + "dowload feito com sucesso")

print('saiu')

conn.commit()
conn.close()
    



# SELECT ben202101_ajustado.ID_CMPT_MOVEL as data, ben202101_ajustado.CD_OPERADORA as cd_operadora, ben202101_ajustado.NM_RAZAO_SOCIAL as nome, ben202101_ajustado.DE_FAIXA_ETARIA_REAJ as idade, ben202101_ajustado.COBERTURA_ASSIST_PLAN as cobertura, ben202101_ajustado.DE_CONTRATACAO_PLANO as contratacao, SAUDE_SUPLEMENTAR.NM_PLANO as nome_plano, ben202101_ajustado.QT_BENEFICIARIO_ATIVO as ben_ativo
# FROM SAUDE_SUPLEMENTAR JOIN ben202101_ajustado ON ben202101_ajustado.CD_PLANO = SAUDE_SUPLEMENTAR.CD_PLANO
# WHERE ben202101_ajustado.CD_OPERADORA = 6246 and ben202101_ajustado.DE_FAIXA_ETARIA_REAJ like '%33%' AND ben202101_ajustado.COBERTURA_ASSIST_PLAN like 'm%'
# order by ben202101_ajustado.QT_BENEFICIARIO_ATIVO DESC

# SELECT NT_VC_MES.ANO_MES as Data, RPS_CADOP.RAZAO_SOCIAL, NT_VC.FAIXA_ETARIA as faixa_etaria, SAUDE_SUPLEMENTAR.NM_PLANO as nome_plano, NT_VC.VL_COML_MENL as preco
# FROM NT_VC, RPS_CADOP, SAUDE_SUPLEMENTAR JOIN NT_VC_MES ON NT_VC_MES.ID_PLANO = RPS_CADOP.ID_PLANO and NT_VC_MES.CD_NOTA = NT_VC.CD_NOTA and RPS_CADOP.CD_PLANO = SAUDE_SUPLEMENTAR.CD_PLANO
# WHERE RPS_CADOP.razao_social like 'sul%' and RPS_CADOP.CD_OPERADORA = 6246 and NT_VC.faixa_etaria like '29%'
# ORDER BY NT_VC_MES.ANO_MES DESC

# Um plano da sula
# SELECT ben202101_ajustado.ID_CMPT_MOVEL as data, ben202101_ajustado.CD_OPERADORA as cd_operadora, ben202101_ajustado.NM_RAZAO_SOCIAL as nome, ben202101_ajustado.DE_FAIXA_ETARIA_REAJ as idade, ben202101_ajustado.COBERTURA_ASSIST_PLAN as cobertura, ben202101_ajustado.DE_CONTRATACAO_PLANO as contratacao, SAUDE_SUPLEMENTAR.NM_PLANO as nome_plano, ben202101_ajustado.QT_BENEFICIARIO_ATIVO as ben_ativo
# FROM SAUDE_SUPLEMENTAR JOIN ben202101_ajustado ON ben202101_ajustado.CD_PLANO = SAUDE_SUPLEMENTAR.CD_PLANO
# WHERE ben202101_ajustado.CD_OPERADORA = 6246 and ben202101_ajustado.DE_FAIXA_ETARIA_REAJ like '%33%' AND ben202101_ajustado.COBERTURA_ASSIST_PLAN like 'm%'
# 	  and (SAUDE_SUPLEMENTAR.NM_PLANO like 'Especial 100 Empresarial%')
# order by ben202101_ajustado.QT_BENEFICIARIO_ATIVO DESC




# SELECT ben202101_ajustado.ID_CMPT_MOVEL as data, ben202101_ajustado.CD_OPERADORA as cd_operadora, ben202101_ajustado.NM_RAZAO_SOCIAL as nome, ben202101_ajustado.DE_FAIXA_ETARIA_REAJ as idade, ben202101_ajustado.COBERTURA_ASSIST_PLAN as cobertura, ben202101_ajustado.DE_CONTRATACAO_PLANO as contratacao, SAUDE_SUPLEMENTAR.NM_PLANO as nome_plano, ben202101_ajustado.QT_BENEFICIARIO_ATIVO as ben_ativo, ben202101_ajustado.QT_NET_ADDS as Net_adds
# FROM SAUDE_SUPLEMENTAR JOIN ben202101_ajustado ON ben202101_ajustado.CD_PLANO = SAUDE_SUPLEMENTAR.CD_PLANO
# WHERE ben202101_ajustado.CD_OPERADORA = 6246 and ben202101_ajustado.DE_FAIXA_ETARIA_REAJ like '%33%' AND ben202101_ajustado.COBERTURA_ASSIST_PLAN like 'm%'
# group by ben202101_ajustado.ID_CMPT_MOVEL, ben202101_ajustado.DE_FAIXA_ETARIA_REAJ, ben202101_ajustado.COBERTURA_ASSIST_PLAN, ben202101_ajustado.DE_CONTRATACAO_PLANO, SAUDE_SUPLEMENTAR.NM_PLANO
# order by ben202101_ajustado.DE_CONTRATACAO_PLANO, ben202101_ajustado.QT_BENEFICIARIO_ATIVO DESC

# SELECT ben202101_ajustado.ID_CMPT_MOVEL as data, ben202101_ajustado.NM_RAZAO_SOCIAL as nome, ben202101_ajustado.DE_FAIXA_ETARIA_REAJ as idade, ben202101_ajustado.COBERTURA_ASSIST_PLAN as cobertura, ben202101_ajustado.DE_CONTRATACAO_PLANO as contratacao, SAUDE_SUPLEMENTAR.NM_PLANO as nome_plano, ben202101_ajustado.QT_BENEFICIARIO_ATIVO as ben_ativo, ben202101_ajustado.QT_NET_ADDS as Net_adds
# FROM SAUDE_SUPLEMENTAR JOIN ben202101_ajustado ON ben202101_ajustado.CD_PLANO = SAUDE_SUPLEMENTAR.CD_PLANO
# WHERE ben202101_ajustado.CD_OPERADORA = 6246 and ben202101_ajustado.DE_FAIXA_ETARIA_REAJ like '%33%' AND ben202101_ajustado.COBERTURA_ASSIST_PLAN like 'm%'
#  	  and (ben202101_ajustado.DE_CONTRATACAO_PLANO like '%EMPR%' and SAUDE_SUPLEMENTAR.NM_PLANO like '%executivo%')
# GROUP by  ben202101_ajustado.ID_CMPT_MOVEL, ben202101_ajustado.NM_RAZAO_SOCIAL, ben202101_ajustado.DE_FAIXA_ETARIA_REAJ, ben202101_ajustado.COBERTURA_ASSIST_PLAN, ben202101_ajustado.DE_CONTRATACAO_PLANO, SAUDE_SUPLEMENTAR.NM_PLANO
# order by ben202101_ajustado.QT_BENEFICIARIO_ATIVO DESC

# SELECT ben202101_ajustado.ID_CMPT_MOVEL as data, ben202101_ajustado.NM_RAZAO_SOCIAL as nome, ben202101_ajustado.DE_FAIXA_ETARIA_REAJ as idade, ben202101_ajustado.COBERTURA_ASSIST_PLAN as cobertura, ben202101_ajustado.DE_CONTRATACAO_PLANO as contratacao, SAUDE_SUPLEMENTAR.NM_PLANO as nome_plano, sum(ben202101_ajustado.QT_BENEFICIARIO_ATIVO) as ben_ativo, sum(ben202101_ajustado.QT_NET_ADDS) as Net_adds
# FROM SAUDE_SUPLEMENTAR JOIN ben202101_ajustado ON ben202101_ajustado.CD_PLANO = SAUDE_SUPLEMENTAR.CD_PLANO
# WHERE ben202101_ajustado.CD_OPERADORA = 6246 and ben202101_ajustado.DE_FAIXA_ETARIA_REAJ like '%33%' AND ben202101_ajustado.COBERTURA_ASSIST_PLAN like 'm%'
#  	  and (ben202101_ajustado.DE_CONTRATACAO_PLANO like '%ade%' and SAUDE_SUPLEMENTAR.NM_PLANO like '%básico%')
# GROUP by  ben202101_ajustado.ID_CMPT_MOVEL, ben202101_ajustado.NM_RAZAO_SOCIAL, ben202101_ajustado.DE_FAIXA_ETARIA_REAJ, ben202101_ajustado.COBERTURA_ASSIST_PLAN, ben202101_ajustado.DE_CONTRATACAO_PLANO
# order by ben202101_ajustado.QT_BENEFICIARIO_ATIVO DESC





# teste para número benf

# SELECT ben202101_ajustado.ID_CMPT_MOVEL as data, ben202101_ajustado.CD_OPERADORA as cd_operadora, ben202101_ajustado.NM_RAZAO_SOCIAL as nome, ben202101_ajustado.COBERTURA_ASSIST_PLAN as cobertura, sum(ben202101_ajustado.QT_BENEFICIARIO_ATIVO) as ben_ativo
# FROM SAUDE_SUPLEMENTAR JOIN ben202101_ajustado ON ben202101_ajustado.CD_PLANO = SAUDE_SUPLEMENTAR.CD_PLANO and ben202101_ajustado.CD_OPERADORA = SAUDE_SUPLEMENTAR.CD_OPERADORA
# WHERE ben202101_ajustado.CD_OPERADORA = 6246 and ben202101_ajustado.COBERTURA_ASSIST_PLAN like 'm%'
# GROUP by ben202101_ajustado.ID_CMPT_MOVEL, ben202101_ajustado.CD_OPERADORA, ben202101_ajustado.NM_RAZAO_SOCIAL, ben202101_ajustado.COBERTURA_ASSIST_PLAN
# order by ben202101_ajustado.QT_BENEFICIARIO_ATIVO DESC

# SELECT sum(sula_planos.QT_BENEFICIARIO_ATIVO)
# FROM sula_planos