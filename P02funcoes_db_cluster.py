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

def incluir_dados_planilhaRPS_CADOP (cur, excelFile):
    excelFileName = excelFile
    try:
            fhand = open(excelFileName)
    except:
            print('Arquivo não encontrado')
    
    fhand.readline()    # skip first row
    p = 1
    j = 0
    for row in fhand:
        # print(p)
        # print(row)
        j = j+1
        rs = row.split(',')
        try:
            P0201funcoes_tabelas.insertRPS_CADOP(cur, rs)
        except:
            # print(str(p) + '  ' + str(j))
            # print(rs[3])
            p = p+1
 
def incluir_dados_planilhaNT_VC (cur, excelFile):
    excelFileName = excelFile
    try:
            fhand = open(excelFileName)
    except:
            print('Arquivo não encontrado')
    
    fhand.readline()    # skip first row
    p = 1
    j = 0
    for row in fhand:
        # print(p)
        # print(row)
        j = j+1
        rs = row.split(',')
        try:
            P0201funcoes_tabelas.insertNT_VC(cur, rs)
        except:
            p = p+1

def incluir_dados_planilhaNT_VC_MES (cur, excelFile):
    excelFileName = excelFile
    try:
            fhand = open(excelFileName)
    except:
            print('Arquivo não encontrado')
    
    fhand.readline()    # skip first row
    p = 1
    j = 0
    for row in fhand:
        # print(p)
        # print(row)
        j = j+1
        rs = row.split(',')
        try:
            P0201funcoes_tabelas.insertNT_VC_MES(cur, rs)
        except:
            p = p+1
            
def incluir_dados_planilhaSAUDE_SUPLEMENTAR (cur, excelFile):
    excelFileName = excelFile
    try:
            fhand = open(excelFileName)
    except:
            print('Arquivo não encontrado')
    
    fhand.readline()    # skip first row
    p = 1
    j = 0
    for row in fhand:
        # print(p)
        # print(row)
        j = j+1
        rs = row.split(';')
        try:
            P0201funcoes_tabelas.insertSAUDE_SUPLEMENTAR(cur, rs)
        except:
            p = p+1

def incluir_dados_planilhaNOME_PLANO (cur):
        
    ##### Para plano individual
    aux = cur.execute('''
        SELECT SAUDE_SUPLEMENTAR.CD_PLANO, SAUDE_SUPLEMENTAR.NM_PLANO, SAUDE_SUPLEMENTAR.CD_OPERADORA, SAUDE_SUPLEMENTAR.NM_PLANO
        FROM SAUDE_SUPLEMENTAR
        WHERE SAUDE_SUPLEMENTAR.CD_OPERADORA = 6246
			and (SAUDE_SUPLEMENTAR.CONTRATACAO like 'indiv%')
        ORDER by SAUDE_SUPLEMENTAR.CD_PLANO
                       ''',)
                       
    tableColumns = [description[0] for description in cur.description]
    df = pd.DataFrame(aux.fetchall(),  columns=tableColumns)
    
    for i, row in df.iterrows():
        rs = row.tolist()
        rs[3] = 'Individual ou familiar'
        try:
            P0201funcoes_tabelas.insertNOME_PLANO(cur, rs)
        except:
            print(rs)
    
    ### Para plano coletivo por adesão
    
    aux = cur.execute('''
        SELECT SAUDE_SUPLEMENTAR.CD_PLANO, SAUDE_SUPLEMENTAR.NM_PLANO, SAUDE_SUPLEMENTAR.CD_OPERADORA, SAUDE_SUPLEMENTAR.NM_PLANO
        FROM SAUDE_SUPLEMENTAR
        WHERE SAUDE_SUPLEMENTAR.CD_OPERADORA = 6246
			and (SAUDE_SUPLEMENTAR.CONTRATACAO like '%ade%' and SAUDE_SUPLEMENTAR.NM_PLANO like '%direto%')
        ORDER by SAUDE_SUPLEMENTAR.CD_PLANO
                       ''',)                
    tableColumns = [description[0] for description in cur.description]
    df = pd.DataFrame(aux.fetchall(),  columns=tableColumns)
    for i, row in df.iterrows():
        rs = row.tolist()
        rs[3] = 'Direto / Coletivo Adesão'
        try:
            P0201funcoes_tabelas.insertNOME_PLANO(cur, rs)
        except:
            print(rs)

    aux = cur.execute('''
        SELECT SAUDE_SUPLEMENTAR.CD_PLANO, SAUDE_SUPLEMENTAR.NM_PLANO, SAUDE_SUPLEMENTAR.CD_OPERADORA, SAUDE_SUPLEMENTAR.NM_PLANO
        FROM SAUDE_SUPLEMENTAR
        WHERE SAUDE_SUPLEMENTAR.CD_OPERADORA = 6246
			and (SAUDE_SUPLEMENTAR.CONTRATACAO like '%ade%' and SAUDE_SUPLEMENTAR.NM_PLANO like '%especial%')
        ORDER by SAUDE_SUPLEMENTAR.CD_PLANO
                       ''',)             
    tableColumns = [description[0] for description in cur.description]
    df = pd.DataFrame(aux.fetchall(),  columns=tableColumns)
    for i, row in df.iterrows():
        rs = row.tolist()
        rs[3] = 'Especial / Coletivo Adesão'
        try:
            P0201funcoes_tabelas.insertNOME_PLANO(cur, rs)
        except:
            print(rs)

    aux = cur.execute('''
        SELECT SAUDE_SUPLEMENTAR.CD_PLANO, SAUDE_SUPLEMENTAR.NM_PLANO, SAUDE_SUPLEMENTAR.CD_OPERADORA, SAUDE_SUPLEMENTAR.NM_PLANO
        FROM SAUDE_SUPLEMENTAR
        WHERE SAUDE_SUPLEMENTAR.CD_OPERADORA = 6246
			and (SAUDE_SUPLEMENTAR.CONTRATACAO like '%ade%' and SAUDE_SUPLEMENTAR.NM_PLANO like '%exato%')
        ORDER by SAUDE_SUPLEMENTAR.CD_PLANO
                       ''',)
    tableColumns = [description[0] for description in cur.description]
    df = pd.DataFrame(aux.fetchall(),  columns=tableColumns)
    for i, row in df.iterrows():
        rs = row.tolist()
        rs[3] = 'Exato / Coletivo Adesão'
        try:
            P0201funcoes_tabelas.insertNOME_PLANO(cur, rs)
        except:
            print(rs)

    aux = cur.execute('''
        SELECT SAUDE_SUPLEMENTAR.CD_PLANO, SAUDE_SUPLEMENTAR.NM_PLANO, SAUDE_SUPLEMENTAR.CD_OPERADORA, SAUDE_SUPLEMENTAR.NM_PLANO
        FROM SAUDE_SUPLEMENTAR
        WHERE SAUDE_SUPLEMENTAR.CD_OPERADORA = 6246
			and (SAUDE_SUPLEMENTAR.CONTRATACAO like '%ade%' and SAUDE_SUPLEMENTAR.NM_PLANO like '%ideal%')
        ORDER by SAUDE_SUPLEMENTAR.CD_PLANO
                       ''',)
    tableColumns = [description[0] for description in cur.description]
    df = pd.DataFrame(aux.fetchall(),  columns=tableColumns)
    for i, row in df.iterrows():
        rs = row.tolist()
        rs[3] = 'Ideal / Coletivo Adesão'
        try:
            P0201funcoes_tabelas.insertNOME_PLANO(cur, rs)
        except:
            print(rs)

    aux = cur.execute('''
        SELECT SAUDE_SUPLEMENTAR.CD_PLANO, SAUDE_SUPLEMENTAR.NM_PLANO, SAUDE_SUPLEMENTAR.CD_OPERADORA, SAUDE_SUPLEMENTAR.NM_PLANO
        FROM SAUDE_SUPLEMENTAR
        WHERE SAUDE_SUPLEMENTAR.CD_OPERADORA = 6246
			and (SAUDE_SUPLEMENTAR.CONTRATACAO like '%ade%' and SAUDE_SUPLEMENTAR.NM_PLANO like '%clássico%')
        ORDER by SAUDE_SUPLEMENTAR.CD_PLANO
                       ''',)
    tableColumns = [description[0] for description in cur.description]
    df = pd.DataFrame(aux.fetchall(),  columns=tableColumns)
    for i, row in df.iterrows():
        rs = row.tolist()
        rs[3] = 'Clássico / Coletivo Adesão'
        try:
            P0201funcoes_tabelas.insertNOME_PLANO(cur, rs)
        except:
            print(rs)
            
    aux = cur.execute('''
        SELECT SAUDE_SUPLEMENTAR.CD_PLANO, SAUDE_SUPLEMENTAR.NM_PLANO, SAUDE_SUPLEMENTAR.CD_OPERADORA, SAUDE_SUPLEMENTAR.NM_PLANO
        FROM SAUDE_SUPLEMENTAR
        WHERE SAUDE_SUPLEMENTAR.CD_OPERADORA = 6246
			and (SAUDE_SUPLEMENTAR.CONTRATACAO like '%ade%' and (SAUDE_SUPLEMENTAR.NM_PLANO like '%básico%' OR SAUDE_SUPLEMENTAR.NM_PLANO like '%basico%' OR SAUDE_SUPLEMENTAR.NM_PLANO like '%BÁSICO%') and SAUDE_SUPLEMENTAR.NM_PLANO not like '%clássico%')
        ORDER by SAUDE_SUPLEMENTAR.CD_PLANO
                       ''',)
    tableColumns = [description[0] for description in cur.description]
    df = pd.DataFrame(aux.fetchall(),  columns=tableColumns)
    for i, row in df.iterrows():
        rs = row.tolist()
        rs[3] = 'Básico / Coletivo Adesão'
        try:
            P0201funcoes_tabelas.insertNOME_PLANO(cur, rs)
        except:
            print(rs)
            
     ### Para plano coletivo por Empresarial
    aux = cur.execute('''
        SELECT SAUDE_SUPLEMENTAR.CD_PLANO, SAUDE_SUPLEMENTAR.NM_PLANO, SAUDE_SUPLEMENTAR.CD_OPERADORA, SAUDE_SUPLEMENTAR.NM_PLANO
        FROM SAUDE_SUPLEMENTAR
        WHERE SAUDE_SUPLEMENTAR.CD_OPERADORA = 6246
			and (SAUDE_SUPLEMENTAR.CONTRATACAO like '%emp%' and SAUDE_SUPLEMENTAR.NM_PLANO like '%direto%')
        ORDER by SAUDE_SUPLEMENTAR.CD_PLANO
                       ''',)                
    tableColumns = [description[0] for description in cur.description]
    df = pd.DataFrame(aux.fetchall(),  columns=tableColumns)
    for i, row in df.iterrows():
        rs = row.tolist()
        rs[3] = 'Direto / Coletivo Empresarial'
        try:
            P0201funcoes_tabelas.insertNOME_PLANO(cur, rs)
        except:
            print(rs)

    aux = cur.execute('''
        SELECT SAUDE_SUPLEMENTAR.CD_PLANO, SAUDE_SUPLEMENTAR.NM_PLANO, SAUDE_SUPLEMENTAR.CD_OPERADORA, SAUDE_SUPLEMENTAR.NM_PLANO
        FROM SAUDE_SUPLEMENTAR
        WHERE SAUDE_SUPLEMENTAR.CD_OPERADORA = 6246
			and (SAUDE_SUPLEMENTAR.CONTRATACAO like '%emp%' and SAUDE_SUPLEMENTAR.NM_PLANO like '%especial%')
        ORDER by SAUDE_SUPLEMENTAR.CD_PLANO
                       ''',)             
    tableColumns = [description[0] for description in cur.description]
    df = pd.DataFrame(aux.fetchall(),  columns=tableColumns)
    for i, row in df.iterrows():
        rs = row.tolist()
        rs[3] = 'Especial / Coletivo Empresarial'
        try:
            P0201funcoes_tabelas.insertNOME_PLANO(cur, rs)
        except:
            print(rs)

    aux = cur.execute('''
        SELECT SAUDE_SUPLEMENTAR.CD_PLANO, SAUDE_SUPLEMENTAR.NM_PLANO, SAUDE_SUPLEMENTAR.CD_OPERADORA, SAUDE_SUPLEMENTAR.NM_PLANO
        FROM SAUDE_SUPLEMENTAR
        WHERE SAUDE_SUPLEMENTAR.CD_OPERADORA = 6246
			and (SAUDE_SUPLEMENTAR.CONTRATACAO like '%emp%' and SAUDE_SUPLEMENTAR.NM_PLANO like '%exato%')
        ORDER by SAUDE_SUPLEMENTAR.CD_PLANO
                       ''',)
    tableColumns = [description[0] for description in cur.description]
    df = pd.DataFrame(aux.fetchall(),  columns=tableColumns)
    for i, row in df.iterrows():
        rs = row.tolist()
        rs[3] = 'Exato / Coletivo Empresarial'
        try:
            P0201funcoes_tabelas.insertNOME_PLANO(cur, rs)
        except:
            print(rs)

    aux = cur.execute('''
        SELECT SAUDE_SUPLEMENTAR.CD_PLANO, SAUDE_SUPLEMENTAR.NM_PLANO, SAUDE_SUPLEMENTAR.CD_OPERADORA, SAUDE_SUPLEMENTAR.NM_PLANO
        FROM SAUDE_SUPLEMENTAR
        WHERE SAUDE_SUPLEMENTAR.CD_OPERADORA = 6246
			and (SAUDE_SUPLEMENTAR.CONTRATACAO like '%emp%' and SAUDE_SUPLEMENTAR.NM_PLANO like '%executivo%')
        ORDER by SAUDE_SUPLEMENTAR.CD_PLANO
                       ''',)
    tableColumns = [description[0] for description in cur.description]
    df = pd.DataFrame(aux.fetchall(),  columns=tableColumns)
    for i, row in df.iterrows():
        rs = row.tolist()
        rs[3] = 'Executivo / Coletivo Empresarial'
        try:
            P0201funcoes_tabelas.insertNOME_PLANO(cur, rs)
        except:
            print(rs)

    aux = cur.execute('''
        SELECT SAUDE_SUPLEMENTAR.CD_PLANO, SAUDE_SUPLEMENTAR.NM_PLANO, SAUDE_SUPLEMENTAR.CD_OPERADORA, SAUDE_SUPLEMENTAR.NM_PLANO
        FROM SAUDE_SUPLEMENTAR
        WHERE SAUDE_SUPLEMENTAR.CD_OPERADORA = 6246
			and (SAUDE_SUPLEMENTAR.CONTRATACAO like '%emp%' and SAUDE_SUPLEMENTAR.NM_PLANO like '%clássico%')
        ORDER by SAUDE_SUPLEMENTAR.CD_PLANO
                       ''',)
    tableColumns = [description[0] for description in cur.description]
    df = pd.DataFrame(aux.fetchall(),  columns=tableColumns)
    for i, row in df.iterrows():
        rs = row.tolist()
        rs[3] = 'Clássico / Coletivo Empresarial'
        try:
            P0201funcoes_tabelas.insertNOME_PLANO(cur, rs)
        except:
            print(rs)
            
    aux = cur.execute('''
        SELECT SAUDE_SUPLEMENTAR.CD_PLANO, SAUDE_SUPLEMENTAR.NM_PLANO, SAUDE_SUPLEMENTAR.CD_OPERADORA, SAUDE_SUPLEMENTAR.NM_PLANO
        FROM SAUDE_SUPLEMENTAR
        WHERE SAUDE_SUPLEMENTAR.CD_OPERADORA = 6246
			and (SAUDE_SUPLEMENTAR.CONTRATACAO like '%emp%' and (SAUDE_SUPLEMENTAR.NM_PLANO like '%básico%' OR SAUDE_SUPLEMENTAR.NM_PLANO like '%basico%' OR SAUDE_SUPLEMENTAR.NM_PLANO like '%BÁSICO%') and SAUDE_SUPLEMENTAR.NM_PLANO not like '%clássico%')
        ORDER by SAUDE_SUPLEMENTAR.CD_PLANO
                       ''',)
    tableColumns = [description[0] for description in cur.description]
    df = pd.DataFrame(aux.fetchall(),  columns=tableColumns)
    for i, row in df.iterrows():
        rs = row.tolist()
        rs[3] = 'Básico / Coletivo Empresarial'
        try:
            P0201funcoes_tabelas.insertNOME_PLANO(cur, rs)
        except:
            print(rs)
            
    ####OUTROS
    
    aux = cur.execute('''
        SELECT SAUDE_SUPLEMENTAR.CD_PLANO, SAUDE_SUPLEMENTAR.NM_PLANO, SAUDE_SUPLEMENTAR.CD_OPERADORA, SAUDE_SUPLEMENTAR.NM_PLANO
        FROM SAUDE_SUPLEMENTAR
        WHERE SAUDE_SUPLEMENTAR.CD_OPERADORA = 6246
			and (SAUDE_SUPLEMENTAR.CONTRATACAO like '%ade%' and SAUDE_SUPLEMENTAR.NM_PLANO not like '%direto%' and SAUDE_SUPLEMENTAR.NM_PLANO not like '%especial%' and SAUDE_SUPLEMENTAR.NM_PLANO not like '%exato%' and SAUDE_SUPLEMENTAR.NM_PLANO not like '%ideal%' and SAUDE_SUPLEMENTAR.NM_PLANO not like '%clássico%' and SAUDE_SUPLEMENTAR.NM_PLANO not like '%básico%' and SAUDE_SUPLEMENTAR.NM_PLANO not like '%basico%' AND SAUDE_SUPLEMENTAR.NM_PLANO not like '%BÁSICO%')
        ORDER by SAUDE_SUPLEMENTAR.CD_PLANO
                       ''',)
    tableColumns = [description[0] for description in cur.description]
    df = pd.DataFrame(aux.fetchall(),  columns=tableColumns)
    for i, row in df.iterrows():
        rs = row.tolist()
        rs[3] = 'Outros / Coletivo Adesão'
        try:
            P0201funcoes_tabelas.insertNOME_PLANO(cur, rs)
        except:
            print(rs)
    
    aux = cur.execute('''
        SELECT SAUDE_SUPLEMENTAR.CD_PLANO, SAUDE_SUPLEMENTAR.NM_PLANO, SAUDE_SUPLEMENTAR.CD_OPERADORA, SAUDE_SUPLEMENTAR.NM_PLANO
        FROM SAUDE_SUPLEMENTAR
        WHERE SAUDE_SUPLEMENTAR.CD_OPERADORA = 6246
			and (SAUDE_SUPLEMENTAR.CONTRATACAO like '%emp%' and SAUDE_SUPLEMENTAR.NM_PLANO not like '%direto%' and SAUDE_SUPLEMENTAR.NM_PLANO not like '%especial%' and SAUDE_SUPLEMENTAR.NM_PLANO not like '%exato%' and SAUDE_SUPLEMENTAR.NM_PLANO not like '%executivo%' and SAUDE_SUPLEMENTAR.NM_PLANO not like '%clássico%' and SAUDE_SUPLEMENTAR.NM_PLANO not like '%básico%' and SAUDE_SUPLEMENTAR.NM_PLANO not like '%basico%' AND SAUDE_SUPLEMENTAR.NM_PLANO not like '%BÁSICO%')
        ORDER by SAUDE_SUPLEMENTAR.CD_PLANO
                       ''',)
    tableColumns = [description[0] for description in cur.description]
    df = pd.DataFrame(aux.fetchall(),  columns=tableColumns)
    for i, row in df.iterrows():
        rs = row.tolist()
        rs[3] = 'Outros / Coletivo Empresarial'
        try:
            P0201funcoes_tabelas.insertNOME_PLANO(cur, rs)
        except:
            print(rs)
    
    
    # AMIL
        aux = cur.execute('''
        SELECT SAUDE_SUPLEMENTAR.CD_PLANO, SAUDE_SUPLEMENTAR.NM_PLANO, SAUDE_SUPLEMENTAR.CD_OPERADORA, SAUDE_SUPLEMENTAR.COBERTURA
        FROM SAUDE_SUPLEMENTAR
        WHERE SAUDE_SUPLEMENTAR.CD_OPERADORA = 326305 and (SAUDE_SUPLEMENTAR.NM_PLANO like '%S40%')
                       ''',)
    tableColumns = [description[0] for description in cur.description]
    df = pd.DataFrame(aux.fetchall(),  columns=tableColumns)
    for i, row in df.iterrows():
        rs = row.tolist()
        rs[3] = 'S40'
        try:
            P0201funcoes_tabelas.insertNOME_PLANO(cur, rs)
        except:
            print(rs)
            
        aux = cur.execute('''
        SELECT SAUDE_SUPLEMENTAR.CD_PLANO, SAUDE_SUPLEMENTAR.NM_PLANO, SAUDE_SUPLEMENTAR.CD_OPERADORA, SAUDE_SUPLEMENTAR.COBERTURA
        FROM SAUDE_SUPLEMENTAR
        WHERE SAUDE_SUPLEMENTAR.CD_OPERADORA = 326305 and (SAUDE_SUPLEMENTAR.NM_PLANO like '%S60%' and SAUDE_SUPLEMENTAR.CONTRATACAO like '%ade%')
                       ''',)
    tableColumns = [description[0] for description in cur.description]
    df = pd.DataFrame(aux.fetchall(),  columns=tableColumns)
    for i, row in df.iterrows():
        rs = row.tolist()
        rs[3] = 'S60 / Coletivo Adesão'
        try:
            P0201funcoes_tabelas.insertNOME_PLANO(cur, rs)
        except:
            print(rs)
        
        aux = cur.execute('''
        SELECT SAUDE_SUPLEMENTAR.CD_PLANO, SAUDE_SUPLEMENTAR.NM_PLANO, SAUDE_SUPLEMENTAR.CD_OPERADORA, SAUDE_SUPLEMENTAR.COBERTURA
        FROM SAUDE_SUPLEMENTAR
        WHERE SAUDE_SUPLEMENTAR.CD_OPERADORA = 326305 and (SAUDE_SUPLEMENTAR.NM_PLANO like '%S60%' and SAUDE_SUPLEMENTAR.CONTRATACAO like '%emp%')
                       ''',)
    tableColumns = [description[0] for description in cur.description]
    df = pd.DataFrame(aux.fetchall(),  columns=tableColumns)
    for i, row in df.iterrows():
        rs = row.tolist()
        rs[3] = 'S60 / Coletivo Empresarial'
        try:
            P0201funcoes_tabelas.insertNOME_PLANO(cur, rs)
        except:
            print(rs)

        aux = cur.execute('''
        SELECT SAUDE_SUPLEMENTAR.CD_PLANO, SAUDE_SUPLEMENTAR.NM_PLANO, SAUDE_SUPLEMENTAR.CD_OPERADORA, SAUDE_SUPLEMENTAR.COBERTURA
        FROM SAUDE_SUPLEMENTAR
        WHERE SAUDE_SUPLEMENTAR.CD_OPERADORA = 326305 and (SAUDE_SUPLEMENTAR.NM_PLANO not like '%S40%' and SAUDE_SUPLEMENTAR.NM_PLANO not like '%S60%')
                       ''',)
    tableColumns = [description[0] for description in cur.description]
    df = pd.DataFrame(aux.fetchall(),  columns=tableColumns)
    for i, row in df.iterrows():
        rs = row.tolist()
        rs[3] = 'Outros'
        try:
            P0201funcoes_tabelas.insertNOME_PLANO(cur, rs)
        except:
            print(rs)
    
    


def incluir_dados_planilhaben202101 (cur, excelFile):
    excelFileName = excelFile
    try:
            fhand = open(excelFileName)
    except:
            print('Arquivo não encontrado')
    
    fhand.readline()    # skip first row
    p = 1
    j = 0
    for row in fhand:
        # print(p)
        # print(row)
        j = j+1
        rs = row.split(';')
        try:
            P0201funcoes_tabelas.insertben202101(cur, rs)
        except:
            p = p+1

def incluir_dados_planilhaben202101_ajustado (cur):
    
    aux = cur.execute('''
    SELECT ben202101.ID_CMPT_MOVEL as data, ben202101.CD_OPERADORA as cd_operadora, ben202101.NM_RAZAO_SOCIAL as nome, ben202101.de_faixa_etaria_reaj as faixa_etaria, ben202101.COBERTURA_ASSIST_PLAN as cobertura, ben202101.DE_CONTRATACAO_PLANO as contratacao, ben202101.CD_PLANO as cd_plano, sum(ben202101.QT_BENEFICIARIO_ATIVO) as benf_ativo, sum(ben202101.QT_BENEFICIARIO_ADERIDO - ben202101.qt_beneficiario_cancelado) as net_adds
    FROM ben202101
    GROUP BY ben202101.ID_CMPT_MOVEL, ben202101.CD_OPERADORA, ben202101.de_faixa_etaria_reaj, ben202101.COBERTURA_ASSIST_PLAN, ben202101.DE_CONTRATACAO_PLANO, ben202101.CD_PLANO
                       ''',)
    
    tableColumns = [description[0] for description in cur.description]
    df = pd.DataFrame(aux.fetchall(),  columns=tableColumns)
    
    for i, row in df.iterrows():
        rs = row.tolist()
        P0201funcoes_tabelas.insertben202101_ajustado(cur, rs)
    

def incluir_dados_planilhasula_planos (cur):
    ### SULA
   
    aux = cur.execute('''
    SELECT ben202101_ajustado.ID_CMPT_MOVEL as data, ben202101_ajustado.CD_OPERADORA as cd_operadora, ben202101_ajustado.NM_RAZAO_SOCIAL as nome, ben202101_ajustado.DE_FAIXA_ETARIA_REAJ as idade, ben202101_ajustado.COBERTURA_ASSIST_PLAN as cobertura, ben202101_ajustado.DE_CONTRATACAO_PLANO as contratacao,NOME_PLANO.NM_PLANO as nome_plano, NOME_PLANO.CLUSTER as cluster, ben202101_ajustado.QT_BENEFICIARIO_ATIVO as ben_ativo, ben202101_ajustado.QT_NET_ADDS as Net_adds
    FROM NOME_PLANO JOIN ben202101_ajustado ON ben202101_ajustado.CD_PLANO = NOME_PLANO.CD_PLANO and ben202101_ajustado.CD_OPERADORA = NOME_PLANO.CD_OPERADORA
    WHERE ben202101_ajustado.CD_OPERADORA = 6246 and ben202101_ajustado.DE_FAIXA_ETARIA_REAJ like '%33%' AND ben202101_ajustado.COBERTURA_ASSIST_PLAN like 'm%'
    order by ben202101_ajustado.QT_BENEFICIARIO_ATIVO DESC
                       ''',)
    
    tableColumns = [description[0] for description in cur.description]
    df = pd.DataFrame(aux.fetchall(),  columns=tableColumns)
    
    for i, row in df.iterrows():
        rs = row.tolist()
        P0201funcoes_tabelas.insertsula_planos(cur, rs)
        
    ### AMIL
   
    aux = cur.execute('''
    SELECT ben202101_ajustado.ID_CMPT_MOVEL as data, ben202101_ajustado.CD_OPERADORA as cd_operadora, ben202101_ajustado.NM_RAZAO_SOCIAL as nome, ben202101_ajustado.DE_FAIXA_ETARIA_REAJ as idade, ben202101_ajustado.COBERTURA_ASSIST_PLAN as cobertura, ben202101_ajustado.DE_CONTRATACAO_PLANO as contratacao,NOME_PLANO.NM_PLANO as nome_plano, NOME_PLANO.CLUSTER as cluster, ben202101_ajustado.QT_BENEFICIARIO_ATIVO as ben_ativo, ben202101_ajustado.QT_NET_ADDS as Net_adds
    FROM NOME_PLANO JOIN ben202101_ajustado ON ben202101_ajustado.CD_PLANO = NOME_PLANO.CD_PLANO and ben202101_ajustado.CD_OPERADORA = NOME_PLANO.CD_OPERADORA
    WHERE ben202101_ajustado.CD_OPERADORA = 326305 and ben202101_ajustado.DE_FAIXA_ETARIA_REAJ like '%33%' AND ben202101_ajustado.COBERTURA_ASSIST_PLAN like 'm%'
    order by ben202101_ajustado.QT_BENEFICIARIO_ATIVO DESC
                       ''',)
    
    tableColumns = [description[0] for description in cur.description]
    df = pd.DataFrame(aux.fetchall(),  columns=tableColumns)
    
    for i, row in df.iterrows():
        rs = row.tolist()
        P0201funcoes_tabelas.insertsula_planos(cur, rs)


def incluir_dados_planilhacluster_planos (cur):
    
    ##########################################
    #### SULA
    
    aux = cur.execute('''
    SELECT ben202101_ajustado.ID_CMPT_MOVEL as data, ben202101_ajustado.NM_RAZAO_SOCIAL as nome, ben202101_ajustado.DE_FAIXA_ETARIA_REAJ as idade, ben202101_ajustado.COBERTURA_ASSIST_PLAN as cobertura, ben202101_ajustado.DE_CONTRATACAO_PLANO as contratacao, NOME_PLANO.CLUSTER as cluster, sum(ben202101_ajustado.QT_BENEFICIARIO_ATIVO) as ben_ativo, sum(ben202101_ajustado.QT_NET_ADDS) as Net_adds
    FROM NOME_PLANO JOIN ben202101_ajustado ON ben202101_ajustado.CD_PLANO = NOME_PLANO.CD_PLANO and ben202101_ajustado.CD_OPERADORA = NOME_PLANO.CD_OPERADORA
    WHERE ben202101_ajustado.CD_OPERADORA = 6246 and ben202101_ajustado.DE_FAIXA_ETARIA_REAJ like '%33%' AND ben202101_ajustado.COBERTURA_ASSIST_PLAN like 'm%'
    GROUP by  ben202101_ajustado.ID_CMPT_MOVEL, ben202101_ajustado.NM_RAZAO_SOCIAL, ben202101_ajustado.DE_FAIXA_ETARIA_REAJ, ben202101_ajustado.COBERTURA_ASSIST_PLAN, ben202101_ajustado.DE_CONTRATACAO_PLANO, NOME_PLANO.CLUSTER
    order by ben202101_ajustado.QT_BENEFICIARIO_ATIVO DESC
                       ''',)
    
    tableColumns = [description[0] for description in cur.description]
    df = pd.DataFrame(aux.fetchall(),  columns=tableColumns)
    
    for i, row in df.iterrows():
        rs = row.tolist()
        P0201funcoes_tabelas.insertcluster_planos(cur, rs)
        

    
##########################################
    #### AMIL
    
    aux = cur.execute('''
    SELECT ben202101_ajustado.ID_CMPT_MOVEL as data, ben202101_ajustado.NM_RAZAO_SOCIAL as nome, ben202101_ajustado.DE_FAIXA_ETARIA_REAJ as idade, ben202101_ajustado.COBERTURA_ASSIST_PLAN as cobertura, ben202101_ajustado.DE_CONTRATACAO_PLANO as contratacao, NOME_PLANO.CLUSTER as cluster, sum(ben202101_ajustado.QT_BENEFICIARIO_ATIVO) as ben_ativo, sum(ben202101_ajustado.QT_NET_ADDS) as Net_adds
    FROM NOME_PLANO JOIN ben202101_ajustado ON ben202101_ajustado.CD_PLANO = NOME_PLANO.CD_PLANO and ben202101_ajustado.CD_OPERADORA = NOME_PLANO.CD_OPERADORA
    WHERE ben202101_ajustado.CD_OPERADORA = 326305 and ben202101_ajustado.DE_FAIXA_ETARIA_REAJ like '%33%' AND ben202101_ajustado.COBERTURA_ASSIST_PLAN like 'm%'
    GROUP by  ben202101_ajustado.ID_CMPT_MOVEL, ben202101_ajustado.NM_RAZAO_SOCIAL, ben202101_ajustado.DE_FAIXA_ETARIA_REAJ, ben202101_ajustado.COBERTURA_ASSIST_PLAN, ben202101_ajustado.DE_CONTRATACAO_PLANO, NOME_PLANO.CLUSTER
    order by ben202101_ajustado.QT_BENEFICIARIO_ATIVO DESC
                       ''',)
    
    tableColumns = [description[0] for description in cur.description]
    df = pd.DataFrame(aux.fetchall(),  columns=tableColumns)
    
    for i, row in df.iterrows():
        rs = row.tolist()
        P0201funcoes_tabelas.insertcluster_planos(cur, rs)

   
        

   