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

## Conversor de data inteiro em texto
# Exemplo, 202012 vira 12/2020

def conversor_data(num):
    aux = str(num)
    string = aux[4:6] + '/' + aux[0:4]
    
    return string

#-----------------------------------------------------------------------------#
#------------------------------# Cria Tabelas  #------------------------------#
# Create tabela A01
def criartabelaA01(cur3):
    
    ### Data / Nome / Qt beneficiários ativos
    # Essa tabela todas as datas mensais, agrupando o número de beneficiários ativos utilizando
    # apenas o nome da empresa como critério

    # Cria tabela de Dados
    cur3.executescript('''
    DROP TABLE IF EXISTS A01;
    CREATE TABLE A01 (
                    DATA                      INTEGER, 
                    NOME_EMPRESA              TEXT,
                    COBERTURA                 TEXT,
                    QT_BENEFICIARIO_ATIVO     INTEGER
    );
    ''')

def criartabelaA02(cur3):
    # Cria tabela de Dados
    cur3.executescript('''
    DROP TABLE IF EXISTS A02;
    CREATE TABLE A02 (
                    DATA                      INTEGER, 
                    NOME_EMPRESA              TEXT,
                    QT_BENEFICIARIO_ATIVO     INTEGER
    );
    ''')

def criartabelaA03(cur3):
    # Cria tabela de Dados
    cur3.executescript('''
    DROP TABLE IF EXISTS A03;
    CREATE TABLE A03 (
                    DATA                      INTEGER, 
                    NOME_EMPRESA              TEXT,
                    COBERTURA                 TEXT,
                    QT_BENEFICIARIO_ATIVO     INTEGER
    );
    ''')

def criartabelaA04(cur3):
    # Cria tabela de Dados
    cur3.executescript('''
    DROP TABLE IF EXISTS A04;
    CREATE TABLE A04 (
                    DATA                      INTEGER, 
                    NOME_EMPRESA              TEXT,
                    COBERTURA                 TEXT,
                    QT_BENEFICIARIO_ATIVO     INTEGER
    );
    ''')

def criartabelaA05(cur3):
    
    # Cria tabela de Dados
    cur3.executescript('''
    DROP TABLE IF EXISTS A05;
    CREATE TABLE A05 (
                    DATA                      INTEGER,
                    UF                        TEXT,
                    NOME_EMPRESA              TEXT,
                    COBERTURA                 TEXT,
                    QT_BENEFICIARIO_ATIVO     INTEGER,
                    QT_BENEFICIARIO_ADERIDO   INTEGER,
                    QT_BENEFICIARIO_CANCELADO INTEGER,
                    QT_BENEFICIARIO_NET       INTEGER
    );
    ''')
    
def criartabelaB01(cur3):
    
    # Cria tabela de Dados
    cur3.executescript('''
    DROP TABLE IF EXISTS B01;
    CREATE TABLE B01 (
                    DATA                      INTEGER,
                    NOME_EMPRESA              TEXT,
                    COBERTURA                 TEXT,
                    NET_Adds                  INTEGER
    );
    ''')

def criartabelaB02(cur3):
    
    # Cria tabela de Dados
    cur3.executescript('''
    DROP TABLE IF EXISTS B02;
    CREATE TABLE B02 (
                    DATA                      INTEGER,
                    COBERTURA                 TEXT,
                    NET_Adds                  INTEGER
    );
    ''')

def criartabelaB03(cur3):
    
    # Cria tabela de Dados
    cur3.executescript('''
    DROP TABLE IF EXISTS B03;
    CREATE TABLE B03 (
                    DATA                      INTEGER, 
                    NOME_EMPRESA              TEXT,
                    COBERTURA                 TEXT,
                    QT_BENEFICIARIO_ATIVO     INTEGER,
                    NET_ADDS                  INTEGER,
                    MS_EVOLUTION_MES          TEXT
    );
    ''')

def criartabelaB04(cur3):
    
    # Cria tabela de Dados
    cur3.executescript('''
    DROP TABLE IF EXISTS B04;
    CREATE TABLE B04 (
                    DATA                      INTEGER, 
                    NOME_EMPRESA              TEXT,
                    COBERTURA                 TEXT,
                    QT_BENEFICIARIO_ATIVO     INTEGER,
                    NET_ADDS                  INTEGER,
                    MS_EVOLUTION_MES          TEXT
    );
    ''')

def criartabelaB05(cur3):
    
    # Cria tabela de Dados
    cur3.executescript('''
    DROP TABLE IF EXISTS B05;
    CREATE TABLE B05 (
                    DATA                      INTEGER, 
                    NOME_EMPRESA              TEXT,
                    COBERTURA                 TEXT,
                    QT_BENEFICIARIO_ATIVO     INTEGER,
                    NET_ADDS_MES              INTEGER,
                    MS_EVOLUTION_MES          TEXT,
                    NET_ADDS_ANO              INTEGER,
                    MS_EVOLUTION_ANO          TEXT
    );
    ''')

#-----------------------------------------------------------------------------#
#--------------------------# Inserir dados tabela  #--------------------------#

def  inserirtabelaA01(cur, rs):
        
    cur.execute('''INSERT INTO A01 (
                    DATA, 
                    NOME_EMPRESA,
                    COBERTURA,
                    QT_BENEFICIARIO_ATIVO)
        VALUES (?,?,?,?)
    ''', (rs[0],rs[1],rs[2],rs[3]))
    
def  inserirtabelaA02(cur, rs):
        
    cur.execute('''INSERT INTO A02 (
                    DATA, 
                    NOME_EMPRESA,
                    QT_BENEFICIARIO_ATIVO)
        VALUES (?,?,?)
    ''', (rs[0],rs[1],rs[2]))
    
def  inserirtabelaA03(cur, rs):
        
    cur.execute('''INSERT INTO A03 (
                    DATA, 
                    NOME_EMPRESA,
                    COBERTURA,
                    QT_BENEFICIARIO_ATIVO)
        VALUES (?,?,?,?)
    ''', (rs[0],rs[1],rs[2],rs[3]))
    
def  inserirtabelaA04(cur, rs):
        
    cur.execute('''INSERT INTO A04 (
                    DATA, 
                    NOME_EMPRESA,
                    COBERTURA,
                    QT_BENEFICIARIO_ATIVO)
        VALUES (?,?,?,?)
    ''', (rs[0],rs[1],rs[2],rs[3]))

def  inserirtabelaA05(cur, rs):
        
    cur.execute('''INSERT INTO A05 (
                    DATA,
                    UF,
                    NOME_EMPRESA,
                    COBERTURA,
                    QT_BENEFICIARIO_ATIVO,
                    QT_BENEFICIARIO_ADERIDO,
                    QT_BENEFICIARIO_CANCELADO,
                    QT_BENEFICIARIO_NET)
        VALUES (?,?,?,?,?,?,?,?)
    ''', (rs[0],rs[1],rs[2],rs[3],rs[4],rs[5],rs[6],rs[7]))

def  inserirtabelaB01(cur, rs):
        
    cur.execute('''INSERT INTO B01 (
                    DATA, 
                    NOME_EMPRESA,
                    COBERTURA,
                    NET_Adds)
        VALUES (?,?,?,?)
    ''', (rs[0],rs[1],rs[2],rs[3]))
    
def  inserirtabelaB02(cur, rs):
        
    cur.execute('''INSERT INTO B02 (
                    DATA,
                    COBERTURA,
                    NET_Adds)
        VALUES (?,?,?)
    ''', (rs[0],rs[1],rs[2]))

def  inserirtabelaB03(cur, rs):
        
    cur.execute('''INSERT INTO B03 (
                    DATA, 
                    NOME_EMPRESA,
                    COBERTURA,
                    QT_BENEFICIARIO_ATIVO,
                    NET_ADDS,
                    MS_EVOLUTION_MES)
        VALUES (?,?,?,?,?,?)
    ''', (rs[0],rs[1],rs[2],rs[3],rs[4],rs[5]))

def  inserirtabelaB04(cur, rs):
        
    cur.execute('''INSERT INTO B04 (
                    DATA, 
                    NOME_EMPRESA,
                    COBERTURA,
                    QT_BENEFICIARIO_ATIVO,
                    NET_ADDS,
                    MS_EVOLUTION_MES)
        VALUES (?,?,?,?,?,?)
    ''', (rs[0],rs[1],rs[2],rs[3],rs[4],rs[5]))

def  inserirtabelaB05(cur, rs):
        
    cur.execute('''INSERT INTO B05 (
                    DATA, 
                    NOME_EMPRESA,
                    COBERTURA,
                    QT_BENEFICIARIO_ATIVO,
                    NET_ADDS_MES,
                    MS_EVOLUTION_MES,
                    NET_ADDS_ANO,
                    MS_EVOLUTION_ANO)
        VALUES (?,?,?,?,?,?,?,?)
    ''', (rs[0],rs[1],rs[2],rs[3],rs[4],rs[5],rs[6],rs[7]))

#-----------------------------------------------------------------------------#
#----------------------------# Preencher tabela  #----------------------------#

def  preenchertabelaA01(cur2, cur3):

    aux = cur2.execute('''
    SELECT Data_mes.ID_CMPT_MOVEL as Data, Empresas_filtrado.NM_RAZAO_SOCIAL as Empresa, Cobertura_plano.COBERTURA_ASSIST_PLAN as Cobertura, sum(Dados.QT_BENEFICIARIO_ATIVO) as BenefAtivo
    FROM Empresas_filtrado, Data_mes, Cobertura_plano JOIN Dados ON Dados.id_CD_OPERADORA = Empresas_filtrado.ID AND Dados.id_ID_CMPT_MOVEL = Data_mes.ID AND Dados.id_COBERTURA_ASSIST_PLAN = Cobertura_plano.ID
    group by  Dados.id_ID_CMPT_MOVEL, Dados.id_CD_OPERADORA, Dados.id_COBERTURA_ASSIST_PLAN
    order by  Data_mes.ID_CMPT_MOVEL DESC, Dados.id_COBERTURA_ASSIST_PLAN, sum(Dados.QT_BENEFICIARIO_ATIVO) DESC, Empresas_filtrado.NM_RAZAO_SOCIAL
                       ''',)
    
    tableColumns = [description[0] for description in cur2.description]
    df = pd.DataFrame(aux.fetchall(),  columns=tableColumns)
    
    for i, row in df.iterrows():
        p = conversor_data(row.tolist()[0])
        inserirtabelaA01(cur3, (p, row.tolist()[1], row.tolist()[2], row.tolist()[3]))

def  preenchertabelaA02(cur2, cur3):

    aux = cur2.execute('''
    SELECT Data_mes.ID_CMPT_MOVEL as Data, Empresas_filtrado.NM_RAZAO_SOCIAL as Empresa, sum(Dados.QT_BENEFICIARIO_ATIVO) as BenefAtivo
    FROM Empresas_filtrado, Data_mes JOIN Dados ON Dados.id_CD_OPERADORA = Empresas_filtrado.ID AND Dados.id_ID_CMPT_MOVEL = Data_mes.ID
    group by  Dados.id_ID_CMPT_MOVEL, Dados.id_CD_OPERADORA
    order by  Data_mes.ID_CMPT_MOVEL DESC, sum(Dados.QT_BENEFICIARIO_ATIVO) DESC, Empresas_filtrado.NM_RAZAO_SOCIAL
                       ''',)
    
    tableColumns = [description[0] for description in cur2.description]
    df = pd.DataFrame(aux.fetchall(),  columns=tableColumns)
    
    for i, row in df.iterrows():
        p = conversor_data(row.tolist()[0])
        inserirtabelaA02(cur3, (p, row.tolist()[1], row.tolist()[2]))

def  preenchertabelaA03(cur2, cur3):

    aux = cur2.execute('''
    SELECT Data_mes.ID_CMPT_MOVEL as Data, Empresas_filtrado.NM_RAZAO_SOCIAL as Empresa, Cobertura_plano.COBERTURA_ASSIST_PLAN as Cobertura, sum(Dados.QT_BENEFICIARIO_ATIVO) as BenefAtivo
    FROM Empresas_filtrado, Data_mes, Cobertura_plano JOIN Dados ON Dados.id_CD_OPERADORA = Empresas_filtrado.ID AND Dados.id_ID_CMPT_MOVEL = Data_mes.ID AND Dados.id_COBERTURA_ASSIST_PLAN = Cobertura_plano.ID
    WHERE Cobertura_plano.COBERTURA_ASSIST_PLAN = 'Médico-hospitalar'
    group by  Dados.id_ID_CMPT_MOVEL, Dados.id_CD_OPERADORA, Dados.id_COBERTURA_ASSIST_PLAN
    order by  Data_mes.ID_CMPT_MOVEL DESC, Dados.id_COBERTURA_ASSIST_PLAN, sum(Dados.QT_BENEFICIARIO_ATIVO) DESC, Empresas_filtrado.NM_RAZAO_SOCIAL
                       ''',)
    
    tableColumns = [description[0] for description in cur2.description]
    df = pd.DataFrame(aux.fetchall(),  columns=tableColumns)
    
    for i, row in df.iterrows():
        p = conversor_data(row.tolist()[0])
        inserirtabelaA03(cur3, (p, row.tolist()[1], row.tolist()[2], row.tolist()[3]))
    
def  preenchertabelaA04(cur2, cur3):

    aux = cur2.execute('''
    SELECT Data_mes.ID_CMPT_MOVEL as Data, Empresas_filtrado.NM_RAZAO_SOCIAL as Empresa, Cobertura_plano.COBERTURA_ASSIST_PLAN as Cobertura, sum(Dados.QT_BENEFICIARIO_ATIVO) as BenefAtivo
    FROM Empresas_filtrado, Data_mes, Cobertura_plano JOIN Dados ON Dados.id_CD_OPERADORA = Empresas_filtrado.ID AND Dados.id_ID_CMPT_MOVEL = Data_mes.ID AND Dados.id_COBERTURA_ASSIST_PLAN = Cobertura_plano.ID
    WHERE Cobertura_plano.COBERTURA_ASSIST_PLAN = 'Odontológico'
    group by  Dados.id_ID_CMPT_MOVEL, Dados.id_CD_OPERADORA, Dados.id_COBERTURA_ASSIST_PLAN
    order by  Data_mes.ID_CMPT_MOVEL DESC, Dados.id_COBERTURA_ASSIST_PLAN, sum(Dados.QT_BENEFICIARIO_ATIVO) DESC, Empresas_filtrado.NM_RAZAO_SOCIAL
                       ''',)
    
    tableColumns = [description[0] for description in cur2.description]
    df = pd.DataFrame(aux.fetchall(),  columns=tableColumns)
    
    for i, row in df.iterrows():
        p = conversor_data(row.tolist()[0])
        inserirtabelaA04(cur3, (p, row.tolist()[1], row.tolist()[2], row.tolist()[3]))
    
def  preenchertabelaA05(cur2, cur3):

    aux = cur2.execute('''
    SELECT Data_mes.ID_CMPT_MOVEL as Data, Localidade.NM_MUNICIPIO as UF, Empresas_filtrado.NM_RAZAO_SOCIAL as Empresa, Cobertura_plano.COBERTURA_ASSIST_PLAN as Cobertura, sum(Dados.QT_BENEFICIARIO_ATIVO) as BenefAtivo, sum(Dados.QT_BENEFICIARIO_ADERIDO) as BenefAderido, sum(Dados.QT_BENEFICIARIO_CANCELADO) as BenefCancelado, sum(Dados.QT_BENEFICIARIO_ADERIDO - Dados.QT_BENEFICIARIO_CANCELADO) as BenfNet
    FROM Empresas_filtrado, Data_mes, Cobertura_plano, Localidade JOIN Dados ON Dados.id_CD_OPERADORA = Empresas_filtrado.ID AND Dados.id_ID_CMPT_MOVEL = Data_mes.ID AND Dados.id_COBERTURA_ASSIST_PLAN = Cobertura_plano.ID AND Dados.id_CD_MUNICIPIO = Localidade.ID
    group by  Dados.id_ID_CMPT_MOVEL, Dados.id_CD_OPERADORA, Dados.id_COBERTURA_ASSIST_PLAN, Localidade.NM_MUNICIPIO
    order by  Data_mes.ID_CMPT_MOVEL DESC, Empresas_filtrado.NM_RAZAO_SOCIAL, Localidade.NM_MUNICIPIO, Dados.id_COBERTURA_ASSIST_PLAN, sum(Dados.QT_BENEFICIARIO_ATIVO) DESC, Empresas_filtrado.NM_RAZAO_SOCIAL
                           ''',)
    
    tableColumns = [description[0] for description in cur2.description]
    df = pd.DataFrame(aux.fetchall(),  columns=tableColumns)
    
    for i, row in df.iterrows():
        p = conversor_data(row.tolist()[0])
        inserirtabelaA05(cur3, (p, row.tolist()[1], row.tolist()[2], row.tolist()[3], row.tolist()[4], row.tolist()[5], row.tolist()[6], row.tolist()[7]))   
    
def  preenchertabelaB01(cur2, cur3):

    aux = cur2.execute('''
    SELECT Data_mes.ID_CMPT_MOVEL as Data, Empresas_filtrado.NM_RAZAO_SOCIAL as EMPRESA, Cobertura_plano.COBERTURA_ASSIST_PLAN as Cobertura, sum(Dados.QT_BENEFICIARIO_ADERIDO - Dados.QT_BENEFICIARIO_CANCELADO) as NETAdds
    FROM Empresas_filtrado, Data_mes, Cobertura_plano JOIN Dados ON Dados.id_CD_OPERADORA = Empresas_filtrado.ID AND Dados.id_ID_CMPT_MOVEL = Data_mes.ID AND Dados.id_COBERTURA_ASSIST_PLAN = Cobertura_plano.ID
    group by  Dados.id_ID_CMPT_MOVEL, Dados.id_CD_OPERADORA, Dados.id_COBERTURA_ASSIST_PLAN
    order by  Data_mes.ID_CMPT_MOVEL DESC, Dados.id_COBERTURA_ASSIST_PLAN, sum(Dados.QT_BENEFICIARIO_ADERIDO - Dados.QT_BENEFICIARIO_CANCELADO) DESC, Empresas_filtrado.NM_RAZAO_SOCIAL
                       ''',)
    
    tableColumns = [description[0] for description in cur2.description]
    df = pd.DataFrame(aux.fetchall(),  columns=tableColumns)
    
    for i, row in df.iterrows():
        p = conversor_data(row.tolist()[0])
        inserirtabelaB01(cur3, (p, row.tolist()[1], row.tolist()[2], row.tolist()[3]))   
    
def  preenchertabelaB02(cur2, cur3):

    aux = cur2.execute('''
	SELECT Data_mes.ID_CMPT_MOVEL as Data, Cobertura_plano.COBERTURA_ASSIST_PLAN as Cobertura, sum(Dados.QT_BENEFICIARIO_ADERIDO - Dados.QT_BENEFICIARIO_CANCELADO) as NETAdds
	FROM  Data_mes, Cobertura_plano JOIN Dados ON  Dados.id_ID_CMPT_MOVEL = Data_mes.ID AND Dados.id_COBERTURA_ASSIST_PLAN = Cobertura_plano.ID
	group by  Dados.id_ID_CMPT_MOVEL, Dados.id_COBERTURA_ASSIST_PLAN
	order by  Data_mes.ID_CMPT_MOVEL DESC, Dados.id_COBERTURA_ASSIST_PLAN, sum(Dados.QT_BENEFICIARIO_ADERIDO - Dados.QT_BENEFICIARIO_CANCELADO) DESC
                       ''',)
    
    tableColumns = [description[0] for description in cur2.description]
    df = pd.DataFrame(aux.fetchall(),  columns=tableColumns)
    
    for i, row in df.iterrows():
        p = conversor_data(row.tolist()[0])
        inserirtabelaB02(cur3, (p, row.tolist()[1], row.tolist()[2]))   
    
def  preenchertabelaB03(cur2, cur3):

    aux = cur2.execute('''
    SELECT Data_mes.ID_CMPT_MOVEL as Data, Empresas_filtrado.NM_RAZAO_SOCIAL as Empresa, Cobertura_plano.COBERTURA_ASSIST_PLAN as Cobertura, sum(Dados.QT_BENEFICIARIO_ATIVO) as BenefAtivo, sum(Dados.QT_BENEFICIARIO_ADERIDO-Dados.QT_BENEFICIARIO_CANCELADO) as NetAdds
    FROM Empresas_filtrado, Data_mes, Cobertura_plano JOIN Dados ON Dados.id_CD_OPERADORA = Empresas_filtrado.ID AND Dados.id_ID_CMPT_MOVEL = Data_mes.ID AND Dados.id_COBERTURA_ASSIST_PLAN = Cobertura_plano.ID
    group by  Dados.id_ID_CMPT_MOVEL, Dados.id_CD_OPERADORA, Dados.id_COBERTURA_ASSIST_PLAN
    order by  Data_mes.ID_CMPT_MOVEL DESC, Dados.id_COBERTURA_ASSIST_PLAN, sum(Dados.QT_BENEFICIARIO_ATIVO) DESC, Empresas_filtrado.NM_RAZAO_SOCIAL
                       ''',)
    
    tableColumns = [description[0] for description in cur2.description]
    df = pd.DataFrame(aux.fetchall(),  columns=tableColumns)
    
    for i, row in df.iterrows():
        p = conversor_data(row.tolist()[0])
        ativo = row.tolist()[3]
        net = row.tolist()[4]
        if (ativo-net) != 0: 
            mke = str(round(100*(net)/(ativo-net), 2)) + '%'
        else:
            mke = 'não tem'
        inserirtabelaB03(cur3, (p, row.tolist()[1], row.tolist()[2], row.tolist()[3], row.tolist()[4], mke))        

def  preenchertabelaB04(cur2, cur3):

    aux = cur2.execute('''
    SELECT Data_mes.ID_CMPT_MOVEL as Data, Empresas_filtrado.GRUPO as Empresa, Cobertura_plano.COBERTURA_ASSIST_PLAN as Cobertura, sum(Dados.QT_BENEFICIARIO_ATIVO) as BenefAtivo, sum(Dados.QT_BENEFICIARIO_ADERIDO-Dados.QT_BENEFICIARIO_CANCELADO) as NetAdds
    FROM Empresas_filtrado, Data_mes, Cobertura_plano JOIN Dados ON Dados.id_CD_OPERADORA = Empresas_filtrado.ID AND Dados.id_ID_CMPT_MOVEL = Data_mes.ID AND Dados.id_COBERTURA_ASSIST_PLAN = Cobertura_plano.ID
	WHERE Data_mes.ID_CMPT_MOVEL >= Empresas_filtrado.Data
	group by  Dados.id_ID_CMPT_MOVEL, Empresas_filtrado.GRUPO, Dados.id_COBERTURA_ASSIST_PLAN
    order by  Data_mes.ID_CMPT_MOVEL DESC, Dados.id_COBERTURA_ASSIST_PLAN, sum(Dados.QT_BENEFICIARIO_ATIVO) DESC, Empresas_filtrado.NM_RAZAO_SOCIAL
                       ''',)
    
    tableColumns = [description[0] for description in cur2.description]
    df = pd.DataFrame(aux.fetchall(),  columns=tableColumns)
    
    for i, row in df.iterrows():
        p = conversor_data(row.tolist()[0])
        ativo = row.tolist()[3]
        net = row.tolist()[4]
        if (ativo-net) != 0: 
            mke = str(round(100*(net)/(ativo-net), 2)) + '%'
        else:
            mke = 'não tem'
        inserirtabelaB04(cur3, (p, row.tolist()[1], row.tolist()[2], row.tolist()[3], row.tolist()[4], mke))
    
    
    aux = cur2.execute('''
    SELECT Data_mes.ID_CMPT_MOVEL as Data, Empresas_filtrado.NM_RAZAO_SOCIAL as Empresa, Cobertura_plano.COBERTURA_ASSIST_PLAN as Cobertura, sum(Dados.QT_BENEFICIARIO_ATIVO) as BenefAtivo, sum(Dados.QT_BENEFICIARIO_ADERIDO-Dados.QT_BENEFICIARIO_CANCELADO) as NetAdds
    FROM Empresas_filtrado, Data_mes, Cobertura_plano JOIN Dados ON Dados.id_CD_OPERADORA = Empresas_filtrado.ID AND Dados.id_ID_CMPT_MOVEL = Data_mes.ID AND Dados.id_COBERTURA_ASSIST_PLAN = Cobertura_plano.ID
	WHERE Data_mes.ID_CMPT_MOVEL < Empresas_filtrado.Data
	group by  Dados.id_ID_CMPT_MOVEL, Empresas_filtrado.NM_RAZAO_SOCIAL, Dados.id_COBERTURA_ASSIST_PLAN
    order by  Data_mes.ID_CMPT_MOVEL DESC, Dados.id_COBERTURA_ASSIST_PLAN, sum(Dados.QT_BENEFICIARIO_ATIVO) DESC, Empresas_filtrado.NM_RAZAO_SOCIAL
                       ''',)
    
    tableColumns = [description[0] for description in cur2.description]
    df = pd.DataFrame(aux.fetchall(),  columns=tableColumns)
    
    for i, row in df.iterrows():
        p = conversor_data(row.tolist()[0])
        ativo = row.tolist()[3]
        net = row.tolist()[4]
        if (ativo-net) != 0: 
            mke = str(round(100*(net)/(ativo-net), 2)) + '%'
        else:
            mke = 'não tem'
        inserirtabelaB04(cur3, (p, row.tolist()[1], row.tolist()[2], row.tolist()[3], row.tolist()[4], mke))
    
    cur3.execute('''
             SELECT * FROM B04
             order by  B04.DATA DESC, B04.COBERTURA, B04.QT_BENEFICIARIO_ATIVO DESC
             ''')

def  preenchertabelaB05(cur2, cur3):

    aux = cur3.execute('''
             SELECT * FROM B04
             order by  B04.DATA DESC, B04.COBERTURA, B04.QT_BENEFICIARIO_ATIVO DESC
             ''')

    tableColumns = [description[0] for description in cur3.description]
    df = pd.DataFrame(aux.fetchall(),  columns=tableColumns)
    
    for i, row in df.iterrows():
        data = row.tolist()[0]
        nome = row.tolist()[1] #str
        cobertura = row.tolist()[2] #str
        ativo = row.tolist()[3] #int
        ano_atual = int(data[3:]) # int
        ano_anterior = ano_atual-1
        data_anterior = data[:3] + str(ano_anterior)
        # pegar ano anterior
        aux2 = cur3.execute('''
                            SELECT QT_BENEFICIARIO_ATIVO
                            FROM B04
                            WHERE NOME_EMPRESA = ? AND COBERTURA = ? AND DATA = ?
                            ''', (nome, cobertura, data_anterior) )
        tableColumns = [description[0] for description in cur3.description]
        df2 = pd.DataFrame(aux2.fetchall(),  columns=tableColumns)
        net_ano = 0
        mke_ano = str('não tem')
        for p, row2 in df2.iterrows():
            ativo_ano = row2.tolist()[0]
            net_ano = ativo - ativo_ano
            if (ativo-net_ano) != 0: 
                mke_ano = str(round(100*(net_ano)/(ativo_ano), 2)) + '%'
        inserirtabelaB05(cur3, (row.tolist()[0], row.tolist()[1], row.tolist()[2], row.tolist()[3], row.tolist()[4], row.tolist()[5], net_ano, mke_ano))  
        


#
    
#     SELECT Data_mes.ID_CMPT_MOVEL as Data, Empresas.CD_OPERADORA, Empresas.NM_RAZAO_SOCIAL as Empresa, Localidade.SG_UF as UF, sum(Dados.QT_BENEFICIARIO_ATIVO) as BenefAtivo, sum(Dados.QT_BENEFICIARIO_ADERIDO) as BenefAderido, sum(Dados.QT_BENEFICIARIO_CANCELADO) as BenefCancelado, sum(Dados.QT_BENEFICIARIO_ADERIDO - Dados.QT_BENEFICIARIO_CANCELADO) as BenfNet
#     FROM Empresas, Data_mes, Localidade JOIN Dados ON Dados.id_CD_OPERADORA = Empresas.ID AND Dados.id_ID_CMPT_MOVEL = Data_mes.ID AND Dados.id_CD_MUNICIPIO = Localidade.ID
#     WHERE Data_mes.ID_CMPT_MOVEL = '202011' AND Empresas.CD_OPERADORA = '368253'
# 	group by  Dados.id_ID_CMPT_MOVEL, Dados.id_CD_OPERADORA, Localidade.SG_UF
#     order by  Data_mes.ID_CMPT_MOVEL DESC, Empresas.NM_RAZAO_SOCIAL, Localidade.SG_UF, sum(Dados.QT_BENEFICIARIO_ATIVO) DESC, Empresas.NM_RAZAO_SOCIAL

# SELECT Data_mes.ID_CMPT_MOVEL as Data, Empresas_filtrado.NM_RAZAO_SOCIAL as Empresa, Cobertura_plano.COBERTURA_ASSIST_PLAN as Cobertura, sum(Dados.QT_BENEFICIARIO_ATIVO) as BenefAtivo, sum(Dados.QT_BENEFICIARIO_ADERIDO-Dados.QT_BENEFICIARIO_CANCELADO) as NetAdds
# FROM Empresas_filtrado, Data_mes, Cobertura_plano JOIN Dados ON Dados.id_CD_OPERADORA = Empresas_filtrado.ID AND Dados.id_ID_CMPT_MOVEL = Data_mes.ID AND Dados.id_COBERTURA_ASSIST_PLAN = Cobertura_plano.ID
# group by  Dados.id_ID_CMPT_MOVEL, Dados.id_CD_OPERADORA, Dados.id_COBERTURA_ASSIST_PLAN
# order by  Data_mes.ID_CMPT_MOVEL DESC, Dados.id_COBERTURA_ASSIST_PLAN, sum(Dados.QT_BENEFICIARIO_ATIVO) DESC, Empresas_filtrado.NM_RAZAO_SOCIAL
    
