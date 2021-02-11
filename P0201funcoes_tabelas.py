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
#------------------------------# Cria Tabelas  #------------------------------#
# Create datatable structures
def createDBTable(cur):
    
    # Cria tabela com empresas
    cur.executescript('''
    DROP TABLE IF EXISTS Empresas;
    CREATE TABLE Empresas (
                ID                  INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT NOT NULL UNIQUE,
                CD_OPERADORA        INTEGER,
                NM_RAZAO_SOCIAL     TEXT UNIQUE,
                NR_CNPJ             INTEGER UNIQUE
    );
    ''')

    # Cria tabela com modalidade da operadora de plano
    cur.executescript('''
    DROP TABLE IF EXISTS Modalidade_plano;
    CREATE TABLE Modalidade_plano (
                ID                    INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT NOT NULL UNIQUE,
                MODALIDADE_OPERADORA  TEXT UNIQUE
    );
    ''')

    # Cria tabela com localidade
    cur.executescript('''
    DROP TABLE IF EXISTS Localidade;
    CREATE TABLE Localidade (
                ID                  INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT NOT NULL UNIQUE,
                SG_UF               TEXT,
                CD_MUNICIPIO        INTEGER UNIQUE,
                NM_MUNICIPIO        TEXT
    );
    ''')

    # Cria tabela com TP_sex
    cur.executescript('''
    DROP TABLE IF EXISTS TP_sex;
    CREATE TABLE TP_sex (
                ID                  INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT NOT NULL UNIQUE,
                TP_SEXO                   TEXT UNIQUE
    );
    ''')

    # Cria tabela com TP_plano
    cur.executescript('''
    DROP TABLE IF EXISTS TP_plano;
    CREATE TABLE TP_plano (
                ID                  INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT NOT NULL UNIQUE,
                TP_VIGENCIA_PLANO         TEXT UNIQUE
    );
    ''')

    # Cria tabela com Faixa_etaria
    cur.executescript('''
    DROP TABLE IF EXISTS Faixa_etaria;
    CREATE TABLE Faixa_etaria (
                ID                  INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT NOT NULL UNIQUE,
                DE_FAIXA_ETARIA           TEXT UNIQUE
    );
    ''')
    
        # Cria tabela com Real_faixa
    cur.executescript('''
    DROP TABLE IF EXISTS Real_faixa;
    CREATE TABLE Real_faixa (
                ID                  INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT NOT NULL UNIQUE,
                DE_FAIXA_ETARIA_REAJ      TEXT UNIQUE
    );
    ''')

    # Cria tabela com CD de plano
    cur.executescript('''
    DROP TABLE IF EXISTS Cd_plano;
    CREATE TABLE Cd_plano (
                ID                    INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT NOT NULL UNIQUE,
                CD_PLANO              INTEGER UNIQUE
    );
    ''')


    # Cria tabela com contratação de plano
    cur.executescript('''
    DROP TABLE IF EXISTS Cont_plano;
    CREATE TABLE Cont_plano (
                ID                    INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT NOT NULL UNIQUE,
                DE_CONTRATACAO_PLANO  TEXT UNIQUE
    );
    ''')

    # Cria tabela com segmentação de plano
    cur.executescript('''
    DROP TABLE IF EXISTS Seg_plano;
    CREATE TABLE Seg_plano (
                ID                    INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT NOT NULL UNIQUE,
                DE_SEGMENTACAO_PLANO  TEXT UNIQUE
    );
    ''')

    # Cria tabela com abrangencia geografica de plano
    cur.executescript('''
    DROP TABLE IF EXISTS Abgr_plano;
    CREATE TABLE Abgr_plano (
                ID                        INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT NOT NULL UNIQUE,
                DE_ABRG_GEOGRAFICA_PLANO  TEXT UNIQUE
    );
    ''')

    # Cria tabela com cobertura de assistencia de plano
    cur.executescript('''
    DROP TABLE IF EXISTS Cobertura_plano;
    CREATE TABLE Cobertura_plano (
                ID                        INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT NOT NULL UNIQUE,
                COBERTURA_ASSIST_PLAN     TEXT UNIQUE
    );
    ''')

    # Cria tabela com tipo de vinculo de plano
    cur.executescript('''
    DROP TABLE IF EXISTS Vinculo_plano;
    CREATE TABLE Vinculo_plano (
                ID                        INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT NOT NULL UNIQUE,
                TIPO_VINCULO              TEXT UNIQUE
    );
    ''')

    # Cria tabela com datas
    cur.executescript('''
    DROP TABLE IF EXISTS Data;
    CREATE TABLE Data (
                ID                  INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT NOT NULL UNIQUE,
                DT_CARGA            TEXT UNIQUE
    );
    ''')

    # Cria tabela com datas
    cur.executescript('''
    DROP TABLE IF EXISTS Data_mes;
    CREATE TABLE Data_mes (
                ID                  INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT NOT NULL UNIQUE,
                ID_CMPT_MOVEL             INTEGER UNIQUE
    );
    ''')

    # Cria tabela de Dados
    cur.executescript('''
    DROP TABLE IF EXISTS Dados;
    CREATE TABLE Dados (
                    id_ID_CMPT_MOVEL             INTEGER, 
                    id_CD_OPERADORA              INTEGER, 
                    id_MODALIDADE_OPERADORA      INTEGER,
                    id_CD_MUNICIPIO              INTEGER,
                    id_DE_FAIXA_ETARIA           INTEGER, 
                    id_DE_FAIXA_ETARIA_REAJ      INTEGER,
                    id_DE_CONTRATACAO_PLANO      INTEGER,
                    id_DE_SEGMENTACAO_PLANO      INTEGER,
                    id_COBERTURA_ASSIST_PLAN     INTEGER,
                    QT_BENEFICIARIO_ATIVO        INTEGER, 
                    QT_BENEFICIARIO_ADERIDO      INTEGER,
                    QT_BENEFICIARIO_CANCELADO    INTEGER
    );
    ''')

######################## tranformar em tabelas de inteiro (atentar para repetições)

#-----------------------------------------------------------------------------#
#------------------------------# FUNÇÕES AQUI  #------------------------------#

# # Insere dados da planilha na tabela Dados
# def insertDados(cur, rs):
#     cur.execute('''INSERT INTO Dados (
#                     ID_CMPT_MOVEL, 
#                     CD_OPERADORA, 
#                     NM_RAZAO_SOCIAL_id,
#                     NR_CNPJ, 
#                     MODALIDADE_OPERADORA, 
#                     SG_UF, 
#                     CD_MUNICIPIO,
#                     NM_MUNICIPIO, 
#                     TP_SEXO, 
#                     DE_FAIXA_ETARIA, 
#                     DE_FAIXA_ETARIA_REAJ,
#                     CD_PLANO, 
#                     TP_VIGENCIA_PLANO, 
#                     DE_CONTRATACAO_PLANO,
#                     DE_SEGMENTACAO_PLANO, 
#                     DE_ABRG_GEOGRAFICA_PLANO, 
#                     COBERTURA_ASSIST_PLAN,
#                     TIPO_VINCULO, 
#                     QT_BENEFICIARIO_ATIVO, 
#                     QT_BENEFICIARIO_ADERIDO,
#                     QT_BENEFICIARIO_CANCELADO, 
#                     DT_CARGA)
#         VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
#     ''', (rs[0],rs[1],rs[2],rs[3],rs[4],rs[5],rs[6],rs[7],rs[8],rs[9],rs[10],rs[11],rs[12],rs[13],rs[14],rs[15],rs[16],rs[17],rs[18],rs[19],rs[20],rs[21]))

# Insere chave id+nome da tabela Empresas
def insertEmpresa(cur, rs):
    cur.execute('''INSERT OR IGNORE INTO Empresas (
        CD_OPERADORA,
        NM_RAZAO_SOCIAL,
        NR_CNPJ)
        VALUES (?,?,?)
    ''', (rs[0],rs[1],rs[2]))

# Insere chave para modalidade de operadora de plano da tabela Modalidade_plano
def insertModalidade_plano(cur, rs):
    cur.execute('''INSERT OR IGNORE INTO Modalidade_plano (
        MODALIDADE_OPERADORA)
        VALUES (?)
    ''', (rs))

# Insere chave para CEP da tabela Localidade
def insertLocalidade(cur, rs):
    cur.execute('''INSERT OR IGNORE INTO Localidade (
        SG_UF, 
        CD_MUNICIPIO,
        NM_MUNICIPIO)
        VALUES (?,?,?)
    ''', (rs[0],rs[1],rs[2]))
    
# Insere chave para etaria da tabela TP_sex
def insertTP_sex(cur, rs):
    cur.execute('''INSERT OR IGNORE INTO TP_sex (
        TP_SEXO)
        VALUES (?)
    ''', (rs))
    
# Insere chave para etaria da tabela TP_plano
def insertTP_plano(cur, rs):
    cur.execute('''INSERT OR IGNORE INTO TP_plano (
        TP_VIGENCIA_PLANO)
        VALUES (?)
    ''', (rs))

# Insere chave para etaria da tabela Faixa_etaria
def insertFaixa_etaria(cur, rs):
    cur.execute('''INSERT OR IGNORE INTO Faixa_etaria (
        DE_FAIXA_ETARIA)
        VALUES (?)
    ''', (rs))

# Insere chave para etaria da tabela Faixa_etaria_real
def insertReal_faixa(cur, rs):
    cur.execute('''INSERT OR IGNORE INTO Real_faixa (
        DE_FAIXA_ETARIA_REAJ)
        VALUES (?)
    ''', (rs))
    
# Insere chave para contratação de plano da tabela Cd_plano
def insertCd_plano(cur, rs):
    cur.execute('''INSERT OR IGNORE INTO Cd_plano (
        CD_PLANO)
        VALUES (?)
    ''', (rs))

# Insere chave para contratação de plano da tabela Cont_plano
def insertCont_plano(cur, rs):
    cur.execute('''INSERT OR IGNORE INTO Cont_plano (
        DE_CONTRATACAO_PLANO)
        VALUES (?)
    ''', (rs))
    
# Insere chave para segmentação de plano da tabela Seg_plano
def insertSeg_plano(cur, rs):
    cur.execute('''INSERT OR IGNORE INTO Seg_plano (
        DE_SEGMENTACAO_PLANO)
        VALUES (?)
    ''', (rs))

# Insere chave para abrangencia geografica de plano da tabela Abgr_plano
def insertAbgr_plano(cur, rs):
    cur.execute('''INSERT OR IGNORE INTO Abgr_plano (
        DE_ABRG_GEOGRAFICA_PLANO)
        VALUES (?)
    ''', (rs))

# Insere chave para cobertura de assistencia de plano da tabela Cobertura_plano
def insertCobertura_plano(cur, rs):
    cur.execute('''INSERT OR IGNORE INTO Cobertura_plano (
        COBERTURA_ASSIST_PLAN)
        VALUES (?)
    ''', (rs))
    
# Insere chave para tipo de vinculo de plano da tabela Cobertura_plano
def insertVinculo_plano(cur, rs):
    cur.execute('''INSERT OR IGNORE INTO Vinculo_plano (
        TIPO_VINCULO)
        VALUES (?)
    ''', (rs))
    
# Insere chave para Data da tabela Data
def insertData(cur, rs):
    cur.execute('''INSERT OR IGNORE INTO Data (
        DT_CARGA)
        VALUES (?)
    ''', (rs))
    
# Insere chave para Data da tabela Data_mes
def insertData_mes(cur, rs):
    cur.execute('''INSERT OR IGNORE INTO Data_mes (
        ID_CMPT_MOVEL)
        VALUES (?)
    ''', (rs))
    
    


# Insere dados da planilha na tabela Dados
def insertDados(cur, rs):
    cur.execute('''INSERT INTO Dados (
                    id_ID_CMPT_MOVEL, 
                    id_CD_OPERADORA, 
                    id_MODALIDADE_OPERADORA, 
                    id_CD_MUNICIPIO,
                    id_DE_FAIXA_ETARIA,
                    id_DE_FAIXA_ETARIA_REAJ,
                    id_DE_CONTRATACAO_PLANO,
                    id_DE_SEGMENTACAO_PLANO,
                    id_COBERTURA_ASSIST_PLAN,
                    QT_BENEFICIARIO_ATIVO, 
                    QT_BENEFICIARIO_ADERIDO,
                    QT_BENEFICIARIO_CANCELADO)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
    ''', (rs[0],rs[1],rs[4],rs[6],rs[9],rs[10],rs[13],rs[14],rs[16],rs[18],rs[19],rs[20]))
    


    
    
    
    
    
    
    
    
    
    