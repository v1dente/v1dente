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
    
    # Cria tabela com RPS_CADOP
    cur.executescript('''
    DROP TABLE IF EXISTS RPS_CADOP;
    CREATE TABLE RPS_CADOP (
                ID_PLANO                 INTEGER,
                CD_PLANO                 TEXT,
                CD_OPERADORA             INTEGER,
                RAZAO_SOCIAL             TEXT,
                GR_MODALIDADE            TEXT,
                PORTE_OPERADORA          TEXT,
                VIGENCIA_PLANO           TEXT,
                GR_CONTRATACAO           TEXT,
                GR_SGMT_ASSISTENCIAL     TEXT,
                LG_ODONTOLOGICO          NUM,
                OBSTETRICIA              TEXT,
                COBERTURA                TEXT,
                TIPO_FINANCIAMENTO       TEXT,
                ABRANGENCIA_COBERTURA    TEXT,
                FATOR_MODERADOR          TEXT,
                ACOMODACAO_HOSPITALAR    TEXT,
                LIVRE_ESCOLHA            TEXT
    );
    ''')    
    
    # Cria tabela com NT_VC
    cur.executescript('''
    DROP TABLE IF EXISTS NT_VC;
    CREATE TABLE NT_VC (
                CD_NOTA                  INTEGER,
                CD_FAIXA_ETARIA          INTEGER,
                FAIXA_ETARIA             TEXT,
                CARREG_TOTAL             REAL,
                CARREG_ADM               REAL,
                CARREG_COM               REAL,
                CARREG_LUCRO             REAL,
                VL_COML_MENL             REAL,
                VL_COML_MIN              REAL,
                VL_COML_MAX              REAL
    );
    ''')   
    
    # Cria tabela com NT_VC_MES
    cur.executescript('''
    DROP TABLE IF EXISTS NT_VC_MES;
    CREATE TABLE NT_VC_MES (
                ID_PLANO                 INTEGER,
                CD_NOTA                  INTEGER,
                ANO_MES                  INTEGER,
                LG_OUTLIER               INTEGER
    );
    ''')
    
    # Cria tabela com caracteristicas_produtos_saude_suplementar
    cur.executescript('''
    DROP TABLE IF EXISTS SAUDE_SUPLEMENTAR;
    CREATE TABLE SAUDE_SUPLEMENTAR (
                ID_PLANO                 INTEGER,
                CD_PLANO                 TEXT,
                NM_PLANO                 TEXT,
                CD_OPERADORA             INTEGER,
                RAZAO_SOCIAL             TEXT,
                GR_MODALIDADE            TEXT,
                PORTE_OPERADORA          TEXT,
                VIGENCIA_PLANO           TEXT,
                CONTRATACAO              TEXT,
                GR_CONTRATACAO           TEXT,
                SGMT_ASSISTENCIAL        TEXT,
                GR_SGMT_ASSISTENCIAL     TEXT,
                LG_ODONTOLOGICO          INTEGER,
                OBSTRETICIA              TEXT,
                COBERTURA                TEXT,
                TIPO_FINANCIAMENTO       TEXT,
                ABRANGENCIA_COBERTURA    TEXT,
                FATOR_MODERADOR          TEXT,
                ACOMODACAO_HOSPITALAR    TEXT,
                LIVRE_ESCOLHA            TEXT,
                SITUACAO_PLANO           TEXT,
                DT_SITUACAO              TEXT,
                DT_REGISTRO_PLANO        TEXT,
                DT_ATUALIZACAO           TEXT
    );
    ''')
    
    cur.executescript('''
    DROP TABLE IF EXISTS NOME_PLANO;
    CREATE TABLE NOME_PLANO (
                ID_PLANO                 INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT NOT NULL UNIQUE,
                CD_PLANO                 INTEGER UNIQUE,
                NM_PLANO                 TEXT,
                CD_OPERADORA             INTEGER,
                CLUSTER                  TEXT
    );
    ''')
    
    # Cria tabela de ben202101
    cur.executescript('''
    DROP TABLE IF EXISTS ben202101;
    CREATE TABLE ben202101 (
                    ID_CMPT_MOVEL             INTEGER, 
                    CD_OPERADORA              INTEGER,
                    NM_RAZAO_SOCIAL           TEXT,
                    NR_CNPJ                   INTEGER,
                    MODALIDADE_OPERADORA      TEXT,
                    SG_UF                     TEXT,
                    CD_MUNICIPIO              INTEGER,
                    NM_MUNICIPIO              TEXT,
                    TP_SEXO                   TEXT,
                    DE_FAIXA_ETARIA           TEXT, 
                    DE_FAIXA_ETARIA_REAJ      TEXT,
                    CD_PLANO                  INTEGER,
                    TP_VIGENCIA_PLANO         TEXT,
                    DE_CONTRATACAO_PLANO      TEXT,
                    DE_SEGMENTACAO_PLANO      TEXT,
                    DE_ABRG_GEOGRAFICA_PLANO  TEXT,
                    COBERTURA_ASSIST_PLAN     TEXT,
                    TIPO_VINCULO              TEXT,
                    QT_BENEFICIARIO_ATIVO        INTEGER, 
                    QT_BENEFICIARIO_ADERIDO      INTEGER,
                    QT_BENEFICIARIO_CANCELADO    INTEGER,
                    DT_CARGA                  TEXT
    );
    ''')
    
    # Cria tabela de ben202101_ajustado
    cur.executescript('''
    DROP TABLE IF EXISTS ben202101_ajustado;
    CREATE TABLE ben202101_ajustado (
                    ID_CMPT_MOVEL             INTEGER, 
                    CD_OPERADORA              INTEGER,
                    NM_RAZAO_SOCIAL           TEXT,
                    DE_FAIXA_ETARIA_REAJ      TEXT,
                    COBERTURA_ASSIST_PLAN     TEXT,
                    DE_CONTRATACAO_PLANO      TEXT,
                    CD_PLANO                  INTEGER,
                    QT_BENEFICIARIO_ATIVO     INTEGER, 
                    QT_NET_ADDS               INTEGER
    );
    ''')
    
    # Cria tabela de Sula -> beneficiários por plano
    cur.executescript('''
    DROP TABLE IF EXISTS sula_planos;
    CREATE TABLE sula_planos (
                    ID_CMPT_MOVEL             INTEGER, 
                    CD_OPERADORA              INTEGER,
                    NM_RAZAO_SOCIAL           TEXT,
                    DE_FAIXA_ETARIA_REAJ      TEXT,
                    COBERTURA_ASSIST_PLAN     TEXT,
                    DE_CONTRATACAO_PLANO      TEXT,
                    NM_PLANO                  TEXT,
                    CLUSTER                   TEXT,
                    QT_BENEFICIARIO_ATIVO     INTEGER, 
                    QT_NET_ADDS               INTEGER
    );
    ''')
    
    # Cria tabela de Cluster -> beneficiários por plano
    cur.executescript('''
    DROP TABLE IF EXISTS cluster_planos;
    CREATE TABLE cluster_planos (
                    ID_CMPT_MOVEL             INTEGER,
                    NM_RAZAO_SOCIAL           TEXT,
                    DE_FAIXA_ETARIA_REAJ      TEXT,
                    COBERTURA_ASSIST_PLAN     TEXT,
                    DE_CONTRATACAO_PLANO      TEXT,
                    NM_PLANO                  TEXT,
                    QT_BENEFICIARIO_ATIVO     INTEGER, 
                    QT_NET_ADDS               INTEGER
    );
    ''')
    
######################## tranformar em tabelas de inteiro (atentar para repetições)

#-----------------------------------------------------------------------------#
#------------------------------# FUNÇÕES AQUI  #------------------------------#


# Insere chave id+nome da tabela RPS_CADOP
def insertRPS_CADOP(cur, rs):
    cur.execute('''INSERT OR IGNORE INTO RPS_CADOP (
                ID_PLANO,
                CD_PLANO,
                CD_OPERADORA,
                RAZAO_SOCIAL,
                GR_MODALIDADE,
                PORTE_OPERADORA,
                VIGENCIA_PLANO,
                GR_CONTRATACAO,
                GR_SGMT_ASSISTENCIAL,
                LG_ODONTOLOGICO,
                OBSTETRICIA,
                COBERTURA,
                TIPO_FINANCIAMENTO,
                ABRANGENCIA_COBERTURA,
                FATOR_MODERADOR,
                ACOMODACAO_HOSPITALAR,
                LIVRE_ESCOLHA)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    ''', (rs))


# Insere chave id+nome da tabela NT_VC
def insertNT_VC(cur, rs):
    cur.execute('''INSERT OR IGNORE INTO NT_VC (
                CD_NOTA,
                CD_FAIXA_ETARIA,
                FAIXA_ETARIA,
                CARREG_TOTAL,
                CARREG_ADM,
                CARREG_COM,
                CARREG_LUCRO,
                VL_COML_MENL,
                VL_COML_MIN,
                VL_COML_MAX)
        VALUES (?,?,?,?,?,?,?,?,?,?)
    ''', (rs))


# Insere chave id+nome da tabela NT_VC_MES
def insertNT_VC_MES(cur, rs):
    cur.execute('''INSERT OR IGNORE INTO NT_VC_MES (
                ID_PLANO,
                CD_NOTA,
                ANO_MES,
                LG_OUTLIER)
        VALUES (?,?,?,?)
    ''', (rs))
    
# Insere chave id+nome da tabela SAUDE_SUPLEMENTAR
def insertSAUDE_SUPLEMENTAR(cur, rs):
    cur.execute('''INSERT OR IGNORE INTO SAUDE_SUPLEMENTAR (
                ID_PLANO,
                CD_PLANO,
                NM_PLANO,
                CD_OPERADORA,
                RAZAO_SOCIAL,
                GR_MODALIDADE,
                PORTE_OPERADORA,
                VIGENCIA_PLANO,
                CONTRATACAO,
                GR_CONTRATACAO,
                SGMT_ASSISTENCIAL,
                GR_SGMT_ASSISTENCIAL,
                LG_ODONTOLOGICO,
                OBSTRETICIA,
                COBERTURA,
                TIPO_FINANCIAMENTO,
                ABRANGENCIA_COBERTURA,
                FATOR_MODERADOR,
                ACOMODACAO_HOSPITALAR,
                LIVRE_ESCOLHA,
                SITUACAO_PLANO,
                DT_SITUACAO,
                DT_REGISTRO_PLANO,
                DT_ATUALIZACAO)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    ''', (rs))

# Insere chave id+nome da tabela NOME_PLANO
def insertNOME_PLANO(cur, rs):
    cur.execute('''INSERT OR IGNORE INTO NOME_PLANO (
                CD_PLANO,
                NM_PLANO,
                CD_OPERADORA,
                CLUSTER)
        VALUES (?,?,?,?)
    ''', (rs))

# Insere chave id+nome da tabela ben202101
def insertben202101(cur, rs):
    cur.execute('''INSERT OR IGNORE INTO ben202101 (
                    ID_CMPT_MOVEL, 
                    CD_OPERADORA,
                    NM_RAZAO_SOCIAL,
                    NR_CNPJ,
                    MODALIDADE_OPERADORA,
                    SG_UF,
                    CD_MUNICIPIO,
                    NM_MUNICIPIO,
                    TP_SEXO,
                    DE_FAIXA_ETARIA, 
                    DE_FAIXA_ETARIA_REAJ,
                    CD_PLANO,
                    TP_VIGENCIA_PLANO,
                    DE_CONTRATACAO_PLANO,
                    DE_SEGMENTACAO_PLANO,
                    DE_ABRG_GEOGRAFICA_PLANO,
                    COBERTURA_ASSIST_PLAN,
                    TIPO_VINCULO,
                    QT_BENEFICIARIO_ATIVO, 
                    QT_BENEFICIARIO_ADERIDO,
                    QT_BENEFICIARIO_CANCELADO,
                    DT_CARGA)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    ''', (rs))
    
# Insere chave id+nome da tabela ben202101_ajustado
def insertben202101_ajustado(cur, rs):
    cur.execute('''INSERT OR IGNORE INTO ben202101_ajustado (
                    ID_CMPT_MOVEL, 
                    CD_OPERADORA,
                    NM_RAZAO_SOCIAL,
                    DE_FAIXA_ETARIA_REAJ,
                    COBERTURA_ASSIST_PLAN,
                    DE_CONTRATACAO_PLANO,
                    CD_PLANO,
                    QT_BENEFICIARIO_ATIVO, 
                    QT_NET_ADDS)
        VALUES (?,?,?,?,?,?,?,?,?)
    ''', (rs))
    
# Insere chave id+nome da tabela sula_planos
def insertsula_planos(cur, rs):
    cur.execute('''INSERT OR IGNORE INTO sula_planos (
                    ID_CMPT_MOVEL, 
                    CD_OPERADORA,
                    NM_RAZAO_SOCIAL,
                    DE_FAIXA_ETARIA_REAJ,
                    COBERTURA_ASSIST_PLAN,
                    DE_CONTRATACAO_PLANO,
                    NM_PLANO,
                    CLUSTER,
                    QT_BENEFICIARIO_ATIVO, 
                    QT_NET_ADDS)
        VALUES (?,?,?,?,?,?,?,?,?,?)
    ''', (rs))
    
def insertcluster_planos(cur, rs):
    cur.execute('''INSERT OR IGNORE INTO cluster_planos (
                    ID_CMPT_MOVEL, 
                    NM_RAZAO_SOCIAL,
                    DE_FAIXA_ETARIA_REAJ,
                    COBERTURA_ASSIST_PLAN,
                    DE_CONTRATACAO_PLANO,
                    NM_PLANO,
                    QT_BENEFICIARIO_ATIVO, 
                    QT_NET_ADDS)
        VALUES (?,?,?,?,?,?,?,?)
    ''', (rs))
    
      
    
    
    
    
    
    