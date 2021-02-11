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

def incluir_dados_planilha (cur, conn, excelFile, dicionario):
    excelFileName = excelFile
    try:
            fhand = open(excelFileName)
    except:
            print('Arquivo não encontrado')
            flag_error = 1
    
    fhand.readline()    # skip first row
    
    for row in fhand:
        rs = row.split(';')
        
        teste = str(rs[1])
        if teste in dicionario:
            
            # Insere nome das empresas planilha com a chave tabela Empresas
            aux = (str(rs[1]),str(rs[2]),rs[3])
            P0201funcoes_tabelas.insertEmpresa(cur, aux)
            # Insere modalidade da operadora planilha com a chave tabela Modalidade_plano
            aux = (str(rs[4]), )
            P0201funcoes_tabelas.insertModalidade_plano(cur, aux)
            # Insere o CEP planilha com a chave tabela Localidade
            aux = (str(rs[5]),str(rs[6]),str(rs[7]))
            P0201funcoes_tabelas.insertLocalidade(cur, aux)
            # Insere o etaria planilha com a chave tabela Faixa_etaria
            aux = (str(rs[9]), )
            P0201funcoes_tabelas.insertFaixa_etaria(cur, aux)
            # Insere o etaria planilha com a chave tabela Faixa_real
            aux = (str(rs[10]), )
            P0201funcoes_tabelas.insertReal_faixa(cur, aux)
            # Insere a contratação de plno com a chave tabela Cont_plano
            aux = (str(rs[13]), )
            P0201funcoes_tabelas.insertCont_plano(cur, aux)
            # Insere a segmentação de plno com a chave tabela Seg_plano
            aux = (str(rs[14]), )
            P0201funcoes_tabelas.insertSeg_plano(cur, aux)
            # Insere a cobertura de assistencia de plno com a chave tabela Cobertura_plano
            aux = (str(rs[16]), )
            P0201funcoes_tabelas.insertCobertura_plano(cur, aux)
            # Insere data da planilha com a chave tabela Data_mes
            aux = (str(rs[0]), )
            P0201funcoes_tabelas.insertData_mes(cur, aux)
            
            aux1 = rs
            # Pega a chave ID da planilha Empresas
            cur.execute('SELECT ID FROM Empresas WHERE NM_RAZAO_SOCIAL = (?)', (rs[2],))
            aux1[1] = cur.fetchone()[0] 
            aux1[2] = aux1[1]
            aux1[3] = aux1[1]
            # Pega a chave ID da planilha Modalidade_plano
            cur.execute('SELECT ID FROM Modalidade_plano WHERE MODALIDADE_OPERADORA = (?)', (rs[4],))
            aux1[4] = cur.fetchone()[0]
            # Pega a chave ID da planilha Localidade
            cur.execute('SELECT ID FROM Localidade WHERE CD_MUNICIPIO = (?) AND SG_UF = (?)', (rs[6],rs[5]))
            aux1[5] = cur.fetchone()[0]
            aux1[6] = aux1[5]
            aux1[7] = aux1[5]
            # Pega a chave ID da planilha Faixa_etaria
            cur.execute('SELECT ID FROM Faixa_etaria WHERE DE_FAIXA_ETARIA = (?) ', (rs[9],))
            aux1[9] = cur.fetchone()[0]
            # Pega a chave ID da planilha Real_faixa
            cur.execute('SELECT ID FROM Real_faixa WHERE DE_FAIXA_ETARIA_REAJ = (?) ', (rs[10],))
            aux1[10] = cur.fetchone()[0]    
            # Pega a chave ID da planilha Cont_plano
            cur.execute('SELECT ID FROM Cont_plano WHERE DE_CONTRATACAO_PLANO = (?)', (rs[13],))
            aux1[13] = cur.fetchone()[0]
            # Pega a chave ID da planilha Seg_plano
            cur.execute('SELECT ID FROM Seg_plano WHERE DE_SEGMENTACAO_PLANO = (?)', (rs[14],))
            aux1[14] = cur.fetchone()[0]
            # Pega a chave ID da planilha Cobertura_plano
            cur.execute('SELECT ID FROM Cobertura_plano WHERE COBERTURA_ASSIST_PLAN = (?)', (rs[16],))
            aux1[16] = cur.fetchone()[0]
            # Pega a chave ID da planilha Data_mes
            cur.execute('SELECT ID FROM Data_mes WHERE ID_CMPT_MOVEL = (?)', (rs[0],))
            aux1[0] = cur.fetchone()[0]
            
            # Insere os dados das planilha na tabela Dados já com as chaves
            P0201funcoes_tabelas.insertDados(cur, aux1)
        
        
