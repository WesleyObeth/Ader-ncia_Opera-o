#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Criado na quinta 04/03/2021

@author: wesleyhernandez
"""
# Pacote para mexer com os arquivos de Excel.
import pandas as pd

# Modulo para cronômetrar o programa.
import time

# Pacote para definir o caminho dos arquivos.
import os

# Começo do programa.
inicio = time.time()

# É definido o caminho da pasta de trabalho.
os.chdir("/Users/wesleyhernandez/Desktop/Especiais")
    
#=============================================================================#
#              IMPORTAÇÃO DOS ESTOQUES E SELEÇÃO DAS COLUNAS
#=============================================================================#

# Importação do estoque de ontem.
ontem = pd.read_excel("Carteiras_Especiais.xlsx", usecols = ["Pasta", "Data Entrada", "Tipo Solicitação", 
                                                             "Grupo", "Data_Limite", "Fase", "Responsável", 
                                                             "Carteira", "Nr Habilitação"])

# Importação do estoque de hoje.
hoje = pd.read_excel("Carteiras_Especiais_2.xlsx", usecols = ["Pasta", "Data Entrada", "Grupo", "Fase",
                                                              "Responsável", "Nr Habilitação"])

#=============================================================================#
#     FUNÇÃO "MERGE" DO PANDAS PARA FAZER "LEFT JOIN" DAS BASES
#=============================================================================#

# Renomeação das colunas dos DataFrames.
ontem.rename(columns = {"Grupo": "Grupo_Aloc", "Fase": "Fase_Aloc", 
                        "Responsável": "Responsável_Aloc"}, inplace = True)

hoje.rename(columns = {"Grupo": "Grupo_pos_plano", "Fase": "Fase_pos_plano", 
                       "Responsável": "Responsável_pos_plano"}, inplace = True)

# Aplicação da função "merge" para fazer o cruzamento "Left Join".
cruzamento = pd.merge(ontem, hoje, how = "left", on = ["Pasta", "Data Entrada", "Nr Habilitação"])

# Filtragem para pegar a fase de solicitação (WHERE).
baixas_solic_v1 = cruzamento[(cruzamento["Fase_Aloc"] == "Solicitação") & 
                             (cruzamento["Fase_pos_plano"] != "Solicitação")]

#=============================================================================#
#     CRIAÇÃO DE UM OBJETO PARA A "DATA_BAIXA" E ADIÇÃO DE NOVAS COLUNAS
#=============================================================================#

# Pacote para mexer com datas.
from datetime import datetime, timedelta, date
 
# Definição do dia do plano.
dia_plano = datetime.today() - timedelta(days=1)

# Definição do dia do plano no formato de string.
dia_plano_sql = dia_plano.strftime('%Y-%m-%d')
dia_plano = pd.to_datetime(dia_plano_sql)

# Adição de novas colunas.
baixas_solic_v1['Etapa'] = "Solicitação"
baixas_solic_v1['DATA_BAIXA'] = dia_plano

# Organização das colunas.
baixas_solic = baixas_solic_v1[['Pasta', 'Carteira', 'Tipo Solicitação', 'Fase_Aloc', 
                                'Grupo_Aloc', 'Grupo_pos_plano', 'Data Entrada', 
                                'Data_Limite', 'Responsável_Aloc', 'Etapa', 'DATA_BAIXA']]

# Eliminação das colunas com o comando "Drop". axis = 0 é para linhas e axis = 1 para colunas.
baixas_solic.drop('Grupo_pos_plano', axis = 1, inplace = True)

#=============================================================================#
#           FUNÇÃO "CONCAT" DO PANDAS PARA JUNTAR DOIS DATASETS
#=============================================================================#

# Importação das base de fup.
fup = pd.read_excel("FUP.xlsx")

# Adição de novas colunas.
fup['Etapa'] = "Follow Up"
fup['DATA_BAIXA'] = dia_plano

# Renomeação das colunas dos DataFrames.
fup.rename(columns = {"Grupo": "Grupo_Aloc", "Fase": "Fase_Aloc", 
                      "Responsável": "Responsável_Aloc"}, inplace = True)

# Criaçáo de uma lista para fazer o append.
lista = [baixas_solic, fup]
baixas = pd.concat(lista, ignore_index= True)

# Exportação do arquivo de baixas
baixas.to_excel("Baixas_Especiais." + dia_plano_sql + ".xlsx", encoding = 'utf8', index = False)

#=============================================================================#
#                 IMPORTAMOS O ARQUIVO ESTOQUE DA BASE ACCESS
#=============================================================================#

# Modulo para fazer conexões com bases de dados relacionais.
import pyodbc

# Cria uma conexão com uma base do Access.
conexion = pyodbc.connect(r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=/Users/wesleyhernandez/Desktop/Estoque.accdb;')

# Função para passar nas tabelas da base de dados.
cursor = conexion.cursor()

# Lista as tabelas disponiveis na base.
tabelas = list(cursor.tables())
nome_tabela = 'BASE CND'

# É definido o query para a tabela. Os colchetes "[]" são usados para quando as colunas tiver espaco.
query = "SELECT * FROM [{}]".format(nome_tabela)
cursor.execute(query)

# Transformação da base para DataFrame.
cnd = pd.read_sql(query, conexion)

# Fecha a execucao com a base de dados.
cursor.close()
conexion.close()

# Filtra uma unica data.
cnd_baixas = cnd.loc[cnd["Data Entrada"] == "15/10/2019"]              
              
# #=============================================================================#
# #                ADERENCIA DAS CARTEIRAS DE AÇÕES ESPECIAIS
# #=============================================================================# 

# Importação da base planejado do plano.
planejado = pd.read_excel("/Users/wesleyhernandez/Desktop/Especiais/Plano_Consolidado/Planos_Consolidados.xlsx")

# Fazendo o "de-para" das carteiras.
planejado["Carteira"].replace({"ASSUNTOS CORPORATIVOS": "Acorp e Grandes Causas", 
                               "Grandes Causas":"Acorp e Grandes Causas"}, inplace = True)

# Fazendo o "de-para" das fases.
planejado["Fase"].replace({"Finalizado": "Follow Up"}, inplace = True)

# Organização das colunas.
planejado_v1 = planejado[['Pasta', 'Carteira', 'Tipo Solicitação', 
                          'Tipo_Demanda', 'Fase','Grupo', 'Data Entrada', 
                          'Data_Limite','Data_Plano', 'Responsável']]

# Filtra todas as datas sem pegar o dia de hoje.
planejado_v2 = planejado_v1.loc[planejado_v1["Data_Plano"] <= dia_plano]   

# Seleção das colunas da base de Baixas_Especiais.
baixas_cruzamento = baixas[["Pasta", "Etapa", "DATA_BAIXA", "Data Entrada"]]

# Renomeação da colunas.
baixas_cruzamento.rename(columns ={"Pasta": "Baixado", "DATA_BAIXA": "Data_Plano", "Etapa": "Fase"}, inplace = True)

# Aplicaçaõ da função "merge" para fazer os cruzamentos.
aderencia_cruzamento = pd.merge(planejado_v2, baixas_cruzamento, how = "left",
                                left_on=["Pasta", "Fase", "Data_Plano", "Data Entrada"],
                                right_on= ["Baixado", "Fase", "Data_Plano", "Data Entrada"])

# Ordenação das coluna selecionadas (Sort)
aderencia_cruzamento_v1 = aderencia_cruzamento.sort_values(['Data_Plano','Fase', 'Data_Limite'],ascending=[True, False, True])
 
# Comando para tirar a duplicidade na base.
aderencia_cruzamento_v2 =aderencia_cruzamento_v1.drop_duplicates(subset=['Pasta','Tipo Solicitação','Data_Plano', 
                                                                         'Data Entrada', 'Responsável'])

# #=============================================================================#
# #                FILTRANDO UNICAMENTE A FASE DE SOLICITACAO
# #=============================================================================# 

# Filtra a fase.
plano_solic = aderencia_cruzamento_v2[aderencia_cruzamento_v2["Fase"] == "Solicitação"].sort_values(["Data_Plano", "Carteira"])   

plano_solic["Aderencia"] = plano_solic["Baixado"].apply(lambda x: "Baixado" if x>=0 else "nao baixado")

# Eliminando colunas com o comando "DROP". axis = 0 é para linhas e axis = 1 para colunas.
# plano_solic.drop('Baixado', axis = 1, inplace = True)

# #==========================================================No docu===================#
# #                FILTRANDO UNICAMENTE CADA CARTEIRA
# #=============================================================================#

prestacao = plano_solic[(plano_solic["Carteira"]== "PRESTAÇÃO DE CONTAS") & (plano_solic["Data_Plano"] == dia_plano)]

planos = plano_solic[(plano_solic["Carteira"] == "PLANOS ECONOMICOS") & (plano_solic["Data_Plano"] == dia_plano)]

# Export das carteiras
prestacao.to_excel("Prestacao_de_contas.xlsx", encoding = "utf8", index = False)

planos.to_excel("Planos_Economicos.xlsx", encoding = "utf8", index = False)

# #=============================================================================#
# #                            ADERENCIA AO PLANO DIA
# #=============================================================================#

# Selecionamos as colunas do plano.
ader_dia_1 = plano_solic[["Carteira", "Fase", "Data_Plano","Pasta", "Baixado"]]

# Renomeamos colunas
ader_dia_1.rename(columns = {"Pasta": "Alocado", "Baixado": "Baixado_no_plano"}, inplace = True)

# É feito a contagem (count) de algumas colunas especificas.
ader_dia = ader_dia_1.groupby(['Carteira', 'Fase', 'Data_Plano'], as_index = False).agg({"Alocado": "count", 'Baixado_no_plano': 'count'})

# É Acrescentado a coluna ADERENCIA = (Baixado no plano/Alocado)
ader_dia["Aderencia"] = ((ader_dia["Baixado_no_plano"]/ader_dia["Alocado"])*100).astype(str) + "%"
# x = 0.25
# x_teste = '{:.2%}'.format(x)

# #=============================================================================#
# #                            ADERENCIA AO PLANO ANO
# #=============================================================================#

# Selecionamos as colunas do plano dia.
ader_ano_1 = ader_dia[["Carteira", "Fase", "Data_Plano","Alocado", "Baixado_no_plano"]]

# Selecionamos especificamente o ano do plano.
ader_ano_1['Data_Plano'] = ader_ano_1['Data_Plano'].dt.year

# Renomeamos colunas
ader_ano_1.rename(columns = {"Data_Plano": "Ano"}, inplace = True)

# É feito a soma (SUM) de algumas colunas especificas.
ader_ano= ader_ano_1.groupby(['Carteira', 'Fase', 'Ano'], as_index = False).agg({"Alocado": "sum", 'Baixado_no_plano': 'sum'})

# É Acrescentado a coluna ADERENCIA = (Baixado no plano/Alocado)
ader_ano["Aderencia"] = ((ader_ano["Baixado_no_plano"]/ader_ano["Alocado"])*100).astype(str) + "%"
# x = 0.25
# x_teste = '{:.2%}'.format(x)
 
# #=============================================================================#
# #                            ADERENCIA AO PLANO MES
# #=============================================================================#

# Selecionamos as colunas do plano dia.
ader_mes_1 = ader_dia[["Carteira", "Fase", "Data_Plano","Alocado", "Baixado_no_plano"]]

# Selecionamos especificamente o ano do plano.
ader_mes_1['Data_Plano'] = ader_mes_1['Data_Plano'].dt.month

# Renomeamos colunas
ader_mes_1.rename(columns = {"Data_Plano": "Mes"}, inplace = True)

# É feito a soma (SUM) de algumas colunas especificas.
ader_mes= ader_mes_1.groupby(['Carteira', 'Fase', 'Mes'], as_index = False).agg({"Alocado": "sum", 'Baixado_no_plano': 'sum'})

# É Acrescentado a coluna ADERENCIA = (Baixado no plano/Alocado)
ader_mes["Aderencia"] = ((ader_mes["Baixado_no_plano"]/ader_mes["Alocado"])*100).astype(str) + "%"
# x = 0.25
# x_teste = '{:.2%}'.format(x)
 
# Fazendo o "de-para" das carteiras ou reemplazo de linhas especificas.
ader_mes["Mes"].replace({1: "JAN"}, inplace = True)

# Renomeamos colunas
ader_mes.rename(columns = {"Mes": "Periodo"}, inplace = True)

# #=============================================================================#
# #                       ADERENCIA A PRODUCAO COM O PLANEJADO
# #=============================================================================#

# Selecionamos as colunas.
ader_producao_agrup_1 = planejado[["Carteira", "Fase", "Data_Plano", "Pasta",]]

# Renomeamos colunas
ader_producao_agrup_1.rename(columns = {"Pasta": "Alocado"}, inplace = True)

# É feito a contagem (count) de algumas colunas especificas.
ader_producao_agrup_2 = ader_producao_agrup_1.groupby(['Carteira', 'Fase', 'Data_Plano'], as_index = False).agg({"Alocado": "count"})

# Filtra uma unica data.
ader_producao_agrup_3 = ader_producao_agrup_2.loc[ader_producao_agrup_2["Data_Plano"] <= dia_plano]   

# #=============================================================================#
# #                  ADERENCIA A PRODUCAO COM O APPEND TABLEA
# #=============================================================================#

# Selecionamos as colunas.
ader_producao_append_1 = baixas[["Carteira", "Etapa", "DATA_BAIXA", "Pasta"]]

# Renomeamos colunas
ader_producao_append_1.rename(columns = {"Pasta": "Baixado", "Etapa": "Fase", "DATA_BAIXA": "Data_Plano"}, inplace = True)

# Usamos o comando replace.
ader_producao_append_1["Carteira"].replace({"ASSUNTOS CORPORATIVOS": "Acorp e Grandes Causas", "Grandes Causas": "Acorp e Grandes Causas" }, inplace = True)

# É feito a contagem (count) de algumas colunas especificas.
ader_producao_append_2 = ader_producao_append_1.groupby(['Carteira', 'Fase', 'Data_Plano'], as_index = False).agg({"Baixado": "count"}).sort_values(['Carteira', 'Fase', 'Data_Plano'],ascending=[True, False, True])   

# #=============================================================================#
# #                  ADERENCIA A PRODUCAO - CRUZAMENTO
# #=============================================================================#

ader_producao = pd.merge(ader_producao_agrup_3, ader_producao_append_2, how = "left",
                                left_on=["Carteira", "Fase", "Data_Plano", "Alocado"],
                                right_on= ["Carteira", "Fase", "Data_Plano", "Baixado"])

# É Acrescentado a coluna ADERENCIA = (Baixado no plano/Alocado)
ader_producao["Aderencia Producao"] = ((ader_producao["Baixado"]/ader_producao["Alocado"])*100).astype(str) + "%"
# x = 0.25
# x_teste = '{:.2%}'.format(x)

# #=============================================================================#
# #                  ADERENCIA A PRODUCAO - ANO
# #=============================================================================#

ader_producao_ano = ader_producao[['Carteira', 'Fase', 'Alocado', 'Baixado']]

# SUMA
ader_producao_ano_1= ader_producao_ano.groupby(['Carteira', 'Fase'], as_index = False).agg({"Alocado": "sum", 'Baixado': 'sum'})

# É Acrescentado a coluna ADERENCIA = (Baixado no plano/Alocado)
ader_producao_ano_1["Aderencia Producao"] = ((ader_producao_ano_1["Baixado"]/ader_producao_ano_1["Alocado"])*100).astype(str) + "%"
# x = 0.25
# x_teste = '{:.2%}'.format(x)

# #=============================================================================#
# #                  ADERENCIA A PRODUCAO - MES
# #=============================================================================#

ader_producao_mes = ader_producao[['Carteira', 'Fase', 'Data_Plano', 'Alocado', 'Baixado']]

# Selecionamos especificamente o ano do plano.
ader_producao_mes['Data_Plano'] = ader_producao_mes['Data_Plano'].dt.month

# Renomeamos colunas
ader_producao_mes.rename(columns = {"Data_Plano": "Mes"}, inplace = True)

# SUMA
ader_producao_mes_1= ader_producao_mes.groupby(['Carteira', 'Fase', 'Mes'], as_index = False).agg({"Alocado": "sum", 'Baixado': 'sum'})

# É Acrescentado a coluna ADERENCIA = (Baixado no plano/Alocado)
ader_producao_mes_1["Aderencia"] = ((ader_producao_mes_1["Baixado"]/ader_producao_mes_1["Alocado"])*100).astype(str) + "%"
# x = 0.25
# x_teste = '{:.2%}'.format(x)

# Fazendo o "de-para" das carteiras ou reemplazo de linhas especificas.
ader_producao_mes_1["Mes"].replace({1: "JAN"}, inplace = True)

# Renomeamos colunas
ader_producao_mes_1.rename(columns = {"Mes": "Periodo"}, inplace = True)

fim = time.time()
print("Tempo em segundos: " + str(fim-inicio))
