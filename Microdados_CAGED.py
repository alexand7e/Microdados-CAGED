# Dependências
import pandas as pd
import numpy as np
import os

# caminho local para leitura os microdados
file_path_micro = "C:/Users/Alexandre/OneDrive/Documentos/R/Projeto CAGED/Files - Microdata/"
# caminho local para salvar os dados (e ler as dimensões)
file_path = "./Tabelas/"
# mês de referência para a importação das bases
mes_atual = 8
# dataframe para inserir as tabelas dimensões
dimensões = {}
# desagregação de dimensões
pages = ["município", "subclasse", "graudeinstrução", "faixaetária", "raçacor", "sexo", "salário"]
# para teste
piaui = 22


#...


# Função para classificar a faixa etária com base na idade
def classificar_faixa_etaria(idade):

    if pd.notna(idade) and isinstance(idade, (int, float)):
        # verificando se o valor é numérico
        idade = int(idade)
        if idade <= 17:
            return "Até 17 anos"
        elif 18 <= idade <= 24:
            return "18 a 24 anos"
        elif 25 <= idade <= 29:
            return "25 a 29 anos"
        elif 30 <= idade <= 39:
            return "30 a 39 anos"
        elif 40 <= idade <= 49:
            return "40 a 49 anos"
        elif 50 <= idade <= 64:
            return "50 a 64 anos"
        else:
            return "Mais de 65 anos"
    else:
        return "Não informado"

    
# Função para classificar o período
def classificar_período(data):
    return f'{data[:4]}-{data[-2:]}-01'

# Função para calcular a soma segura ~ verificando o tipo dos dados
def custom_sum(x):
    def convert_to_float(s):
        try:
            return float(s.replace(',', '.'))
        except ValueError:
            return np.nan

    numeric_values = [convert_to_float(val) for val in x if pd.notna(val)]

    if numeric_values:
        return round(sum(numeric_values), 2)  # Calcula a soma
    else:
        return np.nan
    

# Função para calcular a média segura
def custom_mean(x):
    def convert_to_float(s):
        try:
            return float(s.replace(',', '.'))
        except ValueError:
            return np.nan  # Retorna NaN em caso de erro de conversão

    numeric_values = [convert_to_float(val) for val in x if pd.notna(val)]

    if numeric_values:
        mean_value = np.nanmean(numeric_values)
        return round(mean_value, 2)  # Calcula a média
    else:
        return np.nan  # Retorna NaN se não tiver valores numéricos

    

# função para importar as bases (parâmetros fechados)
def importar_dimensoes():
    filename = ""
    # retirando o último elemento da lista ~ que não é uma dimensão
    pagess = pages[:-1]

    for pagename in pagess:
        if pagename == "município":
            filename = os.path.join(file_path, "dimensao_municipios.xlsx")
        else:
            filename = os.path.join(file_path, "dicionário_caged.xlsx")
            pagename = pagename
        dimensões[pagename] = pd.read_excel(filename, sheet_name=pagename)

    return dimensões


# Função para importar o histórico - falta ajustar e levar ao padrão da função funcional 'importar_caged_mes'
def importar_histórico_caged(anos_base = ["2023"], 
                             ultimo_mes = True):
    caged_base = []

    #
    bases = ["MOV", "EXC", "FOR"]
    anos_base = anos_base
    ultimo_mes = ultimo_mes

    mes_referencia = 0

    for type in bases:
        for ano in anos_base:
            
            # definindo o recorte temporal
            if ultimo_mes == 'atual' and ano == '2023':
                mes_referencia = mes_atual+1
            else:
                mes_referencia = 13

            for mes in range(1, mes_referencia):
                filename = f"CAGED{type}{ano}{str(mes).zfill(2)}.txt"

                caged = pd.read_table(os.path.join(file_path_micro, filename),
                                      sep=";",
                                      decimal=",")
                
                caged['Base'] = type
                
                caged['competênciamov'] = caged['competênciamov'].astype(str)
                caged['Período'] = caged["competênciamov"].str[:4] + "-" + caged["competênciamov"].str[-2:] + "-01"
                
                caged_base.append(caged)
    return caged_base


# Função para importar o caged para o mês espefícico 
def importar_caged_mes(anos_base=["2023"], ultimo_mes=8):
    caged_base = []

    período = f'2023-{str(ultimo_mes).zfill(2)}-01'
    anos_base = anos_base
    mes_referencia = ultimo_mes
    bases = ["MOV", "EXC", "FOR"]

    # Dicionário para armazenar DataFrames por dimensão
    result = {}  

    # Loop inicial - tipo das bases
    for type in bases:

        # Loop para os anos
        for ano in anos_base:

            # Nome do arquivo final
            filename = f"CAGED{type}{ano}{str(mes_referencia).zfill(2)}.txt"

            # Importação do arquivo .txt
            caged = pd.read_table(os.path.join(file_path_micro, filename),
                                  sep=";",
                                  decimal=",").query("uf == @piaui")

            # Alguns ajustes das variáveis
            caged['competênciamov'] = caged['competênciamov'].astype(str)
            caged['faixaetária'] = caged['idade'].apply(classificar_faixa_etaria)
            caged['Período'] = caged["competênciamov"].apply(classificar_período)

            # A base de 'exclusões' entra negativo na série
            if type == "EXC":
                caged['saldomovimentação'] = -caged['saldomovimentação']

            # Inserindo os dados no dicionário que criamos
            caged_base.append(caged)


    # Loop para o agrupamento dos dados conforme a dimensão
    for categoria in pages[:-1]:
        caged_with_base = pd.concat(caged_base, ignore_index=True).query("Período == @período")  # cópia do DataFrame caged

        # summarização dos dados, conforme período e a categoria 'page'
        grupo = caged_with_base.groupby(['Período', categoria]).agg(
            Desligamentos=('saldomovimentação', lambda x: (x == -1).sum()),
            Admissões=('saldomovimentação', lambda x: (x == 1).sum()),
            Salario_medio = ('salário', custom_mean)
        )

        # Variável de saldo do caged
        grupo['Saldo'] = grupo['Admissões'] - grupo['Desligamentos']

        # Certificando de criar o dicionário de data.frames
        if categoria not in result:
            result[categoria] = grupo  #~ 'result' é nosso dicionário de df's final agrupado

    return pd.concat(caged_base), result



# calculo inicial usando agrupamentos simples (município e subclasse)
def formatar_tabelas_caged(df, dimensao):
    
    df_consolidado = pd.concat(df)
    print(df_consolidado)

    grupo = df_consolidado.groupby(['Base',  'Período', dimensao]).agg(
        Desligamentos=('saldomovimentação', lambda x: (x == -1).sum()),  # Soma de -1
        Admissões=('saldomovimentação', lambda x: (x == 1).sum())   # Soma de 1
    )

    grupo['Saldo'] = grupo['Admissões'] - grupo['Desligamentos']

    # Ajustar a base de Exclusão (está contando positivo)
    grupo['Saldo'] = np.where(grupo['Base'] == "EXC", -grupo['Saldo'], grupo['Saldo'])
    grupo['Admissões'] = np.where(grupo['Admissões'] == "EXC", -grupo['Admissões'], grupo['Admissões'])
    grupo['Desligamentos'] = np.where(grupo['Desligamentos'] == "EXC", -grupo['Desligamentos'], grupo['Desligamentos'])
    
    return grupo.reset_index()


# Função para realizar a integração das categorias aos códigos e o agrupamento final das variáveis
# Detalhe: os parâmetros equivalem aos códigos agrupados pela função 'importar_caged_mes' e as dimensões
def consolidar_caged(caged_agrupado, caged_dimensoes):
    try:
        caged_formatado = {} # dicionário onde armazenamos o arquivo

        # loop para executar para a categoria respectiva. Detalhe: fixamos o período em todos
        for categoria in pages[:-1]:
            caged_formatado[categoria] = caged_agrupado[categoria].groupby(['Período', categoria]).agg({
                    'Saldo': 'sum',
                    'Admissões': 'sum',
                    'Desligamentos': 'sum',
                    'Salario_medio': 'mean'
                }).reset_index()
            
        # dicionário que armazena as funções com colunas combinadas das dimensões e códigos
        caged_formatado_completo = {}

        for categoria in pages[:-1]:
            df_ref = caged_formatado[categoria]
            df_cat_ref = caged_dimensoes[categoria]
            df_colunas = ["Período", "Descrição", "Admissões", "Desligamentos", "Saldo", "Salario_medio"]

            # Combinação das tabelas e ajuste das colunas
            df_ref = df_ref.merge(df_cat_ref, left_on = categoria, right_on = 'Código')
            df_ref = df_ref[df_colunas].rename(columns={"Descrição": categoria, "Salario_medio": "Salário médio"})

            caged_formatado_completo[categoria] = df_ref
        
        return caged_formatado_completo
        
    except Exception as e:
        print(f"Erro inesperado: {e}")


# Função para analisar os salários desagregador por município
def analisar_salarios(df_microdados, caged_dimensoes):
    
    df = df_microdados.query("Período == '2023-08-01'")
    grupo = df.groupby(['Período', 'município']).agg(
            Salario_total = ('salário', custom_sum),
            Salario_medio = ('salário', custom_mean)
        )
    
    grupo = grupo.merge(caged_dimensoes['município'], left_on="município", right_on="Código")

    # Removendo algumas colunas
    del grupo["município"]
    del grupo["Código"]
    del grupo["IBGE7"]

    # Ajustando nomes das colunas
    grupo = grupo.rename(columns={"Salario_total":"Montante de Salários", "Salario_medio":"Salário Médio", "Descrição":"Cidades"})

    return grupo.reset_index()


# Função para consolidar o arquivo excel.
# Para cada página, salvamos uma categoria dos dados, resultado do dicionário da função '@consolidar_caged'
def salvar_arquivos(list_of_df):
    filename = './Tabelas/Caged_processado.xlsx'

    with pd.ExcelWriter(filename, engine='xlsxwriter') as writer:
        # 
        for page in pages:
            list_of_df[page].to_excel(writer, sheet_name=page, index=False)
    





# função principal de execução do script -- INATIVA / Substituída pelo notebook
if False:

    d_municípios = pd.read_excel("municípios_piaui.xlsx")
    d_subclasses = pd.read_excel(r"C:\Users\Alexandre\OneDrive\Documentos\R\Projeto CAGED\dicionário_caged.xlsx", sheet_name = "subclasse")

    caged_microdados = importar_histórico_caged(22)
    caged_copia = caged_microdados.copy()

    caged_formatado = formatar_tabelas_caged(caged_copia)  

    try:

        # Ajustar a base de Exclusão (está contando positivo)
        caged_formatado['Saldo'] = np.where(caged_formatado['Base'] == "EXC", -caged_formatado['Saldo'], caged_formatado['Saldo'])
        caged_formatado['Admissões'] = np.where(caged_formatado['Admissões'] == "EXC", -caged_formatado['Admissões'], caged_formatado['Admissões'])
        caged_formatado['Desligamentos'] = np.where(caged_formatado['Desligamentos'] == "EXC", -caged_formatado['Desligamentos'], caged_formatado['Desligamentos'])


        # Agrupar por 'Período', 'município' e 'subclasse' e calcular as somas
        caged_formatado = caged_formatado.groupby(['Período', 'município', 'subclasse']).agg({
            'Saldo': 'sum',
            'Admissões': 'sum',
            'Desligamentos': 'sum'
        }).reset_index()

        # certificando se normalizar o tipo dos dados
        caged_formatado['município'] = caged_formatado['município'].astype(str)
        caged_formatado['subclasse'] = caged_formatado['subclasse'].astype(str)
        d_municípios['IBGE']   = d_municípios['IBGE'].astype(str)
        d_subclasses['Código'] = d_subclasses['Código'].astype(str)

        # completar a base
        caged_formatado_completo = caged_formatado.copy()

        caged_formatado_completo = caged_formatado_completo.merge(d_municípios, left_on = "município", right_on = "IBGE")
        caged_formatado_completo = caged_formatado_completo.merge(d_subclasses, left_on = "subclasse", right_on = "Código")


        caged_formatado_completo.to_excel("caged_pi_formatado.xlsx")

    except Exception as e:
        print(f"Erro inesperado: {e}")