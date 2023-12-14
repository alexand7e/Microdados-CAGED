# Dependências
from decimal import Decimal
import pandas as pd
import numpy as np
import os

"""
NOTA: uma importante atualização foi inserida
Considerando que as bases FOR (dados informados fora de prazo) e EXC (exclusões em competências passadas)
alteram os meses anteriores, a importação e análise desses mesmos meses necessitam de considerar esses dados.
Sabendo disso, a função 'importar_caged_mes_ano' agora realiza as seguintes operações: 

- Verifica os meses e anos posteriores ao período analisado, a fim de ajustar os dados;
- Verifica o ano/mes de referência e importa a base MOV da competência;
- Importa as bases EXC e FOR apenas para os meses seguintes à competência da análise;

Para tanto, incluiu-se as novas funções 'mes_analisado', 'ano_analisado' e 'importar_caged_tipo' (uma desagregação da função anterior).
"""

# caminho local para leitura os microdados
file_path_micro = r"C:\Users\alexa\Documents\Microdados"
# caminho local para salvar os dados (e ler as dimensões)
file_path = "./Tabelas/"
# mês de referência para a importação das bases
mes_atual = 10
# mês de referência para a importação das bases
ano_atual = 2023
# estrutura de data de referência
data_base = f'{ano_atual}{str(mes_atual).zfill(2)}'
# dicionário para inserir as tabelas dimensões
dimensões = {}
# desagregação de dimensões
pages = ["município", "subclasse", "Setores", "Escolaridade", "faixaetária", "raçacor", "sexo", "valorsaláriofixo"]
# para teste
piaui = 22


# Função para classificar a faixa etária com base na idade
def classificar_faixa_etaria(idade):
    bins = [0, 17, 24, 29, 39, 49, 64, float('inf')]
    labels = ["Até 17 anos", 
              "18 a 24 anos", 
              "25 a 29 anos", 
              "30 a 39 anos", 
              "40 a 49 anos", 
              "50 a 64 anos", 
              "Mais de 65 anos"]

    if isinstance(idade, (int, float)):
        idade = int(idade)
        return pd.cut([idade], bins=bins, labels=labels, right=False)[0]
    elif isinstance(idade, pd.Series):
        return pd.cut(idade, bins=bins, labels=labels, right=False)
    else:
        return 'Não Identificado'


# Função para realizar a classificação setorial a partir das seções da CNAE 2.0 
def classificar_grupamento(codigo: str, return_list: bool = False):
    grupos = {
        'A': 'Agricultura, pecuária, produção florestal, pesca e aquicultura',
        'B': 'Indústria geral',
        'C': 'Indústria geral', #'Indústrias de Transformação',
        'D': 'Indústria geral',
        'E': 'Indústria geral',
        'F': 'Construção',
        'G': 'Comércio, reparação de veículos automotores e motocicletas',
        'H': 'Serviços de transporte, armazenagem e correio',
        'I': 'Alojamento e alimentação',
        'J': 'Informação, comunicação e atividades financeiras, imobiliárias, profissionais e administrativas',
        'K': 'Informação, comunicação e atividades financeiras, imobiliárias, profissionais e administrativas',
        'L': 'Informação, comunicação e atividades financeiras, imobiliárias, profissionais e administrativas',
        'M': 'Informação, comunicação e atividades financeiras, imobiliárias, profissionais e administrativas',
        'N': 'Informação, comunicação e atividades financeiras, imobiliárias, profissionais e administrativas',
        'O': 'Administração pública, defesa, seguridade social, educação, saúde humana e serviços sociais',
        'P': 'Administração pública, defesa, seguridade social, educação, saúde humana e serviços sociais',
        'Q': 'Administração pública, defesa, seguridade social, educação, saúde humana e serviços sociais',
        'R': 'Outros serviços',
        'S': 'Outros serviços',
        'T': 'Serviços domésticos',
        'U': 'Outros serviços'
    }
    if return_list:
        return list(set(grupos.values()))
    else:
        return grupos.get(codigo, 'Não Identificado')


def classificar_escolaridade(escolaridade: str, return_list: bool = False):
    grau = {
    1:'Analfabeto',
    2:'Fundamental Incompleto',
    3:'Fundamental Incompleto',
    4:'Fundamental Incompleto',
    5:'Fundamental Completo',
    6:'Médio Incompleto',
    7:'Médio Completo',
    8:'Superior Incompleto',
    9:'Superior Completo',
    10:'Superior Completo',
    11:'Superior Completo',
    80:'Superior Completo',
    99:'Não Identificado'
    }
    if return_list:
        return list(set(grau.values()))
    else:
        return grau.get(escolaridade, 'Não Identificado')
    
#print(classificar_escolaridade("6ª a 9ª Fundamental"))

# Função para classificar o período
def classificar_período(competencia_caged):
    competencia_caged = pd.to_datetime(f'{competencia_caged[:4]}-{competencia_caged[-2:]}-01')
    return pd.Timestamp(competencia_caged).date()


# Função para ajustar valores float (principalmente o de valorsaláriofixo)
def ajustar_coluna_decimal(x):
    if isinstance(x, str):
        x = x.replace(',', '.')
    elif isinstance(x, float):
        x = str(x).replace(',', '.')

    try:
        x_float = float(x)
    except ValueError:
        return np.nan

    if x_float < 0.3 * 1320 or x_float > 150 * 1320:
        return np.nan
    else:
        return x_float


# Função para calcular a soma segura ~ verificando o tipo dos dados
def custom_sum(x):
    numeric_values = [val for val in x if pd.notna(val) and isinstance(val, (int, float))]

    if numeric_values:
        return round(sum(numeric_values), 2)  # Calcula a soma
    else:
        return np.nan

# Função para calcular a média segura
def custom_mean(x):
    numeric_values = [val for val in x if pd.notna(val) and isinstance(val, (int, float))]

    if numeric_values:
        mean_value = Decimal(np.nanmean(numeric_values))
        return round(mean_value, 2)  # Calcula a média
    else:
        return np.nan


#
def mes_analisado(mes_escolhido, ano_base):
    meses = list(range(1, 13))
    if 1 <= mes_escolhido <= 12:
        # Se o ano for '2023', isto limita a lista de meses até o último mês disponível - 'mes_atual'
        if ano_base == 2023:
            meses_selecionados = meses[mes_escolhido - 1:mes_atual]
        else:
            meses_selecionados = meses[mes_escolhido - 1:]
    return meses_selecionados

#
def ano_analisado(ano_base):
    anos = [2020, 2021, 2022, 2023]
    i_ano = anos.index(ano_base)
    
    return anos[i_ano:]


def importar_dimensoes():
    filename = os.path.join(file_path, "dicionário_caged.xlsx")
    sheet_names = pd.ExcelFile(filename).sheet_names
    print(sheet_names)

    # Itere sobre sheet_names em vez de pages
    for pagename in pages[:-1]:
        if pagename == "município":
            dimensões[pagename] = pd.read_excel(os.path.join(file_path, "dimensao_municipios.xlsx"), sheet_name=pagename)
        elif pagename == "Setores":
            lista = classificar_grupamento("", True)  # Ajuste os argumentos conforme necessário
            dimensões[pagename] = pd.DataFrame({'Código': lista, 'Descrição': lista})
        elif pagename == "Escolaridade":
            lista = classificar_escolaridade("", True)  # Ajuste os argumentos conforme necessário
            dimensões[pagename] = pd.DataFrame({'Código': lista, 'Descrição': lista})
            print(dimensões[pagename])
        else:
            dimensões[pagename] = pd.read_excel(filename, sheet_name=pagename)

    return dimensões

# Função para importar dados do CAGED de um determinado tipo
def importar_caged_tipo(ano, mes, tipo, data_inicial = None):
    filename = f"CAGED{tipo}{ano}{str(mes).zfill(2)}.txt"

    caged = pd.read_table(os.path.join(file_path_micro, filename),
                            sep=";",
                            decimal=",").query("uf == 22")
                            #usecols = lambda x: x.strip() in ["competênciamov", "idade","graudeinstrução", "seção", *pages])

    # Realize os ajustes necessários nas colunas
    caged['competênciamov'] = caged['competênciamov'].astype(str)
    caged['faixaetária'] = classificar_faixa_etaria(caged['idade'])#.apply(classificar_faixa_etaria)
    caged['Período'] = caged["competênciamov"].apply(classificar_período)
    caged["valorsaláriofixo"] = caged["valorsaláriofixo"].apply(ajustar_coluna_decimal)
    caged["Setores"] = caged["seção"].apply(classificar_grupamento)
    caged["Escolaridade"] = caged["graudeinstrução"].apply(classificar_escolaridade)


    if data_inicial is None:
        caged = caged.loc[caged["competênciamov"] == data_base]
    else:
        caged = caged.loc[caged["Período"] >= data_inicial]

    # Se a base for 'EXC', é preciso inverter o sinal de 'saldomovimentação'
    if tipo == "EXC":
        caged['saldomovimentação'] = -caged['saldomovimentação']
        caged['valorsaláriofixo'] = np.nan

    return caged


# Função para importar dados do CAGED para um mês e ano específico
def importar_caged_mes_ano(ano_base, mes_base, get_categoria: bool = False):
    caged_base = []  # Lista para armazenar os valores no loop
    result = {}

    bases = ["MOV", "EXC", "FOR"]

    for tipo in bases:
        for ano in ano_analisado(ano_base):
            for mes in mes_analisado(mes_base, ano_base):
                # Verifique se o tipo 'MOV' deve ser importado
                if ano == int(ano_base) and mes == int(mes_base) and tipo == "MOV":
                    caged = importar_caged_tipo(ano, mes, tipo)
                    caged_base.append(caged)
                elif tipo in ["EXC", "FOR"]:
                    caged = importar_caged_tipo(ano, mes, tipo)
                    caged_base.append(caged)

    if get_categoria:
        # Loop para o agrupamento dos dados conforme a dimensão
        for categoria in pages[:-1]:
            caged_with_base = pd.concat(caged_base, ignore_index=True)  # cópia do DataFrame caged

            # summarização dos dados, conforme período e a categoria 'page'
            grupo = caged_with_base.groupby(['Período', categoria]).apply(lambda x: pd.Series({
                'Salario_admissão': custom_mean(x.loc[x['saldomovimentação'] == 1, 'valorsaláriofixo']), #(x["valorsaláriofixo"])
                'Salario_desligamento': custom_mean(x.loc[x['saldomovimentação'] == -1, 'valorsaláriofixo']),
                'Admissões': (x['saldomovimentação'] == 1).sum(),
                'Desligamentos': (x['saldomovimentação'] == -1).sum(),
                'Saldo': (x['saldomovimentação'] == 1).sum() - (x['saldomovimentação'] == -1).sum()
            }))

            # Certificando de criar o dicionário de data.frames
            if categoria not in result:
                result[categoria] = grupo  #~ 'result' é nosso dicionário de df's final agrupado

    return pd.concat(caged_base), result
    

# Função para importar o histórico 
# Detalhe: essa função importará todos os meses a partir do período base (inicial)
def importar_histórico_caged(ano_inicial=2023, mes_inicial=1):

    caged_base = []  # Lista para armazenar os valores no loop
    result = {}
    data_base_histórico = classificar_período(f'{ano_inicial}{str(mes_inicial).zfill(2)}')

    bases = ["MOV", "EXC", "FOR"]

    for tipo in bases:
        for ano in ano_analisado(ano_inicial):
            for mes in mes_analisado(mes_inicial, ano_inicial):
                caged = importar_caged_tipo(ano, mes, tipo, data_base_histórico)
                caged_base.append(caged)

    # ~ As demais partes do código equivale aos da função anterior

    for categoria in pages[:-1]:
        caged_with_base = pd.concat(caged_base, ignore_index=True)  # cópia do DataFrame caged

        # summarização dos dados, conforme período e a categoria 'page'
        grupo = caged_with_base.groupby(['Período', categoria]).agg(
            Desligamentos=('saldomovimentação', lambda x: (x == -1).sum()),
            Admissões=('saldomovimentação', lambda x: (x == 1).sum()),
            Salario_admissão = ('valorsaláriofixo', lambda x: (x == 1).custom_mean()),
            Salario_desligamento = ('valorsaláriofixo', lambda x: (x == -1).custom_mean())
            #Salario_medio = ('valorsaláriofixo', custom_mean)
        )

        # Variável de saldo do caged
        grupo['Saldo'] = grupo['Admissões'] - grupo['Desligamentos']

        # Certificando de criar o dicionário de data.frames
        if categoria not in result:
            result[categoria] = grupo  #~ 'result' é nosso dicionário de df's final agrupado

    return pd.concat(caged_base), result


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
                    'Salario_admissão': 'mean',
                    'Salario_desligamento': 'mean'
                }).reset_index()
            
        # dicionário que armazena as funções com colunas combinadas das dimensões e códigos
        caged_formatado_completo = {}

        for categoria in pages[:-1]:
            df_ref = caged_formatado[categoria]
            df_cat_ref = caged_dimensoes[categoria]
            df_colunas = ["Período", "Descrição", "Admissões", "Desligamentos", "Saldo", "Salario_desligamento", "Salario_admissão"]

            # Combinação das tabelas e ajuste das colunas
            df_ref = df_ref.merge(df_cat_ref, left_on = categoria, right_on = 'Código')
            df_ref = df_ref[df_colunas].rename(columns={"Descrição": categoria})

            caged_formatado_completo[categoria] = df_ref
        
        return caged_formatado_completo
        
    except Exception as e:
        print(f"Erro inesperado: {e}")


def analises_combinadas(df_microdados, caged_dimensoes, grupos = []):
    """
    Realiza análises combinadas com base em dados fornecidos.

    Parameters:
    - df_microdados (DataFrame): DataFrame principal contendo dados a serem analisados.
    - caged_dimensoes (DataFrame): DataFrame contendo dimensões adicionais.
    - grupos (list): Lista de colunas para realizar a análise combinada. Pode incluir 'Setores' e outras categorias.

    Returns:
    - DataFrame: DataFrame resultante da análise combinada, incluindo admissões, desligamentos, saldo e descrições das categorias.
    """

    df = df_microdados
    caged_formatado = df.groupby(['Período', *grupos]).agg(
            
            Admissões=('saldomovimentação', lambda x: (x == 1).sum()),
            Desligamentos=('saldomovimentação', lambda x: (x == -1).sum())
        )
    
    caged_formatado["Saldo"] = caged_formatado["Admissões"] - caged_formatado["Desligamentos"]
    for categoria in grupos:
        if categoria != "Setores":
            df_cat_ref = caged_dimensoes[categoria]
            
            df_cat_ref = df_cat_ref.rename(columns={"Código": categoria})
            df_cat_ref = df_cat_ref.rename(columns={"Código": categoria, "Descrição": f"{categoria}_Descrição"})
            caged_formatado = caged_formatado.join(df_cat_ref.set_index(categoria)[[f"{categoria}_Descrição"]], on=categoria)

              
    return caged_formatado


def analisar_salarios_aprimorado(df_microdados, caged_dimensoes, dimensoes: list):
    """
    Analisa salários com base em dados fornecidos, incluindo novas variáveis e agrupamentos para cada dimensão.

    Parameters:
    - df_microdados (DataFrame): DataFrame principal contendo dados a serem analisados.
    - caged_dimensoes (DataFrame): DataFrame contendo dimensões adicionais.
    - dimensoes (list): Lista de nomes das dimensões para realizar a análise.

    Returns:
    - dict: Dicionário contendo DataFrames resultantes da análise de salários para cada dimensão.
    """
    resultados_por_dimensao = {}

    for dimensao in dimensoes:
        grupo = df_microdados.groupby(['Período', dimensao]).apply(lambda x: pd.Series({
            'Salario_total': custom_sum(x['valorsaláriofixo']),
            'Salario_medio': custom_mean(x['valorsaláriofixo']),
            'Contagem': x['valorsaláriofixo'].count(),
            'Salario_mediana': x['valorsaláriofixo'].median(),
            'Salario_admissao': custom_mean(x.loc[x['saldomovimentação'] == 1, 'valorsaláriofixo']),
            'Salario_desligamento': custom_mean(x.loc[x['saldomovimentação'] == -1, 'valorsaláriofixo'])
        }))

        df_dimensao = caged_dimensoes[dimensao].rename(columns={'Código': dimensao})
        grupo = grupo.merge(df_dimensao, on=dimensao, how="left")

        # Removendo algumas colunas
        if dimensao == "município":
            del grupo["município"]
            del grupo["IBGE7"]

        # Ajustando nomes das colunas
        grupo = grupo.rename(columns={"Salario_total": "Montante de Salários",
                                      "Salario_medio": "Salário Médio",
                                      "Salario_mediana": "Salário Mediana",
                                      "Salario_admissao": "Salário Admissão",
                                      "Salario_desligamento": "Salário Demissão",
                                      "Contagem": "Número de Registros"})

        resultados_por_dimensao[dimensao] = grupo.reset_index()

    return resultados_por_dimensao


# Função para consolidar o arquivo excel.
# Para cada página, salvamos uma categoria dos dados, resultado do dicionário da função '@consolidar_caged'
def salvar_arquivos(dicionário_df, filename):
    filename = f'./Tabelas/{filename}.xlsx'

    with pd.ExcelWriter(filename, engine='xlsxwriter') as arquivo:
        # 
        for page in pages:
            dicionário_df[page].to_excel(arquivo, sheet_name=page, index=False)    

