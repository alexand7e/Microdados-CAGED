"""
NOTA: uma importante atualização foi inserida
Considerando que as bases FOR (dados informados fora de prazo) e EXC (exclusões em competências passadas)
alteram os meses anteriores, a importação e análise desses mesmos meses necessitam de considerar esses dados.
Sabendo disso, a Método 'importar_caged_mes_ano' agora realiza as seguintes operações: 

- Verifica os meses e anos posteriores ao período analisado, a fim de ajustar os dados;
- Verifica o ano/mes de referência e importa a base MOV da competência;
- Importa as bases EXC e FOR apenas para os meses seguintes à competência da análise;

Para tanto, incluiu-se as novas funções 'mes_analisado', 'ano_analisado' e 'importar_caged_tipo' (uma desagregação da Método anterior).
"""

# Dependências
import pandas as pd
import numpy as np
import time
import tempfile
from datetime import datetime
from pathlib import Path
from dateutil.relativedelta import relativedelta

import os
from dags.utils import CagedUtils
from ftp_data_fetcher import SevenZipExtractor, FTPDownloader
from processed_table_caged import Processed_caged


class CagedProcessor:
    """
    """
    def __init__(self, 
                 microdata_directory: str = None,
                 processed_files_directory: str = None,
                 current_month: int = (datetime.now() - relativedelta(months=2)).month,
                 current_year: int = (datetime.now() - relativedelta(months=2)).year,
                 uf_ref: int = 22,
                 download_microdata: bool = False,
                 insert_processed_table: bool = True,
                 categorias_analisadas: list = []) -> None:
        
        # Se file_path_micro e local_directory não forem fornecidos, cria um diretório temporário
        if microdata_directory is None and processed_files_directory is None:
            self.local_directory = self.local_directory = Path(__file__).resolve().parent.parent / "data"
            self.file_path_micro = tempfile.mkdtemp(prefix='caged_')
        else:
            self.file_path_micro = microdata_directory 
            self.local_directory = processed_files_directory 

        self.bases = ["MOV", "EXC", "FOR"]
        self.mes_atual = current_month
        self.ano_atual = current_year
        self.uf_ref = uf_ref

        self.data_base = f'{current_year}{str(current_month).zfill(2)}'
        self.inserir_tabela_processada = insert_processed_table

        default_categories = ["competênciamov", "município", "subclasse", "Setores", "Escolaridade", "faixaetária", "raçacor", "sexo"]
        if not categorias_analisadas:
            self.categorias = default_categories + ["valorsaláriofixo"]
        else: 
            self.categorias = categorias_analisadas + ["competênciamov", "valorsaláriofixo"]

        self.utils = CagedUtils()

        if download_microdata:
            self.baixar_microdados_e_dicionario()

        self.dimensões = self.importar_dicionario()
        
    
    def baixar_microdados_e_dicionario(self) -> None:
        """
        Baixa os microdados e o dicionário do CAGED para o ano e mês atual.
        """
        try:
            caged_downloader = FTPDownloader(
                local_directory=self.file_path_micro,
                file_extension='.7z',
                caged_year=self.ano_atual,
                caged_month=self.mes_atual
            )
            extractor = SevenZipExtractor(caged_downloader.local_directory)
            caged_downloader.download_specific_file("Layout Não-identificado Novo Caged Movimentação.xlsx", is_dict=True)
            files_to_download = caged_downloader.list_files()
            caged_downloader.download_and_extract(files_to_download, extractor)

            print('Download concluído!')

        except Exception as e:
            print(f"Ocorreu um erro: {e}")


    def obter_caged_processado(self) -> None:
        """A documentar"""
        try:
            caged_processed_downloader = Processed_caged(
                mes_referencia = self.utils.classificar_mes(self.mes_atual),
                ano_referencia = self.ano_atual,
                output_dit = self.file_path_micro
            )

            caged_processed_downloader.download_caged_file()
            caged_p_municipios, caged_p_estados = caged_processed_downloader.get_formatted_table()
            return caged_p_municipios, caged_p_estados
        except Exception as e:
            print(f"Ocorreu um erro: {e}")


    def importar_dicionario(self) -> pd.DataFrame:
        """
        A documentar
        """
        filename = os.path.join(self.file_path_micro, "Layout Não-identificado Novo Caged Movimentação.xlsx")
        caged_dimensoes = {}
        # Iterar sobre pages
        for pagename in self.categorias[:-1]:
            print(f"Importando dicionário da categoria: {pagename}")

            if pagename == "município":
                caged_dimensoes[pagename] = pd.read_excel(os.path.join(os.getcwd(), 'data', "auxiliar", "dimensao_municipios.xlsx"), sheet_name=pagename)
            elif pagename == "Setores":
                lista = self.utils.classificar_grupamento("", True) 
                caged_dimensoes[pagename] = pd.DataFrame({'Código': lista, 'Descrição': lista})
            elif pagename == "Escolaridade":
                lista = self.utils.classificar_escolaridade("", True) 
                caged_dimensoes[pagename] = pd.DataFrame({'Código': lista, 'Descrição': lista})
            elif pagename == "competênciamov":
                lista = self.utils.classificar_mes(None, True) 
                caged_dimensoes[pagename] = pd.DataFrame({'Código': [list(item.keys())[0] for item in lista], 
                                                         'Descrição': [list(item.values())[0] for item in lista]})
            elif pagename == "faixaetária":
                faixas = ['Até 17 anos', '18 a 24 anos', '25 a 29 anos', '30 a 39 anos', 
                          '40 a 49 anos', '50 a 64 anos', 'Mais de 65 anos', 'Não Identificado']
                caged_dimensoes[pagename] = pd.DataFrame({"Código": faixas, "Descrição":faixas})
            else:
                caged_dimensoes[pagename] = pd.read_excel(filename, sheet_name=pagename)
            
        return caged_dimensoes

    
    def importar_microdados_caged(self, ano, mes, tipo, data_inicial = None) -> pd.DataFrame:
        """
        Método para importar dados do CAGED de um determinado tipo
        """
        
        filename = f"CAGED{tipo}{ano}{str(mes).zfill(2)}.txt"
        filepath = os.path.join(self.file_path_micro, filename)

        if not os.path.exists(filepath):
            self.baixar_microdados_e_dicionario()
            time.sleep(10)

        caged = pd.read_table(filepath,
                              sep=";",
                              decimal=",").query(f"uf == {self.uf_ref}")

        # Realize os ajustes necessários nas colunas
        caged['competênciamov'] = caged['competênciamov'].astype(str)
        caged['faixaetária'] = self.utils.classificar_faixa_etaria(caged['idade'])
        caged['Período'] = caged["competênciamov"].apply(self.utils.classificar_período)
        caged["valorsaláriofixo"] = caged["valorsaláriofixo"].apply(self.utils.ajustar_coluna_decimal)
        caged["Setores"] = caged["seção"].apply(self.utils.classificar_grupamento)
        caged["Escolaridade"] = caged["graudeinstrução"].apply(self.utils.classificar_escolaridade)


        if data_inicial is None:
            caged = caged.loc[caged["competênciamov"] == self.data_base]
        else:
            caged = caged.loc[caged["Período"] >= data_inicial]

        # Se a base for 'EXC', invertemos o sinal de 'saldomovimentação' e desconsideramos o salário (para não afetar a média)
        if tipo == "EXC":
            caged['saldomovimentação'] = -caged['saldomovimentação']
            caged['valorsaláriofixo'] = np.nan

        return caged


    def processar_caged_por_mes_ano(self, get_categoria: bool = False) -> tuple:
        """
        Método para importar dados do CAGED para um mês e ano específico
        """
        caged_base = []  # Lista para armazenar os valores no loop
        result = {}

        for tipo in self.bases:
            for ano in self.utils.ano_analisado(self.ano_atual):
                for mes in self.utils.mes_analisado(self.mes_atual, self.ano_atual, self.ano_atual, self.mes_atual):
                    # Verifique se o tipo 'MOV' deve ser importado
                    if ano == int(self.ano_atual) and mes == int(self.mes_atual) and tipo == "MOV":
                        caged = self.importar_microdados_caged(ano, mes, tipo)
                        caged_base.append(caged)
                    elif tipo in ["EXC", "FOR"]:
                        caged = self.importar_microdados_caged(ano, mes, tipo)
                        caged_base.append(caged)

        if get_categoria:
            # Loop para o agrupamento dos dados conforme a dimensão
            for categoria in self.categorias[:-1]:
                caged_with_base = pd.concat(caged_base, ignore_index=True)  # cópia do DataFrame caged

                # summarização dos dados, conforme período e a categoria 'page'
                grupo = caged_with_base.groupby(['Período', categoria]).apply(lambda x: pd.Series({
                    'Salario_admissão': self.utils.custom_mean(x.loc[x['saldomovimentação'] == 1, 'valorsaláriofixo']), 
                    'Salario_desligamento': self.utils.custom_mean(x.loc[x['saldomovimentação'] == -1, 'valorsaláriofixo']),
                    'Admissões': (x['saldomovimentação'] == 1).sum(),
                    'Desligamentos': (x['saldomovimentação'] == -1).sum(),
                    'Saldo': (x['saldomovimentação'] == 1).sum() - (x['saldomovimentação'] == -1).sum()
                }))

                # Certificando de criar o dicionário de data.frames
                if categoria not in result:
                    result[categoria] = grupo  #~ 'result' é nosso dicionário de df's final agrupado

        return pd.concat(caged_base), result
    

    def processar_caged_por_histórico(self, ano_inicial: int = 2024, mes_inicial: int = 1) -> tuple:
        """
        : Método para importar o histórico 
        : Detalhe: esse Método importará todos os meses a partir do período base (inicial)
        """
        caged_base = []  # Lista para armazenar os valores no loop
        result = {}
        data_base_histórico = self.utils.classificar_período(f'{ano_inicial}{str(mes_inicial).zfill(2)}')

        for tipo in self.bases:
            for ano in self.utils.ano_analisado(ano_inicial):
                for mes in self.utils.mes_analisado(mes_inicial, ano_inicial, self.mes_atual):
                    caged = self.importar_microdados_caged(ano, mes, tipo, data_base_histórico)
                    caged_base.append(caged)

        # ~ As demais partes do código equivale aos da Método anterior
        for categoria in self.categorias[:-1]:
            caged_with_base = pd.concat(caged_base, ignore_index=True)  # cópia do DataFrame caged

            # summarização dos dados, conforme período e a categoria 'page'
            grupo = caged_with_base.groupby(['Período', categoria]).agg(
                Desligamentos=('saldomovimentação', lambda x: (x == -1).sum()),
                Admissões=('saldomovimentação', lambda x: (x == 1).sum()),
                Salario_admissão = ('valorsaláriofixo', lambda x: (x == 1).self.utils.custom_mean()),
                Salario_desligamento = ('valorsaláriofixo', lambda x: (x == -1).self.utils.custom_mean())
                #Salario_medio = ('valorsaláriofixo', custom_mean)
            )

            # Variável de saldo do caged
            grupo['Saldo'] = grupo['Admissões'] - grupo['Desligamentos']

            # Certificando de criar o dicionário de data.frames
            if categoria not in result:
                result[categoria] = grupo  #~ 'result' é nosso dicionário de df's final agrupado

        return pd.concat(caged_base), result


    def consolidar_caged(self, caged_agrupado: dict) -> dict:
        """
        : Método para realizar a integração das categorias aos códigos e o agrupamento final das variáveis
        : Detalhe: os parâmetros equivalem aos códigos agrupados pela Método 'importar_caged_mes' e as dimensões
        """

        try:
            caged_formatado = {} # dicionário onde armazenamos o arquivo

            # loop para executar para a categoria respectiva. Detalhe: fixamos o período em todos
            for categoria in self.categorias[:-1]:
                caged_formatado[categoria] = caged_agrupado[categoria].groupby(['Período', categoria]).agg({
                        'Saldo': 'sum',
                        'Admissões': 'sum',
                        'Desligamentos': 'sum',
                        'Salario_admissão': 'mean',
                        'Salario_desligamento': 'mean'
                    }).reset_index()
                
            # dicionário que armazena as funções com colunas combinadas das dimensões e códigos
            caged_formatado_completo = {}

            for categoria in self.categorias[:-1]:
                df_ref = caged_formatado[categoria]
                df_cat_ref = self.dimensões[categoria]
                df_colunas = ["Período", "Descrição", "Admissões", "Desligamentos", "Saldo", "Salario_desligamento", "Salario_admissão"]

                # Combinação das tabelas e ajuste das colunas
                df_ref = df_ref.merge(df_cat_ref, left_on = categoria, right_on = 'Código')
                df_ref = df_ref[df_colunas].rename(columns={"Descrição": categoria})

                caged_formatado_completo[categoria] = df_ref

            if self.inserir_tabela_processada:
                resultado_processamento = self.processar_tabela(self.dimensões["município"])
                caged_formatado_completo["Histórico - Estoque"] = resultado_processamento[0]
                caged_formatado_completo["Histórico - Saldo"] = resultado_processamento[1]
                caged_formatado_completo["Histórico - Variação"] = resultado_processamento[2]
                caged_formatado_completo["Territórios - Mês"] = resultado_processamento[3]
                caged_formatado_completo["Territórios - Ano"] = resultado_processamento[4]

            return caged_formatado_completo
            
        except Exception as e:
            print(f"Erro inesperado: {e}")
            return {}


    def analises_combinadas(self, df_microdados, grupos=[]):
        """
        Realiza análises combinadas com base em dados fornecidos.

        Parameters:
        - df_microdados (DataFrame): DataFrame principal contendo dados a serem analisados.
        - grupos (list): Lista de colunas para realizar a análise combinada. Pode incluir 'Setores' e outras categorias.

        Returns:
        - DataFrame: DataFrame resultante da análise combinada, incluindo admissões, desligamentos, saldo e descrições das categorias.
        """

        df = df_microdados
        caged_formatado = df.groupby(['Período', *grupos]).agg(
                Admissões=('saldomovimentação', lambda x: (x == 1).sum()),
                Desligamentos=('saldomovimentação', lambda x: (x == -1).sum())
            ).reset_index()  # Reset the index here to keep the group columns as regular columns

        caged_formatado["Saldo"] = caged_formatado["Admissões"] - caged_formatado["Desligamentos"]
        for categoria in grupos:
            if categoria != "Setores":
                df_cat_ref = self.dimensões[categoria]
                df_cat_ref = df_cat_ref.rename(columns={"Código": categoria, "Descrição": f"{categoria}_Descrição"})

                df_cat_ref[categoria] = df_cat_ref[categoria].astype(str)
                caged_formatado[categoria] = caged_formatado[categoria].astype(str)

                caged_formatado = caged_formatado.join(df_cat_ref.set_index(categoria)[[f"{categoria}_Descrição"]], on=categoria)

        return caged_formatado


    def analisar_salarios_aprimorado(self, df_microdados, dimensoes: list):
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
                'Salario_total': self.utils.custom_sum(x['valorsaláriofixo']),
                'Salario_medio': self.utils.custom_mean(x['valorsaláriofixo']),
                'Contagem': x['valorsaláriofixo'].count(),
                'Salario_mediana': x['valorsaláriofixo'].median(),
                'Salario_admissao': self.utils.custom_mean(x.loc[x['saldomovimentação'] == 1, 'valorsaláriofixo']),
                'Salario_desligamento': self.utils.custom_mean(x.loc[x['saldomovimentação'] == -1, 'valorsaláriofixo'])
            }))

            df_dimensao = self.dimensões[dimensao].rename(columns={'Código': dimensao})
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


    def salvar_arquivos(self, saving_dir: str, dicionario_df: dict, suffix_file: str = "caged"):
        """
        Método para consolidar o arquivo excel.
        Para cada página, salvamos uma categoria dos dados, resultado do dicionário da Método '@consolidar_caged'
        """
        arquivo = pd.ExcelWriter(f'{saving_dir}/{suffix_file}_{self.utils.classificar_mes(self.mes_atual)}.xlsx')
        for page, data in dicionario_df.items():
            if isinstance(data, dict):
                try:
                    df = pd.DataFrame(data)
                except ValueError:
                    df = pd.DataFrame([data])
            else:
                df = data 
            df.to_excel(arquivo, sheet_name=page)
        arquivo.close()


    def get_categorias(self):
        return self.categorias


    def processar_tabela(self, caged_dimensao_municipios: dict):
        df_est_pc, df_mun_pc = self.obter_caged_processado()
        estado_referencia = self.utils.classificar_ufs(self.uf_ref)
        regiao_referencia = self.utils.classificar_regiao(self.uf_ref)
        mes_referencia = self.utils.classificar_mes(self.mes_atual)

        df_hist_sal = df_est_pc[(df_est_pc["Região"] == "Piauí") & (df_mun_pc["Variável"] == "Saldos")]
        df_hist_estoq = df_est_pc[(df_est_pc["Região"] == "Piauí") & (df_mun_pc["Variável"] == "Estoque")]

        df_hist_var = df_est_pc[(df_est_pc["Região"].isin(["Brasil", regiao_referencia, estado_referencia])) & (df_est_pc["Variável"] == "Variação Relativa (%)")].pivot(index=['Data', 'Variável'], columns='Região', values='Valor')

        df_hist_est = df_mun_pc[df_mun_pc["Região"].astype(str).str.startswith(str(self.uf_ref))]
        df_hist_est.loc[:, 'Região'] = df_hist_est['Região'].astype(str)

        caged_dimensao_municipios['Código'] = caged_dimensao_municipios['Código'].astype(str)
        df_hist_est = df_hist_est.merge(caged_dimensao_municipios, left_on="Região", right_on="Código")
        
        df_hist_est['Data'] = df_hist_est['Data'].astype(str)
        df_pi_terr_month = df_hist_est[df_hist_est['Data'].str.contains(f"{"Março"}/{self.ano_atual}")].pivot_table(
            index='Território (PI)',
            columns='Variável',
            values='Valor',
            aggfunc='sum'
        ) 

        # Dados anuais acumulados: criar df_pi_terr_ann
        df_hist_est['Data'] = df_hist_est['Data'].astype(str)
        df_pi_terr_ann = df_hist_est[df_hist_est['Data'].str.contains("Acumulado do Ano")].pivot_table(
            index='Território (PI)',
            columns='Variável',
            values='Valor',
            aggfunc='sum'
        ) 

        return ( df_hist_estoq, df_hist_sal, df_hist_var, df_pi_terr_month, df_pi_terr_ann)
    
    def get_maior_setor_por_municipio(self, caged_microdados_full): # uso interno para fins do relatório
        df_setor_por_muni = self.analises_combinadas(caged_microdados_full, grupos=["município", "subclasse"])
        idx = df_setor_por_muni.groupby("município")["Saldo"].idxmax()
        df_setor_por_muni = df_setor_por_muni.loc[idx]

        return df_setor_por_muni