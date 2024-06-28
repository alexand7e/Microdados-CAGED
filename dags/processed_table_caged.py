import pandas as pd
import requests
from bs4 import BeautifulSoup as bs
import os
import zipfile
import shutil


class Processed_caged:
    def __init__(self, output_dir: str, mes_referencia: str, ano_referencia: int) -> None:
        self.mes = mes_referencia
        self.ano = ano_referencia
        self.output_dir = output_dir
        self.file_path = f'{self.output_dir}/caged_processada_{self.mes}_{self.ano}.xlsx'
        self.url = f'https://www.gov.br/trabalho-e-emprego/pt-br/assuntos/estatisticas-trabalho/novo-caged/novo-caged-{self.ano}/{self.mes}'
        
    def download_caged_file(self) -> None:
        response = requests.get(self.url)
        if response.status_code == 200:
            soup = bs(response.text, "html.parser")
            link = soup.find('a', string='Tabelas.xls')
            if link:
                href = link['href']
                print(f"Requisitando {href}...")
                self._download_and_unzip(href)
            else:
                print("Link para download não encontrado.")
        else:
            print("Falha ao acessar a página do CAGED.")
    
    def _download_and_unzip(self, url):
        response = requests.get(url)
        if response.status_code == 200:
            temp_path = os.path.join(self.output_dir, 'temp.zip')
            with open(temp_path, 'wb') as file:
                file.write(response.content)
            
            if zipfile.is_zipfile(temp_path):
                with zipfile.ZipFile(temp_path, 'r') as zip_ref:
                    zip_ref.extractall(self.output_dir)
                    extracted_files = zip_ref.namelist()
                os.remove(temp_path)
                if extracted_files:
                    original_file = os.path.join(self.output_dir, extracted_files[0])
                    shutil.move(original_file, self.file_path)
                print("Arquivo ZIP descompactado e renomeado com sucesso.")
            else:
                shutil.move(temp_path, self.file_path)
                print("Arquivo XLSX baixado e renomeado com sucesso.")
        else:
            print("Falha ao baixar o arquivo.")
    

    def get_formatted_table(self, adjusted_caged: bool = True) -> tuple:    
        """
        Importação e processamento da planilha-base do CAGED.
        
        Dependendo do valor de adjusted_caged, seleciona as tabelas ajustadas ou não ajustadas para processamento.
        As tabelas 7 e 8 são as versões não ajustadas, enquanto as tabelas 7.1 e 8.1 são as versões ajustadas.
        
        Args:
            adjusted_caged (bool): Flag para determinar se as tabelas ajustadas devem ser usadas. O valor padrão é True.

        Returns:
            dict: Um dicionário de DataFrames processados com chaves sendo o nome das páginas (ex: 'Tabela 7.1').
        """
        dict_tb = {}
        pages = ["Tabela 7.1", "Tabela 8.1"] if adjusted_caged else ["Tabela 7", "Tabela 8"]

        for page in pages:
            # Usando um operador ternário para condicionar o processamento
            df = pd.read_excel(self.file_path, sheet_name=page).iloc[3:-6, 1:]#-8]
            if page in ["Tabela 7", "Tabela 7.1"]: 
                df_selected = df.drop(columns=["Unnamed: 2", "Unnamed: 3", "Unnamed: 4", "Unnamed: 5"])
                df_processed = self.format_caged_table(df_selected)
            else:
                df_selected = df.drop(columns=["Unnamed: 1", "Unnamed: 3", "Unnamed: 4", "Unnamed: 5", "Unnamed: 6", "Unnamed: 7"])
                df_processed = self.format_caged_table(df_selected)
                df_processed['Região'] = df_processed['Região'].astype(str).str[:-2]
            
            dict_tb[page] = df_processed

        return dict_tb[pages[0]], dict_tb[pages[1]]

        
    def format_caged_table(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Formata um DataFrame do CAGED, transformando-o de um formato largo para um formato longo.
        
        Args:
            df (pd.DataFrame): DataFrame original do CAGED.

        Returns:
            pd.DataFrame: DataFrame transformado com as colunas 'Região', 'Data', 'Variável', 'Valor'.
        """
        # Para a primeira coluna
        df_ajustada = df.copy()
        df_ajustada.insert(loc=df_ajustada.shape[1] - 4, column='Adjust1', value=[" "] * df_ajustada.shape[0])
        df_ajustada.insert(loc=df_ajustada.shape[1] - 0, column='Adjust2', value=[" "] * df_ajustada.shape[0])

        variables = df_ajustada.iloc[1, 1:].values 
        dates = df_ajustada.iloc[0, 1::5].values  
        transformed_data_corrected = []

        # Iterando sobre cada região/UF
        # for index, row in df_ajustada.iloc[2:].iterrows():  
        #     regiao_uf = row[0]  
        #     for date_index, date in enumerate(dates):
        #         variables_subset = variables[date_index*5:(date_index+1)*5]  
        #         for var_index, variable in enumerate(variables_subset):
        #             valor_index = 1 + date_index*5 + var_index  
        #             if valor_index < len(row):  
        #                 valor = row[valor_index]  
        #                 if pd.notnull(valor):  
        #                     transformed_data_corrected.append([regiao_uf, date, variable, valor])
        for index, row in df_ajustada.iloc[2:].iterrows():
            regiao_uf = row.iloc[0]
            for date_index, date in enumerate(dates):
                variables_subset = variables[date_index*5:(date_index+1)*5]
                for var_index, variable in enumerate(variables_subset):
                    valor_index = 1 + date_index*5 + var_index
                    if valor_index < len(row):
                        valor = row.iloc[valor_index]
                        if pd.notnull(valor):
                            transformed_data_corrected.append([regiao_uf, date, variable, valor])
                            
        df_transformed = pd.DataFrame(transformed_data_corrected, columns=['Região', 'Data', 'Variável', 'Valor'])
        return df_transformed

