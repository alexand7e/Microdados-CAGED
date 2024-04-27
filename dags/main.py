import os
import sys
import pandas as pd

# Ensure that custom modules can be loaded
sys.path.insert(0, os.path.join( os.getcwd(), 'dags' ))
sys.path.insert(1, '../Python')

from caged_data_processor import Microdata_caged

def setup_paths():
    local_path = os.path.dirname(os.getcwd())
    micro_path = os.path.dirname(local_path)
    return local_path, micro_path

def create_caged_instance():
    caged = Microdata_caged(mes_atual=2, ano_atual=2024, download_microdata=True)
    return caged

def process_data(caged):
    caged_microdados_full, caged_agrupamentos = caged.importar_caged_mes_ano(get_categoria=True)
    caged_dimensoes = caged.importar_dicionario()
    return caged_microdados_full, caged_agrupamentos, caged_dimensoes

def analyze_data(caged, caged_microdados_full, caged_agrupamentos, caged_dimensoes):
    if isinstance(caged_agrupamentos["município"], pd.DataFrame):
        print("É um DataFrame")
    else:
        print("Não é um DataFrame")

    dataframes_dict = caged.get_data_final_dictionary(caged_agrupamentos)
    caged_formatado_completo = caged.consolidar_caged(caged_agrupamentos, caged_dimensoes)
    df_salários = caged.analisar_salarios_aprimorado(caged_microdados_full, caged_dimensoes, ["Setores"])
    caged_formatado_completo["valorsaláriofixo"] = df_salários
    
    return caged_formatado_completo

def save_files(caged, local_path, caged_formatado_completo, caged_microdados_full, caged_dimensoes):
    caged.salvar_arquivos(file_path=os.path.join(local_path, 'data'), dicionario_df=caged_formatado_completo, filename="CAGED2")
    
    df_detalhamento = caged.analises_combinadas(caged_microdados_full, caged_dimensoes, grupos=["Setores"])
    df_detalhamento.to_excel(os.path.join(local_path, 'data', 'Setores.xlsx'))

    df_setor_por_muni = caged.analises_combinadas(caged_microdados_full, caged_dimensoes, grupos=["município", "subclasse"])
    idx = df_setor_por_muni.groupby("município")["Saldo"].idxmax()
    df_setor_por_muni = df_setor_por_muni.loc[idx]
    df_setor_por_muni.to_excel(os.path.join(local_path, 'data', 'setor_por_muni2.xlsx'))

def main():
    local_path, micro_path = setup_paths()
    caged = create_caged_instance()
    caged_microdados_full, caged_agrupamentos, caged_dimensoes = process_data(caged)
    caged_formatado_completo = analyze_data(caged, caged_microdados_full, caged_agrupamentos, caged_dimensoes)
    save_files(caged, local_path, caged_formatado_completo, caged_microdados_full, caged_dimensoes)

if __name__ == "__main__":
    #main()
    print ( os.getcwd() )
