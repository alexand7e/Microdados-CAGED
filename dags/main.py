import os
import sys
import pandas as pd

# Ensure that custom modules can be loaded
sys.path.insert(0, os.path.join(os.getcwd(), 'dags'))
sys.path.insert(1, os.getcwd())

from caged_data_processor import CagedProcessor

def main():
    # Setup paths
    local_path = os.getcwd()
    paths = {
        'bronze': os.path.join(local_path, 'data', 'bronze'), # microdados
        'silver': os.path.join(local_path, 'data', 'silver'), # semi-processados
        'gold': os.path.join(local_path, 'data', 'gold') # tabela(s) final(is)
    }

    # Create instance of Microdata_caged
    caged = CagedProcessor(microdata_directory=paths.get('bronze'),
                            processed_files_directory=paths.get('silver'),
                            current_month=2, 
                            current_year=2024, 
                            download_microdata=True, 
                            insert_processed_table=True)
    
    # Process data
    caged_microdados_full, caged_agrupamentos = caged.processar_caged_por_mes_ano(get_categoria=True)

    caged_formatado_completo = caged.consolidar_caged(caged_agrupamentos)

    caged_formatado_completo["salarios"] = caged.analisar_salarios_aprimorado(caged_microdados_full, ["Setores"])
    caged_formatado_completo["Setor - Município"] = caged.get_maior_setor_por_municipio(caged_microdados_full)
    
    # Save files
    caged.salvar_arquivos(saving_dir=paths.get('gold'), dicionario_df=caged_formatado_completo, suffix_file="CAGED")
    
    df_detalhamento = caged.analises_combinadas(caged_microdados_full, caged.dimensões(), grupos=["Setores"])
    df_detalhamento.to_excel(os.path.join(local_path, 'data', 'Setores.xlsx'))

if __name__ == "__main__":
    main()
