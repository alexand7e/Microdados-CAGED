import pandas as pd
import numpy as np
from decimal import Decimal

class CagedUtils:

    def __init__(self) -> None:
        pass

    def classificar_mes(self, mês = None, return_list = False):
        if isinstance(mês, str):
            mês = int(mês)
        month_ = {
            1: "janeiro",
            2: "fevereiro",
            3: "março",
            4: "abril",
            5: "maio",
            6: "junho",
            7: "julho",
            8: "agosto",
            9: "setembro",
            10: "outubro",
            11: "novembro",
            12: "dezembro"
        }
        if return_list:
            return_list = [{f"{ano}{str(mês).zfill(2)}": f"{nome_mes} de {ano}"}
                        for ano in range(2020, 2025)
                        for mês, nome_mes in month_.items()]
            return return_list
        else:
            return month_.get(mês, 'error')
        
    def classificar_ufs(self, codigo_ibge=None):
        codigos_estados = {
            11: "Rondônia",
            12: "Acre",
            13: "Amazonas",
            14: "Roraima",
            15: "Pará",
            16: "Amapá",
            17: "Tocantins",
            21: "Maranhão",
            22: "Piauí",
            23: "Ceará",
            24: "Rio Grande do Norte",
            25: "Paraíba",
            26: "Pernambuco",
            27: "Alagoas",
            28: "Sergipe",
            29: "Bahia",
            31: "Minas Gerais",
            32: "Espírito Santo",
            33: "Rio de Janeiro",
            35: "São Paulo",
            41: "Paraná",
            42: "Santa Catarina",
            43: "Rio Grande do Sul",
            50: "Mato Grosso do Sul",
            51: "Mato Grosso",
            52: "Goiás",
            53: "Distrito Federal"
        }
        # Verificando se o código do IBGE é um dígito e está no dicionário
        if isinstance(codigo_ibge, int) and codigo_ibge in codigos_estados:
            return codigos_estados[codigo_ibge]
        else:
            return 'Código inválido'
    
    def classificar_faixa_etaria(self, idade: int):
        if isinstance(idade, (int, float)):
            if 0 <= idade <= 17:
                return "Até 17 anos"
            elif 17 < idade <= 24:
                return "18 a 24 anos"
            elif 24 < idade <= 29:
                return "25 a 29 anos"
            elif 29 < idade <= 39:
                return "30 a 39 anos"
            elif 39 < idade <= 49:
                return "40 a 49 anos"
            elif 49 < idade <= 64:
                return "50 a 64 anos"
            elif idade > 64:
                return "Mais de 65 anos"
            else:
                return 'Idade inválida'  # Para idades negativas
        elif isinstance(idade, pd.Series):
            return idade.apply(self.classificar_faixa_etaria)
        else:
            return 'Não Identificado'
        

    def classificar_escolaridade(self, escolaridade: str, return_list: bool = False):
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
        
        
    # Método para realizar a classificação setorial a partir das seções da CNAE 2.0 
    def classificar_grupamento(self, codigo: str, return_list: bool = False):
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
        

    # Método para classificar o período
    def classificar_período(self, competencia_caged: str):
        competencia_caged = pd.to_datetime(f'{competencia_caged[:4]}-{competencia_caged[-2:]}-01')
        return pd.Timestamp(competencia_caged).date()


    # Método para ajustar valores float (principalmente o de valorsaláriofixo)
    def ajustar_coluna_decimal(self, x):
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


    # Método para calcular a soma segura ~ verificando o tipo dos dados
    def custom_sum(self, x):
        numeric_values = [val for val in x if pd.notna(val) and isinstance(val, (int, float))]

        if numeric_values:
            return round(sum(numeric_values), 2)  # Calcula a soma
        else:
            return np.nan


    # Método para calcular a média segura
    def custom_mean(self, x):
        numeric_values = [val for val in x if pd.notna(val) and isinstance(val, (int, float))]

        if numeric_values:
            mean_value = Decimal(np.nanmean(numeric_values))
            return round(mean_value, 2)  # Calcula a média
        else:
            return np.nan


    #
    def mes_analisado(self, mes_escolhido: int, ano_base: int, ano_atual: int, mes_atual: int):
        meses = list(range(1, 13))
        meses_selecionados = []

        # Verifica se o mês escolhido está no intervalo válido.
        if 1 <= mes_escolhido <= 12:
            if ano_base < ano_atual:
                # Se o ano base for anterior ao ano atual, inclui todos os meses após o mês escolhido.
                meses_selecionados = meses[mes_escolhido - 1:]
            elif ano_base == ano_atual:
                # Se o ano base for igual ao ano atual, inclui os meses até o mês atual.
                meses_selecionados = meses[mes_escolhido - 1:mes_atual]
            # Se o ano base for maior que o ano atual, isso implica uma configuração inválida.
            # O método pode retornar uma lista vazia ou gerar uma exceção, dependendo da preferência.

        return meses_selecionados

    #
    def ano_analisado(self, ano_base):
        anos = [2020, 2021, 2022, 2023, 2024]
        i_ano = anos.index(ano_base)
        
        return anos[i_ano:]
        
