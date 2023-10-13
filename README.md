# Repositório de Organização de Microdados CAGED

Este repositório contém scripts em Python para a organização de microdados do CAGED (Cadastro Geral de Empregados e Desempregados). Os scripts e arquivos aqui presentes são projetados para facilitar a leitura, formatação e importação de dados do CAGED a partir de um servidor FTP do governo para o seu host local.

## Conteúdo do Repositório

O repositório contém os seguintes arquivos e diretórios:

1. `microdados_caged.py`: Este arquivo contém funções que são responsáveis pela leitura e formatação dos arquivos do CAGED. Essas funções permitem processar os dados brutos e torná-los mais utilizáveis para análises posteriores.

2. `notebook.ipynb`: Este é um arquivo Jupyter Notebook onde você pode executar as funções do `microdados_caged.py` e salvar os dados processados. O notebook é uma ótima ferramenta para realizar análises exploratórias e visualizações de dados.

3. `FTP_caged.py`: Este arquivo contém o código necessário para fazer o download dos microdados do servidor FTP do governo e importá-los para o seu host local. Ele é essencial para manter seus dados atualizados.

## Como Usar

Para começar a utilizar este repositório, siga as etapas abaixo:

1. Clone este repositório para o seu ambiente local:

 **git clone https://github.com/AlexandreDevSK/Microdados-CAGED**

2. Certifique-se de que você tenha as dependências necessárias instaladas. Você pode fazer isso executando:

 **pip install -r requirements.txt**

3. Certifique-se também de alterar as variáveis que determinam o diretório de importação e exportação dos dados.
 
4. Execute o arquivo `FTP_caged.py` para baixar os microdados do servidor FTP do governo. Você pode configurar as opções e parâmetros necessários no arquivo.

5. Use o arquivo `microdados_caged.py` para ler e formatar os dados conforme necessário. Consulte a documentação das funções neste arquivo para obter mais detalhes.

6. Utilize o arquivo `notebook.ipynb` para realizar análises e visualizações de dados com os dados processados.

## Contribuições

Contribuições para melhorias neste repositório são bem-vindas. Sinta-se à vontade para abrir problemas (issues) e enviar solicitações de pull (pull requests) conforme necessário.

## Licença

Este projeto é licenciado sob a licença MIT. Consulte o arquivo [LICENSE](LICENSE) para obter detalhes.

Agradecemos por usar este repositório. Esperamos que ele seja útil para a sua organização e análise de dados do CAGED.

