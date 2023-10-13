from ftplib import FTP

# Configurações do servidor FTP
ftp_host = 'ftp.mtps.gov.br' 
ftp_user = 'Anonymous'
ftp_pass = ''
remote_directory = '/pdet/microdados/NOVO CAGED/2023/'

# Diretório local onde você deseja salvar os arquivos baixados
local_directory = 'C:/Users/Alexandre/OneDrive/Documentos/R/Projeto CAGED/Files - Microdata/Zip'

# Extensão de arquivo desejada
file_extension = '.7z'

# Função para baixar um arquivo
def download_file(ftp, remote_directory, local_directory, filename):
    remote_path = remote_directory + '/' + filename
    local_path = local_directory + '/' + filename
    with open(local_path, 'wb') as local_file:
        ftp.retrbinary('RETR ' + remote_path, local_file.write)

try:
    # Conectar ao servidor FTP
    ftp = FTP(ftp_host)
    ftp.login()  # Autenticar no servidor FTP

    # Mudar para o diretório remoto
    ftp.cwd(remote_directory)

    # Listar as pastas no diretório remoto
    folder_list = [item for item in ftp.nlst() if ftp.nlst(item)]

    # Iterar pelas pastas e baixar arquivos nelas
    for folder in folder_list:
        folder_files = ftp.nlst(folder)
        for filename in folder_files:
            if filename.endswith(file_extension):
                download_file(ftp, folder, local_directory, filename)

    # Fechar a conexão FTP
    ftp.quit()

    print('Download concluído!')

except Exception as e:
    print(f"Ocorreu um erro: {e}")
