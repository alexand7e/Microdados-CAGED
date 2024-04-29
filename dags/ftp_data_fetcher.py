from ftplib import FTP
import tempfile
import py7zr
import os
import shutil
import atexit

import functools

def debug(func):
    """Decorador para registrar informações sobre chamadas de função para debugging."""
    @functools.wraps(func)
    def wrapper_debug(*args, **kwargs):
        args_repr = [repr(a) for a in args]
        kwargs_repr = [f"{k}={v!r}" for k, v in kwargs.items()]
        signature = ", ".join(args_repr + kwargs_repr)
        print(f"Chamando: {func.__name__}({signature})")
        value = func(*args, **kwargs)
        print(f"{func.__name__!r} retornou {value!r}")
        return value
    return wrapper_debug


class FTPDownloader:
    def __init__(self, local_directory: str = None, file_extension: str = None, caged_year: int = None, caged_month: int = None):
        self.ftp_host = 'ftp.mtps.gov.br'
        self.ftp_user = 'Anonymous'
        self.ftp_pass = ''
        self.date_directory = f'/{caged_year}/{caged_year}{str(caged_month).zfill(2)}' if caged_year and caged_month else ''
        self.remote_directory = "pdet/microdados/NOVO CAGED"
        self.file_extension = file_extension
        self.local_directory = local_directory if local_directory else tempfile.gettempdir()
        # atexit.register(self.cleanup)

    @debug
    def download_file_callback(self, filename):
        """Callback function to append data to the file."""
        def callback(data):
            with open(filename, 'ab') as file:  # Append binary mode
                file.write(data)
        return callback

    @debug
    def download_specific_file(self, target_file_name, is_dict=False):
        """Downloads a specific file using class settings, adjusting for 'is_dict'."""
        if is_dict:
            directory_path = self.remote_directory  # Specific path for the dictionary file
            target_file_name = "Layout Não-identificado Novo Caged Movimentação.xlsx"
        else:
            directory_path = f"{self.remote_directory}/{self.date_directory}".strip('/')

        local_filename = os.path.join(self.local_directory, target_file_name)

        # Check if the file already exists
        if os.path.exists(local_filename):
            print(f"File already exists: {local_filename}. Skipping download.")
            return

        try:
            with FTP(self.ftp_host) as ftp:
                ftp.encoding = 'iso-8859-1'
                ftp.login(self.ftp_user, self.ftp_pass)
                ftp.cwd(directory_path)
                print(f"Attempting to download: {directory_path}/{target_file_name}")
                with open(local_filename, 'wb') as local_file:
                    ftp.retrbinary(f'RETR {target_file_name}', local_file.write)
                print(f"File successfully downloaded: {local_filename}")
        except Exception as e:
            print(f"Error downloading file: {e}")

    @debug
    def list_files(self) -> list:
        """Lista arquivos em um diretório remoto com a extensão desejada."""
        files = []
        try:
            with FTP(self.ftp_host) as ftp:
                ftp.encoding = 'iso-8859-1'
                ftp.login(self.ftp_user, self.ftp_pass)
                ftp.cwd(f"{self.remote_directory}/{self.date_directory}".strip('/'))
                ftp.retrlines('LIST', lambda x: files.append(x.split()[-1]))
        except Exception as e:
            print(f"Erro ao listar arquivos: {e}")
        return [f for f in files if f.endswith(self.file_extension)]


    @debug
    def download_and_extract(self, files, extractor) -> None:
        """Baixa e extrai uma lista de arquivos."""
        for filename in files:
            self.download_specific_file(filename)
            file_path = os.path.join(self.local_directory, filename)
            extractor.extract(file_path)

    @debug
    def delete_downloaded_files(self) -> None:
        """Exclui os últimos 3 arquivos baixados do diretório local."""
        files = [file for file in os.listdir(self.local_directory) if file.endswith(self.file_extension)]
        files.sort(key=lambda x: os.path.getmtime(os.path.join(self.local_directory, x)))
        
        files_to_delete = files[-3:]
        
        for file in files_to_delete:
            file_path = os.path.join(self.local_directory, file)
            os.remove(file_path)
            print(f'Arquivo excluído: {file_path}')

    # @debug
    # def cleanup(self):
    #     """Remove o diretório local e todos os seus arquivos."""
    #     try:
    #         shutil.rmtree(self.local_directory)
    #         print(f"Diretório {self.local_directory} removido com sucesso.")
    #     except Exception as e:
    #         print(f"Erro ao remover o diretório {self.local_directory}: {e}")

class SevenZipExtractor:
    def __init__(self, target_directory):
        self.target_directory = target_directory

    def extract(self, file_path):
        """Extrai um arquivo .7z para o diretório de destino."""
        with py7zr.SevenZipFile(file_path, mode='r') as z:
            z.extractall(self.target_directory)

