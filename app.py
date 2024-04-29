
import tkinter as tk
from tkinter import filedialog, Listbox, messagebox

class App:
    def __init__(self, root):
        self.root = root
        self.root.title("Selecionador de Elementos e Diretórios")
        self.root.iconbitmap(r"C:/Users/usuario/OneDrive/Documentos/R/Projeto CAGED/Python/icon.ico")
        self.root.config(bg='light blue')

        # Definir o tamanho da janela
        largura = 600  # Largura em pixels
        altura = 400   # Altura em pixels
        self.root.geometry(f'{largura}x{altura}')

        # Centralizar a janela na tela
        largura_tela = self.root.winfo_screenwidth()
        altura_tela = self.root.winfo_screenheight()
        x = (largura_tela // 2) - (largura // 2)
        y = (altura_tela // 2) - (altura // 2)
        self.root.geometry(f'+{x}+{y}')


        self.diretorio_selecionado = None
        self.item_selecionado = None

        self.criar_primeiro_frame()
        self.criar_segundo_frame()

        self.segundo_frame.pack_forget()  # Inicialmente, ocultamos o segundo frame

    def criar_primeiro_frame(self):
        self.primeiro_frame = tk.Frame(self.root, bg='light grey')
        self.primeiro_frame.pack(padx=10, pady=10)

        self.lista = Listbox(self.primeiro_frame)
        self.lista.insert(1, "Item 1")
        self.lista.insert(2, "Item 2")
        self.lista.insert(3, "Item 3")
        self.lista.pack(pady=(0, 10))
        self.lista.bind('<<ListboxSelect>>', self.item_selecionado)

        self.botao_diretorio = tk.Button(self.primeiro_frame, text="Escolher Diretório", command=self.escolher_diretorio)
        self.botao_diretorio.pack(pady=(0, 10))

        self.label_selecao = tk.Label(self.primeiro_frame, text="", bg='light grey', font=('Arial', 10))
        self.label_selecao.pack()

        self.label_diretorio = tk.Label(self.primeiro_frame, text="", bg='light grey', font=('Arial', 10))
        self.label_diretorio.pack()

        self.botao_avancar = tk.Button(self.primeiro_frame, text="Avançar", command=self.avancar)
        self.botao_avancar.pack()

    def criar_segundo_frame(self):
        self.segundo_frame = tk.Frame(self.root, bg='light grey')
        # Adicione widgets ao segundo frame aqui, por exemplo:
        tk.Label(self.segundo_frame, text="Este é o segundo frame", bg='light grey').pack()

    def avancar(self):
        if self.diretorio_selecionado:
            self.primeiro_frame.pack_forget()  # Oculta o primeiro frame
            self.segundo_frame.pack(padx=10, pady=10)  # Mostra o segundo frame
        else:
            messagebox.showwarning("Atenção", "Selecione um diretório e um item da lista para continuar.")

    def escolher_diretorio(self):
        diretorio = filedialog.askdirectory()
        if diretorio:
            self.label_diretorio.config(text=diretorio)
            self.diretorio_selecionado = diretorio

    def item_selecionado(self, event):
        widget = event.widget
        if widget.curselection():
            selecionado = widget.curselection()
            valor = widget.get(selecionado[0])
            self.label_selecao.config(text=valor)
            self.item_selecionado = valor

    def concluir_execução(self):
        pass

    

    # Métodos escolher_diretorio e item_selecionado permanecem os mesmos

if __name__ == "__main__":
    root = tk.Tk()
    app = App(root)
    root.mainloop()

