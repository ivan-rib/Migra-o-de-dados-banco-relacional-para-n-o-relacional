import psycopg2
import pandas as pd

"""[conexão com o banco postgre]

Returns:
    [type]: [tentando a conexão]
"""
class interface_db:
    host, database, user, password = "", "", "", "" 
    
    def __init__(self, host, database, user, password):
        # """Construtor da classe interface_db

        # Args:
        #     usuario (string): usuario para conexao ao banco
        #     senha (string): senha para acesso ao banco
        #     host (string): string contendo o endereco do host
        #     banco (string): string contendo o nome do banco
        # """
        try:
            self.host = host
            self.database = database
            self.user = user            
            self.password = password
        except Exception as e:
            print(str(e))
    
    def conectar(self):
        try:
            conn = psycopg2.connect(host=self.host, database=self.database, user=self.user, password=self.password,)
            cursor = conn.cursor()
            return conn, cursor
        except Exception as e:
            print(str(e))
            
    def desconectar(self, conn, cursor):
        try:
            cursor.close()
            conn.commit()
            conn.close()
        except Exception as e:
            print(str(e))
    
    def selecionar(self,comandospostgre):
        try:
            conn, cursor = self.conectar()
            query = comandospostgre
            cursor.execute(query)
            return cursor.fetchall()
        except Exception as e:
            print(str(e))
        finally:
            self.desconectar(conn, cursor)
            
    def inserir(self,comandospostgre):
        try:
            conn, cursor = self.conectar()
            insert = comandospostgre
            cursor.execute(insert)                    
            return cursor.fetchall()
        except Exception as e:
            print(str(e))
        finally:
            self.desconectar(conn, cursor)

    def buscar_lista(self):
        busca = interface_db("localhost","old_tech","postgres","soulcode123")        
        lista = []
        tabela = input("Digite o nome da tabela a ser buscada: ")
        query = (f"SELECT * from {tabela}")
        a = busca.selecionar(query)              
        query1 = pd.DataFrame(a)
        query1 = query1.dropna(axis=0)
        query1.columns = ['nota_fiscal','vendedor','total']
        lista.append(query1)
        print(lista)
        return lista
        
    def inseri_massa(self):
        
        inserscao = interface_db("localhost","old_tech","postgres","soulcode123")
        arquivo = input("Digite o nome do arquivo a ser tratado: ")
        arquivo_exp = input("Digite o nome do arquivo tratado: ")
        dados_vendas = pd.read_csv(f"./{arquivo}")
        dados_banco = pd.DataFrame(dados_vendas)
        dados_banco = dados_banco.dropna(axis = 0)
        dados_csv = dados_banco.to_csv(f"{arquivo_exp}", index=False)
        dados_vendas = pd.read_csv(f"{arquivo_exp}")
        #  a = inserscao.inserir(dados_banco)
        dados_csv = dados_vendas
            
        for i,row in dados_csv.iterrows():
            row = """ INSERT INTO  vendas_filial(vendedor,total) values('%s','%f');
                    """% (row['vendedor'],row['total'])
            row = inserscao.inserir(row)
            
        
    

            
    