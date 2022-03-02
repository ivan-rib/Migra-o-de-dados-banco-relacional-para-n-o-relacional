# from modules.conector_cassandra import Conection
import mysql.connector 
import pandas as pd


class Interface_mysql():
    usuario, senha, host, banco, con = "", "", "", "", ""
    
    def __init__(self, usuario, senha, host, banco):
        
        """Construtor da classe interface_db

        Args:
            usuario (string): usuario para conexao ao banco
            senha (string): senha para acesso ao banco
            host (string): string contendo o endereco do host
            banco (string): string contendo o nome do banco
        """
        try:
            self.usuario = usuario
            self.senha = senha
            self.host = host
            self.banco = banco
        except Exception as e:
            print(str(e))
            
    
    def conectar(self):
        """Função que executa a conexção com o Banco de Dados

        """
        try:
            con = mysql.connector.connect(user=self.usuario, password=self.senha, host=self.host, database=self.banco)
            cursor = con.cursor()
            return con, cursor
        except Exception as e:
            print(str(e))
            
    def desconectar(self, con, cursor):
        """Função que faz o programa se desconectar do Banco de Dados

        Args:
            con (CMySQLConnection): Representa uma conexão aberta com o MySQL
            cursor (CMySQLCursor): Abre um cursor no MySQL
        """
        try:
            cursor.close()
            con.commit()
            con.close()
        except Exception as e:
            print(str(e))
               
    def consultar(self, query):
        """Executa Queries SQL

        Args:
            parametros (string): String com todos os parâmetros necessários para realizar uma Query SQL

        Returns:
            [cursor.fetchall()]: Mostra uma busca executada em todas as linhas da tabela pesquisada
        """
        
        try:
            con, cursor = self.conectar() # con, cursor= processo conctar mysql.
            cursor.execute(query) # Realiza a query
            
            return cursor.fetchall() # armazena o resultado
        except Exception as e:
            print(str(e))
        finally:
            self.desconectar(con, cursor) # fecha o cursor
                       
            
    def inserir(self, tabela):
        """Executa uma Query de inserção de dados no Banco de Dados MySQL

        Args:
            onde (string): Em qual tabela os dados deverão ser inseridos
            o_que (string): Quais colunas/atributos da tabela serão afetados
            valores (string): Quais dados deverão ser inseridos
        """
        try:
            con, cursor = self.conectar()
            query = f"INSERT INTO vendas_filial(nota_fiscal,vendedor, total) values {tabela}" 
            cursor.execute(query)
            return cursor.fetchall()
                        
        except Exception as e:
            print(e)
        finally:
            self.desconectar(con, cursor)
            
    def alterar(self, tabela, campo, valor, condicao):
        """Executa uma Query de alteração de dados em uma tabela do Banco de Dados MySQL

        Args:
            tabela (string): Qual tabela sofrerá a alteração
            campo (string): Qual coluna/atributo da tabela será modificado
            valor (string): Quais dados permanecerão na tabela
            condicao (string): Condições que devem ser atendidas para que a alteração seja efetuada
        """
        try:
            con, cursor = self.conectar()
            query = "UPDATE " + tabela + " SET " + campo + " = " + valor + " WHERE " + condicao + ";"
            cursor.execute(query)
            cursor.close()
        except Exception as e:
            print(e)
        finally:
            self.desconectar(con, cursor)
            
    def buscar_itens(self, query):
        
        busca = Interface_mysql("root","admin","127.0.0.1","santaclaus")        
        lista = []
        # tabela = input("Digite o nome da tabela a ser buscada: ")
        query = (f"SELECT * from entries")
        a = busca.consultar(query)              
        query1 = pd.DataFrame(a)
        query1 = query1.dropna(axis=0)
        query1.columns = ['nota_fiscal','vendedor','total']
        # query1 = query1.drop(columns=['codigo'])
        lista.append(query1)
        return lista        
        
        
    def inserir_tudo(self):
        
        inserscao = Interface_mysql("root","admin","35.198.57.217","santaclaus") 
        arquivo = input("Digite o nome do arquivo a ser tratado: ")
        arquivo_exp = input("Digite o nome do arquivo tratado: ")
        dados_vendas = pd.read_csv(f"./{arquivo}")
        dados_banco = pd.DataFrame(dados_vendas)
        dados_banco = dados_banco.dropna(axis = 0)
        dados_csv = dados_banco.to_csv(f"{arquivo_exp}", index=False)
        dados_vendas = pd.read_csv(f"{arquivo_exp}")
        dados_csv = dados_vendas
    
        for i,tabela in dados_csv.iterrows():
            tabela = (tabela['nota_fiscal'],tabela['vendedor'],tabela['total'])
            inserscao.inserir(tabela)
           