from cassandra.cluster import Cluster
import pandas as pd

class Conection():
    
    def __init__(self, database):
        self.database = database
    
    
    def inserir_cassandra(self,query, session):
        try:      
            session.execute(query)
            
        except Exception as e:
            print(str(e))
            
    def buscar_tudo(self):
        clstr=Cluster()
        clstr.session = clstr.connect(self.database)              
        tabela = input("Digite o nome da tabela a ser buscada: ")
        query = f"SELECT * FROM {tabela};" 
        dados = clstr.session.execute(query)
        #retorna dado tipo resultSet do Cassandra  
        dados1 = []
        for i in dados:
            dados1.append(i)
        dados2 = pd.DataFrame(dados1)
        dados2 = dados2.drop(columns=['codigo'])
        print(dados2)  
                     
        
    
# dados = pd.read_csv('./Sistema_B_NoSQL.csv')
# clstr=Cluster()
# session=clstr.connect('oldtech_ltda_matriz')
# dados['vendedor'].fillna('Sem Identificação', inplace=True)
# for i,row in dados.iterrows():
#    inserir = """INSERT INTO vendas_matriz (codigo, nota_fiscal, vendedor, total) values(uuid(), %i,'%s',%f);
#       """% (row['nota_fiscal'],row['vendedor'],row['total'])
#    session.execute(inserir)
# query = "SELECT * FROM vendas_matriz;" 
# #retorna dado tipo resultSet do Cassandra  
# dados = session.execute(query)
# dados1 = []
# for i in dados:
#     dados1.append(i)
# dados2 = pd.DataFrame(dados1)
# del dados2['codigo']
# print(dados2)
# # dados2.dropna(axis=1, inplace = True)
# # print(dados2.isnull().sum())
# # dados2['vendedor'].fillna(' ', inplace=True)#fillna = preenche linha com vazio / implace=True = incorpora a alteração
# print(dados2.isnull().sum())
# # print(dados2['total'].describe())
# # dados2['total'].hist()
# # plt.show()