import pandas as pd
from modules.conector_postgre import interface_db
from modules.conector_mysql import Interface_mysql
from modules.conector_cassandra import Conection
from cassandra.cluster import Cluster

if __name__ == "__main__":
    
  
    
    while True: 
        
            selecao = (input("\n[1]- Popular Banco de Dados sql "
                    "\n[2]- Importar dados banco sql "
                    "\n[3]- Exportar dados para o Cassandra "                
                    "\n0- Sair \n"
                    ))
        
            if selecao == "1":
                escolha = input("Importar para? Postgre [p] ou workbanch [w]: ")
                if escolha == "p":
                    inserir_lista_p = interface_db("localhost","old_tech","postgres","soulcode123")
                    inserir_lista_p.inseri_massa()
                elif escolha == "w":
                    inserir_lista_w = Interface_mysql("root","Ivanrib*1","127.0.0.1","OLDTech_Ltda_filial")
                    inserir_lista_w.inserir_tudo()
            
            if selecao == "2":
                escolha = input("Exportar de? Postgre [p] ou workbanch [w]: ")
                if escolha == "p":
                    buscar_lista_p = interface_db("localhost","old_tech","postgres","soulcode123")
                    dados = buscar_lista_p.buscar_lista()               
                                            
                            
                elif escolha == "w":
                    buscar_lista_w = Interface_mysql("root","admin","35.198.57.217","santaclaus")
                    query = "select * from entries;"
                    values = buscar_lista_w.consultar(query)
                    # session = Cluster().connect('oldtech_ltda_matriz')
                    # print(values)
                    # # nova_lista = []
                    # for item in values:
                    #     query =  f"INSERT INTO vendas_matriz(codigo, nota_fiscal, vendedor, total) VALUES (uuid(), {item[0]}, '{item[1]}' , {item[2]});"
                    #     cassandra_db = Conection('oldtech_ltda_matriz')
                    #     cassandra_db.inserir_cassandra(query, session)
                    print(values)
                        
                            
            if selecao == "3":
                session = Cluster().connect('oldtech_ltda_matriz')
                buscar_cassandra = Conection('oldtech_ltda_matriz')
                buscar_cassandra.buscar_tudo()        
            
            if selecao == "0":
                break  
            
            
            
    
    
    
    
    
    # buscar_lista = Interface_mysql("root","Ivanrib*1","127.0.0.1","OLDTech_Ltda_filial")
    # buscar_lista.buscar_itens()
    
    
    # inserir_lista = Interface_mysql("root","Ivanrib*1","127.0.0.1","OLDTech_Ltda_filial")
    # inserir_lista.inserir_tudo()



    