import psycopg2
from tabulate import tabulate

def query(command):
    conn = None

    try:
        conn = psycopg2.connect(
            host="localhost",
            database="productsdb",
            user="nabson",
            password="pass")
        cur = conn.cursor()
        
        cur.execute(command)
        
        result = cur.fetchall()

        cur.close()
        conn.commit()

        return result

    except psycopg2.DatabaseError as error:
        print("** Erro ao executar a consulta:", error)

    finally:
        if conn is not None:
            conn.close()


def main():
    
    dashboard_queries = {
        'a': """
                (
                    SELECT 'MAIOR' classe, r.*
                    FROM products p
                    JOIN reviews r ON r.productasin = p.asin
                    WHERE p.asin = '{}'
                    ORDER BY r.helpful DESC, r.rating DESC
                    LIMIT 5
                )
                UNION
                (
                    SELECT 'MENOR' classe ,r.*
                    FROM products p
                    JOIN reviews r ON r.productasin = p.asin
                    WHERE p.asin = '{}'
                    ORDER BY r.helpful DESC, r.rating ASC
                    LIMIT 5
                )
                ORDER BY classe
                LIMIT 10;
             """,

        'b': """
                SELECT similar_prod.*
                FROM products p
                JOIN similarproducts sp ON sp.productasin = p.asin
                JOIN products similar_prod ON similar_prod.asin = sp.similarproductasin
                WHERE similar_prod.salesrank < p.salesrank AND p.asin = '{}';
             """,

        'c': """
                SELECT reviewDate, AVG(rating) 
                FROM reviews 
                GROUP BY productasin, reviewDate HAVING productasin = '{}' 
                ORDER BY reviewDate;
             """,

        'd': """
                SELECT asin, title, groupname, salesrank, rows 
                FROM 
                    (
                        SELECT *, ROW_NUMBER() 
                        OVER 
                            (
                                PARTITION BY groupname 
                                ORDER BY 
                                CASE WHEN salesrank <= 0 THEN 1 ELSE 0 END, 
                                salesrank ASC
                            ) 
                        AS rows FROM products 
                        WHERE salesrank IS NOT NULL
                    ) 
                AS aux 
                WHERE rows <= 10;
             """,

        'e': """
                SELECT asin, title, groupname, avg_product_rating 
                FROM 
                    (
                        SELECT asin, title, groupname, AVG(rating) avg_product_rating 
                        FROM products p 
                        INNER JOIN reviews r ON r.productasin = p.asin AND r.rating > 3 AND r.helpful > 0
                        GROUP BY asin
                    ) 
                AS aux_reviews
                ORDER BY avg_product_rating DESC
                LIMIT 10;
             """,
        
        'f': """
                SELECT c.name, AVG(r.rating) avg
                FROM categories c
                JOIN categories_hierarchy ch ON ch.categoriesId = c.id
                JOIN reviews r ON r.productAsin = ch.hierarchyProductAsin AND rating > 3 AND helpful > 0
                GROUP BY c.name
                ORDER BY avg DESC
                LIMIT 5;
             """,

        'g': """
                SELECT * 
                FROM 
                    (
                        SELECT *, ROW_NUMBER() 
                        OVER 
                            (
                                PARTITION BY groupname 
                                ORDER BY count_customer_reviews DESC
                            ) 
                        AS rows 
                        FROM 
                            (
                                SELECT customerid, groupname, COUNT(customerid) count_customer_reviews 
                                FROM products p 
                                INNER JOIN reviews r ON r.productasin = p.asin 
                                GROUP BY customerid, groupname
                            ) 
                        AS aux_reviews
                    ) 
                AS aux WHERE rows <= 10;
             """
    }
    
    print("\nBem-vindo ao Dashboard :)\n")

    print("\n------------------------------------------------------------------------------------\n")

    print("\nDados do produto utilizado para as queries:\n")

    result = query("SELECT * FROM products WHERE asin = '0966498011';")

    print("ASIN: {}\nID: {}\nTitulo: {}\nRank de vendas: {}\nGrupo: {}\n".format(result[0][0], result[0][1], result[0][2], result[0][3], result[0][4]))

    print("Informação adicional: O grupo interpretou avaliações como úteis positivas quando a nota dela é maior que 3 e a utilidade dela é maior que 0.")

    print("\n\n a - Dado produto, listar os 5 comentários mais úteis e com maior avaliação e os 5 comentários mais úteis e com menor avaliação.\n\n")
    
    result = query(dashboard_queries['a'].format("0966498011", "0966498011"))

    print("Mais útil e com a MAIOR avaliação\n")

    print(tabulate(result[:5], headers=['Classe', 'ASIN', 'Data', 'ID do Usuário', 'Número', 'Nota', 'Votos', 'Útil']))
    
    print("\nMais útil e com a MENOR avaliação\n")

    print(tabulate(result[5:], headers=['Classe','ASIN','Data','ID do Usuário', 'Número', 'Nota', 'Votos', 'Útil']))
    
    print("\n\n b - Dado um produto, listar os produtos similares com maiores vendas do que ele.\n\n")
    result = query(dashboard_queries['b'].format("0966498011"))

    print(tabulate(result, headers=['ASIN', 'ID', 'Titulo', 'Rank de vendas', 'Grupo']))

    print("\n\n c - Dado um produto, mostrar a evolução diária das médias de avaliação ao longo do intervalo de tempo coberto no arquivo de entrada.\n\n")
    result = query(dashboard_queries['c'].format("0966498011"))

    print(tabulate(result, headers=['Data', 'Média de Avaliações'])) 

    print("\n\n d - Listar os 10 produtos líderes de venda em cada grupo de produtos.\n\n")
    result = query(dashboard_queries['d'])

    print(tabulate(result, headers=['ASIN', 'Titulo', 'Grupo', 'Rank de Vendas', 'Rank dentro do Grupo']))

    print("\n\n e - Listar os 10 produtos com a maior média de avaliações úteis positivas por produto.\n\n")
    result = query(dashboard_queries['e'])

    print(tabulate(result, headers=['ASIN', 'Titulo', 'Grupo', 'Média de Avaliações'])) 

    print("\n\n f - Listar a 5 categorias de produto com a maior média de avaliações úteis positivas por produto.\n\n")
    result = query(dashboard_queries['f'])

    print(tabulate(result, headers=['Nome da Categoria', 'Média de Avaliação']))

    print("\n\n g - Listar os 10 clientes que mais fizeram comentários por grupo de produto.\n\n")
    result = query(dashboard_queries['g'])

    print(tabulate(result, headers=['ID do Usuário', 'Nome do Grupo', 'Número de Comentários', 'Rank dentro do Grupo']))

    print("\n------------------------------------------------------------------------------------\n")

    print("\nObrigado por utilizar o Dashboard :)\n")


if __name__ == "__main__":
    main()