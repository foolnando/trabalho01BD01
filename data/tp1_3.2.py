import psycopg2
import re

# definicao das expressoes regulares referente os dados de interesse presentes no arquivo
reg_dic = {
    'id': re.compile(r'Id:   (?P<id>\d+)\n'),
    'asin': re.compile(r'ASIN: (?P<asin>.*)\n'),
    'title': re.compile(r'title: (?P<title>.*)\n'),
    'group': re.compile(r'group: (?P<group>.*)\n'),
    'salesrank': re.compile(r'salesrank: (?P<salesrank>.*)\n'),
    'similar': re.compile(r'similar: (?P<similar>.*)\n'),
    'categories': re.compile(r'categories: (?P<categories>\d+)\n'),
    'reviews': re.compile(r'reviews: (?P<reviews>.*)\n')
}


# definicao da estrutura que guarda os dados extraidos
CHUNK_DIC = {
        "groups": set(),
        "products": [],
        "reviews": [],
        "categories": {},
        "hierarchy": [],
        "categories_hierarchy": []
    }

# definicao do esquema das tabelas presentes no banco de daddos
def create_schema():
    commands = [
        """
            CREATE TABLE groups (
                name VARCHAR(50),
                CONSTRAINT pk_groups_name PRIMARY KEY (name)
            );
        """,
        """
            CREATE TABLE products (
                asin CHAR(10),
                id INT UNIQUE,
                title VARCHAR(500),
                salesrank INT,
                groupName VARCHAR(50),
                CONSTRAINT pk_products_asin PRIMARY KEY (asin),
                FOREIGN KEY (groupName) REFERENCES groups(name)
            );
        """,
        """
            CREATE TABLE similarProducts (
                productAsin CHAR(10),
                similarProductAsin CHAR(10),
                PRIMARY KEY (productAsin, similarProductAsin),
                FOREIGN KEY (productAsin) REFERENCES products(asin),
                FOREIGN KEY (similarProductAsin) REFERENCES products(asin)
            );
        """,
        """
            CREATE TABLE reviews (
                productAsin CHAR(10),
                reviewDate DATE,
                customerId CHAR(14),
                number INT,
                rating INT NOT NULL,
                votes INT NOT NULL,
                helpful INT NOT NULL,
                PRIMARY KEY (reviewDate, productAsin, customerId, number),
                FOREIGN KEY (productAsin) REFERENCES products(asin)
            );
        """,
        """
            CREATE TABLE categories (
                id INT,
                name VARCHAR(100),
                CONSTRAINT pk_categories_id PRIMARY KEY (id)
            );
        """,
        """
            CREATE TABLE hierarchy (
                productAsin CHAR(10),
                number INT,
                PRIMARY KEY (productAsin, number),
                FOREIGN KEY (productAsin) REFERENCES products(asin)
            );
        """,
        """
            CREATE TABLE categories_hierarchy (
                hierarchyProductAsin CHAR(10),
                hierarchyNumber INT,
                categoriesId INT,
                rank INT NOT NULL,
                PRIMARY KEY (hierarchyProductAsin, hierarchyNumber, categoriesId),
                FOREIGN KEY (hierarchyProductAsin, hierarchyNumber) REFERENCES hierarchy(productAsin, number),
                FOREIGN KEY (categoriesId) REFERENCES categories(id)
            );
        """,
        """
            CREATE OR REPLACE FUNCTION insert_product() RETURNS TRIGGER AS
                $$
                BEGIN
                    IF NOT EXISTS (SELECT 1 FROM products WHERE asin = NEW.similarProductAsin) THEN
                        INSERT INTO products(asin) VALUES(NEW.similarProductAsin);
                    END IF;
                    RETURN NEW;
                END
                $$
                LANGUAGE PLPGSQL;
        """,
        """
            CREATE TRIGGER similares
                BEFORE INSERT ON similarProducts
                FOR EACH ROW
                EXECUTE PROCEDURE insert_product();
        """
    ]
    print("******* CREATING SCHEMA *******")
    conn = None
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="productsdb",
            user="nabson",
            password="pass"
        )
        cur = conn.cursor()
        
        for command in commands:
            cur.execute(command)
            
        cur.close()
        conn.commit()

        print("> Esquema criado.\n")
    except (Exception, psycopg2.DatabaseError) as error:
        print("** Erro ao criar esquema:", error)
    finally:
        if conn is not None:
            conn.close()


# parser da linha do arquivo, identifica o valor de atributo a ser extraido presente na linha
# recebe como parametro uma linha do arquivo de entrado do parser_file
# retorna a chave (key) e a combinação (match) satifeita por uma das expressões regulares
def parse_line(line):
    for key, rx in reg_dic.items():
        match = rx.search(line)
        if match:
            return key, match
    return None, None


# parser do arquivo que contem os dados a serem extraídos 
# recebe como parametro o diretorio do arquivo a ser parseado
# retorna os dados extraidos do arquivo de entrada
def parse_file(filepath):
    # calculo de divisao dos dados em particoes para evitar o multiplo acesso ao BD
    chunk_size = int(0.1 * 548552)
    chunk_cnt = 1
    chunk_data = {
        "groups": set(),
        "products": [],
        "similarProducts": [],
        "reviews": [],
        "categories": {},
        "hierarchy": [],
        "categories_hierarchy": []
    }

    product_data = ()
    productAsin = -1
    total_products = 0
    product_asins_object = {}

    print("******** PARSING FILE ********")
    print('> Chunk', chunk_cnt)
    print('    [1/4] Carregando dados...')

    # inicio da leitura do arquivo com os dados de interesse
    with open(filepath, 'r') as file_object:
        line = file_object.readline()
        while line:

            key, match = parse_line(line)

            # tratamento do valor de atributo retornado pelo parser_file
            # de acordo com o atributo correspondente ao valor da chave retornada pela Ex. regular
            if key == 'id':
                if (productAsin != -1 ):
                    if (len(product_data) < 5):
                        product_data = product_data + (None,None,None)
                    chunk_data['products'].append(product_data)
                    total_products += 1
                productId = int(match.group('id'))
                product_data = (productId,)

            # tratamento do atributo 'asin'
            elif key == 'asin':
                productAsin = match.group('asin')
                product_asins_object[productAsin] = True
                product_data = product_data + (productAsin,)

            # tratamento do atributo 'title'
            elif key == 'title':
                product_data = product_data + (match.group('title'),)

            # tratamento do atributo 'group'
            elif key == 'group':
                groupName = match.group('group')
                product_data = product_data + (groupName,)
                chunk_data['groups'].add((groupName,))

            # tratamento do atributo 'salesrank'
            elif key == 'salesrank':
                product_data = product_data + (int(match.group('salesrank')),)

            # tratamento do atributo 'similar products'
            elif key == 'similar':
                similar_products_asin = match.group('similar').split('  ')[1:]
                for similar_asin in similar_products_asin:
                    chunk_data['similarProducts'].append((productAsin, similar_asin))

            # tratamento do atributo 'categories'
            elif key == 'categories':
                total_hierarchy = int(match.group('categories'))
                for i in range(total_hierarchy):
                    hierarchy = (productAsin, i)
                    chunk_data['hierarchy'].append(hierarchy)
                    line = re.split('\||\[|\]', file_object.readline())[1:]
                    rank = 1
                    j = 0
                    while j < len(line):
                        categorie_name = line[j]
                        categorie_id = line[j+1]
                        try:
                            categorie_id = int(categorie_id)
                        except (ValueError):
                            categorie_id = line[j+3]
                            j+=2
                        chunk_data['categories_hierarchy'].append((hierarchy[0], hierarchy[1], categorie_id, rank))
                        chunk_data['categories'][categorie_id] = None if categorie_name == '' else categorie_name
                        rank+=1
                        j+=3

            # tratamento do atributo 'reviews'
            elif key == 'reviews':
                reviews = match.group('reviews')
                total_reviews = int(reviews.split(' ')[4])
                for i in range(total_reviews):
                    line = list(filter(lambda a: a != "", file_object.readline().split(' ')))
                    review_date = line[0]
                    customer_id = line[2]
                    rating = int(line[4])
                    votes = int(line[6])
                    helpful = int(line[8])
                    chunk_data['reviews'].append((productAsin, review_date, customer_id, i, rating, votes, helpful))

            # verificando se o valor de dados de chunks e' igual tamanho total dos dados
            if (total_products >= chunk_cnt * chunk_size):
                print('    [2/4] Dados carregados.')
                print('    [3/4] Escrevendo dados no banco...')
                chunk_data['groups'] = list(chunk_data['groups'])
                chunk_data['categories'] = [(id, name) for id, name in chunk_data['categories'].items()]
                chunk_data['products'] = chunk_data['products']
                insert_products(chunk_data)

            # iterando sobre o numero de chunks
                print('    [4/4] Dados escritos.')
                chunk_cnt += 1
                chunk_data = {
                        "groups": set(),
                        "products": [],
                        "similarProducts": [],
                        "reviews": [],
                        "categories": {},
                        "hierarchy": [],
                        "categories_hierarchy": []
                    }
                print('> Chunk ', chunk_cnt)
                print('    [1/4] Carregando dados...')

            line = file_object.readline()

        # tratamente de produtos dos campos similar products mas que nao estao listados na base
        # adiciona a lista de produtos apenas com as informacoes de Id e Asin
        if (len(product_data) < 5):
            product_data = product_data + (None,None,None)
        chunk_data['products'].append(product_data)
        total_products += 1

        # 'fim' de um chunck e chamada da funcao de escrita no banco          
        print('    [2/4] Dados carregados.')
        print('    [3/4] Escrevendo dados no banco...')
        chunk_data['groups'] = list(chunk_data['groups'])
        chunk_data['categories'] = [(id, name) for id, name in chunk_data['categories'].items()]
        insert_products(chunk_data)
        print('    [4/4] Dados escritos.')
        print('******** PARSER END ********')


# insercao dos dados extraidos nas relacoes 'products', 'categories_hierarchy', 'categories', 'reviews' e 'groups'
# recebe como parametro os dados que 
def insert_products(data):
    insert_commands = {
        "groups": """
            INSERT INTO groups(name) 
            VALUES (%s) 
            ON CONFLICT ON CONSTRAINT pk_groups_name 
            DO NOTHING
        """,
        "products": """
            INSERT INTO products(id, asin, title, groupName, salesrank) 
            VALUES (%s, %s, %s, %s, %s) 
            ON CONFLICT ON CONSTRAINT pk_products_asin
            DO UPDATE SET 
                title = EXCLUDED.title, 
                groupName = EXCLUDED.groupName, 
                salesrank = EXCLUDED.salesrank, 
                id = EXCLUDED.id;
        """,
        "similarProducts": """
            INSERT INTO similarProducts(productAsin, similarProductAsin) 
            VALUES (%s, %s)
        """,
        "reviews": """
            INSERT INTO reviews(productAsin, reviewDate, customerId, number, rating, votes, helpful) 
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        "categories": """
            INSERT INTO categories 
            VALUES (%s, %s) 
            ON CONFLICT ON CONSTRAINT pk_categories_id 
            DO NOTHING
        """,
        "hierarchy": """
            INSERT INTO hierarchy(productAsin, number) 
            VALUES (%s, %s)
        """,
        "categories_hierarchy": """
            INSERT INTO categories_hierarchy(hierarchyProductAsin, hierarchyNumber, categoriesId, rank) 
            VALUES (%s, %s, %s, %s)
        """
    }

    conn = None
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="productsdb",
            user="nabson",
            password="pass"
        )
        cur = conn.cursor()
        cur.executemany(insert_commands["groups"], data["groups"])
        cur.executemany(insert_commands["products"], data["products"])
        cur.executemany(insert_commands["similarProducts"], data["similarProducts"])
        cur.executemany(insert_commands["reviews"], data["reviews"])
        cur.executemany(insert_commands["categories"], data["categories"])
        cur.executemany(insert_commands["hierarchy"], data["hierarchy"])
        cur.executemany(insert_commands["categories_hierarchy"], data["categories_hierarchy"])
        conn.commit()
        cur.close()
    except psycopg2.DatabaseError as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

if __name__ == '__main__':
    filepath = 'amazon-meta.txt'
    create_schema()
    data = parse_file(filepath)