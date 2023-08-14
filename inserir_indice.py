"""
Módulo para recuperar índices econômicos e inseri-los em um banco de dados SQL Server.
"""
from datetime import datetime
import pyodbc
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

def indices_avulso(banco, indice):
    """Recuperar Índices avulso"""
    banco = banco
    indice = indice
    # Configurações do banco de dados SQL Server
    SERVER = 'max-bdmg.cv9q5k0jru0g.us-east-1.rds.amazonaws.com'
    DATABASE = banco
    USERNAME = 'max_master'
    PASSWORD = 'qBG3V%CW'
    DRIVER = 'SQL Server'

    # Conectar ao banco de dados SQL Server
    connection_string = f'DRIVER={DRIVER};SERVER={SERVER};DATABASE={DATABASE};UID={USERNAME};PWD={PASSWORD}'
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()

    if indice != "Todos":
        # Inclua as aspas simples na variável 'indice' antes de utilizá-la na query
        indice = f"'{indice}'"        
        select_query = f"SELECT CodFatorIndiceMonetario FROM tbFatorIndiceMonetarioAtualizado " \
                      f"INNER JOIN TbIndiceMonetario ON tbFatorIndiceMonetarioAtualizado.CodIndiceMonetario = TbIndiceMonetario.CodIndiceMonetario " \
                      f"WHERE TbIndiceMonetario.DescIndiceMonetario = {indice}"
        cursor.execute(select_query)
        codigos_fator = cursor.fetchall()
    else:
        # Coletar os CodFatorIndiceMonetario que atendem aos critérios desejados
        select_query = f"SELECT CodFatorIndiceMonetario FROM tbFatorIndiceMonetarioAtualizado Where CodIndiceMonetario <> 9"
        cursor.execute(select_query)
        codigos_fator = cursor.fetchall()

    if len(codigos_fator) > 0:  # Check if there are results
        # Deletar apenas os valores recuperados do SELECT
        delete_query = f"DELETE FROM tbFatorIndiceMonetarioAtualizado WHERE CodFatorIndiceMonetario IN ({', '.join(map(str, [x[0] for x in codigos_fator]))})"
        cursor.execute(delete_query)
        conn.commit()
    else:
        print("Nenhum índice encontrado no banco de dados")

    # Chamado a função para recuperar os indicadores econômicos
    url = "https://debit.com.br/tabelas/indicadores-economicos.php"
    todos_indices = get_todos_indices(url)

    indice = indice.replace("'", "")

    if indice != "Todos":
        # Filter indices by the specified tipo_indice
        filtered_indices = {
            indice_name: values for indice_name, values in todos_indices.items() if indice_name == indice
        }
    else:
        # If tipo_indice is not specified, use all indices
        filtered_indices = todos_indices

    meses_dict = {
    'Jan': 1, 'Fev': 2, 'Mar': 3, 'Abr': 4, 'Mai': 5, 'Jun': 6,
    'Jul': 7, 'Ago': 8, 'Set': 9, 'Out': 10, 'Nov': 11, 'Dez': 12,
    'Jan.': 1  # Adicione o formato específico aqui
    }

    # buscar pelo item dentro do dicionario
    for indice_name, values in filtered_indices.items():
        print(indice_name)

        # Perform the query to get the CodIndiceMonetario
        select_query = f"SELECT CodIndiceMonetario FROM tbIndiceMonetario WHERE DescIndiceMonetario = '{indice_name}'"
        cursor.execute(select_query)
        row = cursor.fetchone()

        if row:
            CodIndiceMonetario = row[0]  # Fetch the first column of the first row (CodIndiceMonetario)
        else:
            # If the index doesn't exist, insert it into the tbIndiceMonetario table
            tipo = 1 if '%' in list(values.values())[0] else 2
            insert_query = f"INSERT INTO tbIndiceMonetario (DescIndiceMonetario, Status, DtCadastro, UserCadastro, Tipo) VALUES ('{indice_name}', 1, GETDATE(), 6026, {tipo})"
            cursor.execute(insert_query)
            conn.commit()

            # Perform the query again to get the inserted CodIndiceMonetario
            cursor.execute(select_query)
            CodIndiceMonetario = cursor.fetchone()[0]

        for date_string, value_string in values.items():
            mes_ano = date_string.split('/')
            mes = meses_dict.get(mes_ano[0], -1)
            ano = int(mes_ano[1])
            valor = float(value_string.replace('.', '').replace(',', '.').replace('%', '')) if value_string != '-' else None

            # Incluir no banco de dados o indice
            if valor != "-" and valor != "" and valor != None :
                query = f"INSERT INTO tbFatorIndiceMonetarioAtualizado (Mes, Ano, Valor, DtCadastro, UserCadastro, CodIndiceMonetario, status) VALUES ({mes}, {ano}, {valor}, GETDATE(), 6026, {CodIndiceMonetario}, 1)"
                cursor.execute(query)
                print(query)
                # Confirmar as alterações e fechar a conexão
                conn.commit()
            else:
                print("O campo valor esta nulo")

        print('Índices atualizados com sucesso')




def get_todos_indices(url):
    #Chamada das funções de recuperar indices
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()
        page.goto(url)
        content = page.content()
        browser.close()

    soup = BeautifulSoup(content, 'html.parser')

    indices_economicos = get_indices_economicos(url)
    indices_diversos = get_indices_diversos(url)
    
    # Juntar os indicadores
    todos_indices = {**indices_economicos, **indices_diversos}
    
    return todos_indices


# Recuperar "Indicadores de inflação"
def get_indices_economicos(url):
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()
        page.goto(url)
        content = page.content()
        browser.close()

    soup = BeautifulSoup(content, 'html.parser')
    indices_economicos = {}

    # Recuperar cabeçalhos do <thead>
    thead = soup.find('thead')
    th_list = thead.find_all('th')[1:]  # Ignorar o primeiro <th> vazio
    headers = [th.text.strip() for th in th_list]

    # Recuperar valores do <tbody>
    tbody = soup.find('tbody')
    tr_list = tbody.find_all('tr')
    for tr in tr_list:
        td_list = tr.find_all('td')
        indice_name = td_list[0].find('a').text.strip()
        values = [td.text.strip() for td in td_list[1:]]
        indices_economicos[indice_name] = dict(zip(headers, values))

    return indices_economicos


def get_indices_diversos(url):
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()
        page.goto(url)
        content = page.content()
        browser.close()

    soup = BeautifulSoup(content, 'html.parser')
    indices_diversos = {}

    # Recuperar cabeçalhos do segundo <thead>
    thead_list = soup.find_all('thead')
    second_thead = thead_list[1] if len(thead_list) > 1 else None

    if second_thead:
        th_list = second_thead.find_all('th')[1:]  # Ignorar o primeiro <th> vazio
        headers = [th.text.strip() for th in th_list]

        # Recuperar valores do segundo <tbody>
        tbody_list = soup.find_all('tbody')
        second_tbody = tbody_list[1] if len(tbody_list) > 1 else None

        if second_tbody:
            tr_list = second_tbody.find_all('tr')
            for tr in tr_list:
                td_list = tr.find_all('td')
                indice_name = td_list[0].find('a').text.strip()
                values = [td.text.strip() for td in td_list[1:]]
                indices_diversos[indice_name] = dict(zip(headers, values))
        else:
            print("No second <tbody> found.")
    else:
        print("No second <thead> found.")

    return indices_diversos