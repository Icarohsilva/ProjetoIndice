"""
Módulo para recuperar índices econômicos do TJMG e inseri-los em um banco de dados SQL Server.
"""
from datetime import datetime
import pyodbc
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright


def indices_tjmg(banco, ano_base, indice_name):
    """Recuperar Índices do TJMG"""  
    banco = banco
    ano_base = ano_base
    # Configurações do banco de dadospip SQL Server
    SERVER = 'max-bdmg.cv9q5k0jru0g.us-east-1.rds.amazonaws.com'
    DATABASE = banco
    USERNAME = 'max_master'
    PASSWORD = 'qBG3V%CW'
    DRIVER = 'SQL Server'

    # Conectar ao banco de dados SQL Server
    connection_string = f'DRIVER={DRIVER};SERVER={SERVER};DATABASE={DATABASE};UID={USERNAME};PWD={PASSWORD}'
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()

    # Definir o valor de ano_inicio com base no parâmetro ano
    if ano_base is not None:
        ano_inicio = ano_base
    else:
        ano_inicio = 1964
    ano_atual = datetime.now().year
    #vai realizar uma iteração passando por todos os anos, de 1964 até hoje
    
    for ano in range(ano_inicio, ano_atual + 1):

        with sync_playwright() as sync:
            browser = sync.chromium.launch()
            page = browser.new_page()
            # URL da página
            url = 'http://www8.tjmg.jus.br/cadej/pages/web/consulta-indice/indicadoresEconomicos.xhtml'
            page.goto(url)

            ano_label = page.query_selector('#formAno_label')
            ano_label.click()
            ano_item = page.query_selector(f'li[data-label="{ano}"]')
            ano_item.click()
            indicelabel = page.query_selector('#formIndice_label')
            indicelabel.click()
            #encontra e clica no campo Indicador ICGJ (TJMG)
            indice = page.wait_for_selector('#formIndice_0')
            indice.click()

            #encontra e clica no botão pesquisar
            pesquisar = page.wait_for_selector('#btnPesquisar')
            pesquisar.click()

            tabela = page.wait_for_selector('#tableIndice').inner_html()
            browser.close() #Encerrar o navegador

            #transforma o html da tabela em um objeto BeautifulSoup, pois fica mais facil de tratar
            html_tabela = BeautifulSoup(tabela, 'html.parser')

            dados_tabela = []

            # Encontra o segundo tr dentro da thead
            second_tr = html_tabela.select_one('#tableIndice_head tr:nth-of-type(2)')

            # Percorre todos os ths dentro do segundo tr para obter as descrições dos índices
            descricoes = []
            for th in second_tr.find_all('th'):
                descricao = th.get('aria-label', '').strip()
                if descricao.endswith(')'):
                    descricao = descricao[:descricao.rindex('(')].strip()
                descricao = descricao.replace('%', '').replace('R$', '').replace('(', '').replace(')', '')
                if descricao:
                    descricoes.append(descricao)

            # Encontra os trs dentro da tbody
            trs = html_tabela.select('#tableIndice_data tr')

            # Percorre os trs dentro da tbody para obter os valores dos índices
            for tr in trs:
                mes_ano = tr.find('td', role='gridcell', style='font-weight: bold; text-align:left; padding-right:2px; padding-left:2px; color: #880000; background-color: #e7e7e7')
                if mes_ano:
                    mes_ano = mes_ano.text.strip()

                    # Pega os valores dos índices dentro do tr
                    valores = tr.find_all('td', role='gridcell', style='text-align:right; padding-right:2px; padding-left:2px')

                    # Ignora o primeiro valor (mes/ano) e pega os demais
                    for descricao, valor in zip(descricoes, valores[0:]):
                        valor = valor.text.strip().replace('.', '').replace(',', '.')
                        dados_tabela.append((descricao, mes_ano, valor))

            # Agora a lista dados_tabela contém tuplas com o mês/ano, a descrição e o valor de cada índice
            for registro in dados_tabela:
                descricao, mes_ano, valor = registro
                print(f"Mês/Ano: {mes_ano}, Descrição: {descricao}, Valor: {valor}")


        meses_dict = {
            'Jan': 1, 'Fev': 2, 'Mar': 3, 'Abr': 4, 'Mai': 5, 'Jun': 6,
            'Jul': 7, 'Ago': 8, 'Set': 9, 'Out': 10, 'Nov': 11, 'Dez': 12,
            'Jan.': 1  # Adicione o formato específico aqui
        }


        for linha in dados_tabela:
            indice = linha[0].strip() # Remover espaços extras no final do índice
            
            if indice_name != "Todos" and indice != indice_name:
                continue
            else:
                indice = indice_name

            mes_ano = linha[1]
            valor = linha[2]

            if valor != "-" and mes_ano != 'Ac.Ano':
            
                mes, ano = mes_ano.split('/')

                # Converter o nome do mês para o número correspondente
                mes_numero = meses_dict.get(mes)
                if mes_numero is None:
                    raise ValueError(f"Mês inválido: {mes}")

                # Verificar se o indice ja esta cadastrado
                if indice == "50 OTN's - Valor de alçada":
                    indice = "50 OTNs - Valor de alçada"

                select_query = f"SELECT CodIndiceMonetario FROM tbIndiceMonetario WHERE DescIndiceMonetario = '{indice}'"
                cursor.execute(select_query)
                row = cursor.fetchone()

                if row:
                    CodIndiceMonetario = row[0]  # Fetch the first column of the first row (CodIndiceMonetario)
                else:
                    # If the index doesn't exist, insert it into the tbIndiceMonetario table
                    if indice in ["50 OTNs - Valor de alçada", "Salário Mínimo", "UFEMG", "Valor da Cessão do Metro Quadrado"]:
                        tipo = 2
                    else:
                        tipo = 1

                    insert_query = f"INSERT INTO tbIndiceMonetario (DescIndiceMonetario, Status, DtCadastro, UserCadastro, Tipo) VALUES ('{indice}', 1, GETDATE(), 6026, {tipo})"
                    cursor.execute(insert_query)
                    conn.commit()

                    # Perform the query again to get the inserted CodIndiceMonetario
                    cursor.execute(select_query)
                    CodIndiceMonetario = cursor.fetchone()[0]

                # Verificar se o registro já existe com os mesmos valores
                select_query = f"SELECT COUNT(*) FROM tbFatorIndiceMonetarioAtualizado WHERE Mes = {mes_numero} AND Ano = {ano} AND Valor = {valor} AND CodIndiceMonetario = {CodIndiceMonetario}"
                cursor.execute(select_query)
                count = cursor.fetchone()[0]

                if count == 0:
                    # Verificar se existe um registro com o mesmo ano, mês e CodIndiceMonetario, mas com valor diferente
                    select_query = f"SELECT Valor FROM tbFatorIndiceMonetarioAtualizado WHERE Mes = {mes_numero} AND Ano = {ano} AND CodIndiceMonetario = {CodIndiceMonetario}"
                    cursor.execute(select_query)
                    existing_value = cursor.fetchone()

                    if existing_value:
                        existing_value = existing_value[0]
                        if existing_value != valor:
                            # Atualizar o valor no registro existente
                            update_query = f"UPDATE tbFatorIndiceMonetarioAtualizado SET Valor = {valor} WHERE Mes = {mes_numero} AND Ano = {ano} AND CodIndiceMonetario = {CodIndiceMonetario}"
                            cursor.execute(update_query)
                            print(update_query)
                            conn.commit()
                    else:
                        # Inserir um novo registro
                        if valor is not None:
                            insert_query = f"INSERT INTO tbFatorIndiceMonetarioAtualizado (Mes, Ano, Valor, DtCadastro, UserCadastro, CodIndiceMonetario, status) VALUES ({mes_numero}, {ano}, {valor}, GETDATE(), 6026, {CodIndiceMonetario}, 1)"
                            cursor.execute(insert_query)
                            print(insert_query)
                            conn.commit()
                else:
                    print("O registro já existe")

    
    print('Índices atualizados com sucesso')
            
if __name__ == "__main__":
    pass