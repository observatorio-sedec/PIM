from conexao import conexao
import psycopg2
from etl_pim import dataframe

def executar_sql():
    cur = conexao.cursor()
    
    cur.execute('SET search_path TO pim, public')
    
    dados_pim = '''
    CREATE TABLE IF NOT EXISTS pim.indicadores_economicos (
    id VARCHAR(50),
    nome VARCHAR(255),
    id_produto VARCHAR(50),
    produto VARCHAR(255),
    unidade VARCHAR(50),
    "PIMPF - Número-índice (2022=100)" NUMERIC,
    data DATE,
    "PIMPF - Número-índice com ajuste sazonal (2022=100)" NUMERIC
    );
    '''
    
    cur.execute(dados_pim)
    verificando_existencia_pim_dados = f'''
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema= 'pim' AND table_type='BASE TABLE' AND table_name='indicadores_economicos';
    '''
    
    cur.execute(verificando_existencia_pim_dados)
    resultado_pim = cur.fetchone()
    if resultado_pim[0] == 1: 
        dropando_tabela_dados = f'''
        TRUNCATE TABLE pim.indicadores_economicos;
        '''
        cur.execute(dropando_tabela_dados)
    
    inserindo_indicadores_economicos = '''
        INSERT INTO pim.indicadores_economicos (id, nome, id_produto, produto, unidade, 
            "PIMPF - Número-índice (2022=100)", data, 
            "PIMPF - Número-índice com ajuste sazonal (2022=100)"
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    '''

    try:
        for i in dataframe.iter_rows(named=True):
            dados = (
                i['id'],
                i['nome'],
                i['id_produto'],
                i['produto'],
                i['unidade'],
                i['PIMPF - Número-índice (2022=100)'],
                i['ano'],
                i['PIMPF - Número-índice com ajuste sazonal (2022=100)']
            )
            cur.execute(inserindo_indicadores_economicos, dados)
    except psycopg2.Error as e:
        print(f"Erro ao inserir dados: {e}")

    conexao.commit()
    cur.close()