import ssl
# import pandas as pd
import requests as rq
from datetime import datetime
import polars as pl
import concurrent.futures
import time
url = f'https://servicodados.ibge.gov.br/api/v3/agregados/8888/periodos/202201/variaveis/12606%7C12607?localidades=N3[all]&classificacao=544[129315,129316,129317,129318,129319,129320,129321,129322,129323,129324,129325,129326,56689,129330,129331,129332,129333,129334,129335,129336,129337,129338,129339,129340,129341,129342]'

class TLSAdapter(rq.adapters.HTTPAdapter):
    def init_poolmanager(self, *args, **kwargs):
        ctx = ssl.create_default_context()
        ctx.set_ciphers("DEFAULT@SECLEVEL=1")
        ctx.options |= 0x4   
        kwargs["ssl_context"] = ctx
        return super(TLSAdapter, self).init_poolmanager(*args, **kwargs)


def requisitando_dados(url):
    with rq.session() as s:
        s.mount("https://", TLSAdapter())
        dados_brutos_url = s.get(url, verify=True)
    
    if dados_brutos_url.status_code != 200:
        raise Exception(f"A solicitação à url falhou com o código de status: {dados_brutos_url.status_code}")

    try:
        dados_brutos = dados_brutos_url.json()
    except Exception as e:
        raise Exception(f"Erro ao analisar a resposta JSON da url: {str(e)}")

    if len(dados_brutos) < 2:
        print(f"Aviso: Dados insuficientes para a URL {url}")
        return None, None
    
    if dados_brutos_url.status_code == 500:
        print(f"Aviso: Erro 500 para a URL {url}")
        return None, None

    dados_brutos_12606 = dados_brutos[0]
    dados_brutos_12607 = dados_brutos[1]
    return dados_brutos_12606, dados_brutos_12607

def tratando_dados(dados_brutos_12606, dados_brutos_12607):
    dados_limpos_12606 = []
    dados_limpos_12607 = []

    variaveis = [dados_brutos_12606, dados_brutos_12607]

    for i in variaveis:
        id_tabela = i['id']
        variavel = i['variavel']
        unidade = i['unidade']
        dados = i['resultados']

        for ii in dados:
            dados_produto = ii['classificacoes']
            dados_producao = ii['series']
            
            for iii in dados_produto:
                dados_id_produto = iii['categoria']

                for id_produto, nome_produto in dados_id_produto.items():
    
                    for iv in dados_producao:
                        id = iv['localidade']['id']
                        nome = iv['localidade']['nome']
                        dados_ano_producao = iv['serie'] 
                        
                        for ano, producao in dados_ano_producao.items():
                            partes = ano.split("/")
                            ano = int(partes[0][:4])
                            mes = (partes[0][4:6])
                            producao = producao.replace('-', '0').replace('...', '0')
                            
                            dict = {

                                'id': id,
                                'nome': nome,
                                'id_produto': id_produto,
                                'produto': nome_produto,
                                'unidade': unidade,
                                variavel: producao,
                                'ano': f'01/{mes}/{ano}'   
                            }
                           
                            if id_tabela == '12606':
                                dados_limpos_12606.append(dict)
                            elif id_tabela == '12607':
                                dados_limpos_12607.append(dict)


    return dados_limpos_12606, dados_limpos_12607

def gerando_dataframe(dados_limpos_12606, dados_limpos_dados_brutos_12607):
    
    schema_12606 = {
        'id': pl.Utf8,
        'nome': pl.Utf8,
        'id_produto': pl.Utf8,
        'produto': pl.Utf8,
        'unidade': pl.Utf8,
        'PIMPF - Número-índice (2022=100)': pl.Float64,
        'ano': pl.Utf8,  
    }
    schema_12607 = {
        'id': pl.Utf8,
        'nome': pl.Utf8,
        'id_produto': pl.Utf8,
        'produto': pl.Utf8,
        'unidade': pl.Utf8,
        'PIMPF - Número-índice com ajuste sazonal (2022=100)': pl.Float64,
        'ano': pl.Utf8, 
    }
    
    df12606 = pl.DataFrame(dados_limpos_12606, schema=schema_12606)
    df12607 = pl.DataFrame(dados_limpos_dados_brutos_12607, schema=schema_12607)

    df12606 = df12606.with_columns(
        pl.col('ano').str.to_date('%d/%m/%Y')
    )
    df12607 = df12607.with_columns(
        pl.col('ano').str.to_date('%d/%m/%Y')
    )

    dataframe = df12606.join(df12607, on=['id', 'nome', 'id_produto', 'produto', 'unidade', 'ano'], how='inner')
    
    if dataframe.is_empty():
        return dataframe
    
    dataframe = dataframe.with_columns([
        pl.col('produto').str.replace(r'^\d+\.\d+\s+', '').str.replace(r'^\d+', '')
    ])
    
    return dataframe

def processar_url(url):
    try:
        variavel12606, variavel_12607 = requisitando_dados(url)
        if variavel12606 is not None and variavel_12607 is not None:
            return tratando_dados(variavel12606, variavel_12607)
    except Exception as e:
        print(f"Erro ao processar URL {url}: {e}")
    return [], []

def executando_loop_datas():
    mes_atual = int(datetime.now().month)
    ano_atual = int(datetime.now().year)
    urls = []
    
    for ano in range(2018, ano_atual+1):
        for mes in range(1, 13):
            if ano == ano_atual and mes == mes_atual-1:
                break
            
            periodo = f"{ano}{mes:02d}"
            url = f'https://servicodados.ibge.gov.br/api/v3/agregados/8888/periodos/{periodo}/variaveis/12606%7C12607?localidades=N3[all]&classificacao=544[all]'
            urls.append(url)

    print(f"Total URLs to process: {len(urls)}")

    lista_dados_12606 = [] 
    lista_dados_12607 = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_url = {executor.submit(processar_url, url): url for url in urls}
        for future in concurrent.futures.as_completed(future_to_url):
            novos_dados_12606, novos_dados_12607 = future.result()
            lista_dados_12606.extend(novos_dados_12606)
            lista_dados_12607.extend(novos_dados_12607)
            
    return lista_dados_12606, lista_dados_12607

inicio = time.perf_counter()
dados_limpos_12606, dados_limpos_dados_brutos_12607= executando_loop_datas()
dataframe = gerando_dataframe(dados_limpos_12606, dados_limpos_dados_brutos_12607)
print(dataframe)
fim = time.perf_counter()
print(f"Tempo de execução: {round(fim - inicio, 2)}")
if __name__ == '__main__':
    from sql import executar_sql
    executar_sql()