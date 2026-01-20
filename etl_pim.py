import ssl
import pandas as pd
import requests as rq
from datetime import datetime


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
    
    # Verificação se a solicitação foi bem-sucedida antes de continuar
    if dados_brutos_url.status_code != 200:
        raise Exception(f"A solicitação à url falhou com o código de status: {dados_brutos_url.status_code}")

    # Verificação se a resposta pode ser convertida para JSON
    try:
        dados_brutos = dados_brutos_url.json()
    except Exception as e:
        raise Exception(f"Erro ao analisar a resposta JSON da url: {str(e)}")

    # Verificação se a resposta contém os dados esperados
    if len(dados_brutos) < 2:
        raise Exception("A resposta da url não contém dados suficientes.")
    
    if dados_brutos_url.status_code == 500:
        raise Exception(f"Os dados passou de 100.0000 por isso o codigo de: {dados_brutos_url.status_code}")

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

    df12606 = pd.DataFrame(dados_limpos_12606)
    df12607 = pd.DataFrame(dados_limpos_dados_brutos_12607)


    dataframe = pd.merge(df12606, df12607, on=['id', 'nome', 'id_produto', 'produto', 'unidade', 'ano'], how='inner')
    dataframe['PIMPF - Número-índice (2022=100)'] = dataframe['PIMPF - Número-índice (2022=100)'].astype(float)
    dataframe['PIMPF - Número-índice com ajuste sazonal (2022=100)'] = dataframe['PIMPF - Número-índice com ajuste sazonal (2022=100)'].astype(float)
    dataframe['produto'] = dataframe['produto'].str.replace(r'^\d+\.\d+\s+', '', regex=True)
    dataframe['produto'] = dataframe['produto'].str.replace(r'^\d+', '', regex=True)
    dataframe['ano'] = pd.to_datetime(dataframe['ano'], format='%d/%m/%Y').dt.date

    return dataframe

def executando_loop_datas():
    mes_atual = int(datetime.now().month)
    ano_atual = int(datetime.now().year)
    lista_dados_12606 = [] 
    lista_dados_12607 = []
    for ano in range(2018, ano_atual+1):
        for mes in range(1, 13):
            if ano == ano_atual and mes == mes_atual-1:
                break
            if mes == 10 or mes == 11 or mes == 12:
                url = f'https://servicodados.ibge.gov.br/api/v3/agregados/8888/periodos/{ano}{mes}/variaveis/12606%7C12607?localidades=N3[all]&classificacao=544[all]'     
            else:
                url = f'https://servicodados.ibge.gov.br/api/v3/agregados/8888/periodos/{ano}0{mes}/variaveis/12606%7C12607?localidades=N3[all]&classificacao=544[all]'     
            variavel12606, variavel_12607= requisitando_dados(url)
            if len(variavel12606) == 0 and len(variavel_12607) == 0 :
                lista_dados_12606, lista_dados_12607= tratando_dados(variavel12606, variavel_12607)

            else:
                novos_dados_12606, novos_dados_12607= tratando_dados(variavel12606, variavel_12607)
                lista_dados_12606.extend(novos_dados_12606)
                lista_dados_12607.extend(novos_dados_12607)

    return  lista_dados_12606,lista_dados_12607

dados_limpos_12606, dados_limpos_dados_brutos_12607= executando_loop_datas()
dataframe = gerando_dataframe(dados_limpos_12606, dados_limpos_dados_brutos_12607)
dataframe.to_excel('C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\TABELAS\\TABELAS EM CSV\\PIM_PROD_FISICA.xlsx', index=False)
print(dataframe)
if __name__ == '__main__':
    from sql import executar_sql
    executar_sql()