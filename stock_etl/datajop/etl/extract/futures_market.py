from datetime import datetime
import json
from bs4 import BeautifulSoup
from infra.util import cal_std_day, execute_rest_api
from infra.hdfs_client import get_client
from infra.logger import get_logger

class RawMaterialsExtractor:
    # 원자재
    FILE_DIR = '/finance/futures_market/' 
    FILE_NAME =[ 'energy_' + str(cal_std_day(0))+'.json', 'non_metal_' + str(cal_std_day(0))+'.json',
            'agriculture_' + str(cal_std_day(0))+'.json' ]
    URL = 'https://finance.naver.com/marketindex/?tabSel=materials#tab_section'

    
    
    @classmethod
    def extract_data(cls) :
        response_txt = execute_rest_api('get',cls.URL,{},{})
        soup = BeautifulSoup(response_txt, 'html.parser')
        soup_div = soup.find('div',{'id':'content'}).findAll('div',{'class':'section_material'})
        ops_list = [soup_div[0].findAll('tr')[1:],soup_div[1].findAll('tr')[1:],soup_div[2].findAll('tr')[1:]]

        cols=['product_name','current_month_contract','unit','price','fluctuation_rate','futures_exchange_name','is_rise']
        cols_non_metal =['product_name','unit','price','fluctuation_rate','futures_exchange_name','is_rise']
        data_energy = []
        data_non_metal = []
        data_agriculture = []

        for idx, ops in enumerate(ops_list):
            for op in ops:
                if idx == 0 :
                    # 에너지 선물
                    # 상품명, 월물, 단위, 현재가, 등락율, 거래소명, 상승여부(전일대비) : True -> 상승
                    rows = []
                    rows.append(op.findAll('td')[0].text)
                    rows.append(op.findAll('td')[1].text.strip())
                    rows.append(op.findAll('td')[2].text)
                    rows.append(op.findAll('td')[3].text.replace(',',''))
                    rows.append(op.findAll('td')[5].text.replace('%','').replace('-',''))
                    rows.append(op.findAll('td')[7].text)
                    if op['class'][0] == 'down' :
                        rows.append(0)
                        tmp = dict(zip(cols,rows))
                        data_energy.append(tmp)
                    else :
                        rows.append(1)
                        tmp = dict(zip(cols,rows))
                        data_energy.append(tmp)
                if idx == 1 :
                    rows=[]
                    # 비철금속 현물
                    #상품명, 단위, 현재가, 등락율, 거래소명, 상승여부(전일대비) : True -> 상승
                    rows.append(op.findAll('td')[0].text)
                    rows.append(op.findAll('td')[1].text)
                    rows.append(op.findAll('td')[2].text.replace(',',''))
                    rows.append(op.findAll('td')[4].text.replace('%','').replace('-',''))
                    rows.append(op.findAll('td')[6].text)
                    if op['class'][0] == 'down' :
                        rows.append(0)
                        tmp = dict(zip(cols_non_metal,rows))
                        data_non_metal.append(tmp)
                    else :
                        rows.append(1)
                        tmp = dict(zip(cols_non_metal,rows))
                        data_non_metal.append(tmp)
                        
                if idx == 2 :
                # 농산물 선물
                # 상품명, 월물, 단위, 현재가, 등락율, 거래소명, 상승여부(전일대비) : True -> 상승
                    rows = []
                    rows.append(op.findAll('td')[0].text)
                    rows.append(op.findAll('td')[1].text.strip())
                    rows.append(op.findAll('td')[2].text)
                    rows.append(op.findAll('td')[3].text.replace(',',''))
                    rows.append(op.findAll('td')[5].text.replace('%','').replace('-',''))
                    rows.append(op.findAll('td')[7].text)
                    if op['class'][0] == 'down' :
                        rows.append(0)
                        tmp = dict(zip(cols,rows))
                        data_agriculture.append(tmp)
                    else :
                        rows.append(1)
                        tmp = dict(zip(cols,rows))
                        data_agriculture.append(tmp)

        res_energy = {
            'meta':{
                'desc':'에너지 선물',
                'cols':{
                    'product_name':'상품명','current_month_contract':'월물','unit':'단위','price':'현재가',
                    'fluctuation_rate':'등락율','futures_exchange_name':'선물거래소명','is_rise':'상승여부'
                },
                'std_day':cal_std_day(0),
                'product_line':'energy'
            },
            'data':data_energy
        }
        res_non_metal = {
            'meta':{
                'desc':'비철금속 현물',
                'cols':{
                    'product_name':'상품명','unit':'단위','price':'현재가',
                    'fluctuation_rate':'등락율','futures_exchange_name':'선물거래소명','is_rise':'상승여부'
                },
                'std_day':cal_std_day(0),
                'product_line':'non_metal'
            },
            'data':data_non_metal
        }
        res_agriculture = {
            'meta':{
                'desc':'농산물 선물',
                'cols':{
                    'product_name':'상품명','current_month_contract':'월물','unit':'단위','price':'현재가',
                    'fluctuation_rate':'등락율','futures_exchange_name':'선물거래소명','is_rise':'상승여부'
                },
                'std_day':cal_std_day(0),
                'product_line':'agriculture'
            },
            'data':data_agriculture
        }
        res_list = [res_energy,res_non_metal,res_agriculture]
        for idx,file_na in enumerate(cls.FILE_NAME):
            get_client().write(cls.FILE_DIR+file_na,json.dumps(res_list[idx],ensure_ascii=False),encoding='utf-8')

class OilPreciousMetalExtractor:

    # 시장지표 크롤링

    # 환전고시, 유가/금시세, 원자재
    FILE_DIR = '/finance/futures_market/'
    FILE_NAME =[ 'oil_price_' + str(cal_std_day(0))+'.json', 'gold_price_' + str(cal_std_day(0))+'.json' ]
    URL = 'https://finance.naver.com/marketindex/?tabSel=gold#tab_section'

    ## oil_gold
    @classmethod
    def extract_data(cls):
        response_txt = execute_rest_api('get',cls.URL,{},{})
        soup = BeautifulSoup(response_txt, 'html.parser')
        ops_list = [soup.find('div',{'id':'content'}).findAll('div',{'class':'section_exchange'})[0].findAll('tr')[1:],
            soup.find('div',{'id':'content'}).findAll('div',{'class':'section_exchange'})[1].findAll('tr')[1:] ]

        cols = ['product_name','unit','price','fluctuation_rate','is_rise']


        data_oil = []
        data_gold = []
        rows = []
        for idx,ops in enumerate(ops_list) :
            for op in ops:
                # 유가
                if idx ==  0:
                    rows = []
                    # 상품명, 단위명, 현재가, 등락율, 상승여부 : True는 상승
                    rows.append(op.findAll('td')[0].text)
                    rows.append(op.findAll('td')[1].text)
                    rows.append(op.findAll('td')[2].text.replace(',',''))
                    rows.append(op.findAll('td')[-2].text[:-1].replace(' ','').replace('%','').replace('+','').replace('-',''))
                    if op['class'][0] == 'down' :
                        rows.append(0)
                        tmp = dict(zip(cols,rows))
                        data_oil.append(tmp)
                    else :
                        rows.append(1)
                        tmp = dict(zip(cols,rows))
                        data_oil.append(tmp)
                # 귀금속
                if idx == 1 :
                    rows=[]
                    # 상품명, 단위명, 현재가, 등락율, 상승여부 : True는 상승
                    rows.append(op.findAll('td')[0].text)
                    rows.append(op.findAll('td')[1].text)
                    rows.append(op.findAll('td')[2].text.replace(',',''))
                    rows.append(op.findAll('td')[-2].text.replace(' ','').replace('%','').replace('+','').replace('-',''))
                    if op['class'][0] == 'down' :
                        rows.append(0)
                        tmp = dict(zip(cols,rows))
                        data_gold.append(tmp)
                    else :
                        rows.append(1)
                        tmp = dict(zip(cols,rows))
                        data_gold.append(tmp)

        res_oil = {
                'meta':{
                    'desc':'국내 및 국제 유가',
                    'cols':{
                    'product_name':'상품명',
                    'unit':'단위',
                    'price':'현재가',
                    'fluctuation_rate':'등락율',
                    'is_rise':'전일대비상승여부'
                    },
                    'std_day':cal_std_day(0),
                    'product_line':'oil'
                },
                'data':data_oil
        }
        res_gold = {
                'meta':{
                    'desc':'귀금속 국내 및 국제가격',
                    'cols':{
                    'product_name':'상품명',
                    'unit':'단위',
                    'price':'현재가',
                    'fluctuation_rate':'등락율',
                    'is_rise':'전일대비상승여부'
                    },
                    'std_day':cal_std_day(0),
                    'product_line':'precious_metal'
                },
                'data':data_gold
        }
        res_list = [res_oil,res_gold]
        for idx,file_na in enumerate(cls.FILE_NAME):
            #유가선물, 귀금속선물 저장
            get_client().write(cls.FILE_DIR+file_na,json.dumps(res_list[idx],ensure_ascii=False),encoding='utf-8') 
    
