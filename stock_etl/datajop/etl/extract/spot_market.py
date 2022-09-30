from datetime import datetime
import json
from bs4 import BeautifulSoup
from infra.util import cal_std_day, execute_rest_api
from infra.hdfs_client import get_client
from infra.logger import get_logger


class GlobalMarketCapExtractor:
    """
    주요국가별 시가총액 추출 Class 해당 데이터는 monthly로 업데이트됨
    """
    FILE_DIR = '/finance/spot_market/'
    FILE_NAME = 'market_cap_' + str(cal_std_day(0))+'.json'
    URL = 'https://www.ceicdata.com/en/indicator/market-capitalization'

    @classmethod
    def extract_data(cls):
        response_txt = execute_rest_api('get',cls.URL,{},{})
        soup = BeautifulSoup(response_txt, 'html.parser')
        trs = soup.findAll('table',{'class':'dp-table'})[1].find('tbody').findAll('tr')
        cols = ['country_name_en','country_name', 'market_cap(USD mn)']
        data = []
        for tr in trs:
            rows = []
            rows.append(tr.findAll('td')[0].text.strip()[:-9])
            if tr.findAll('td')[0].text.strip()[:-9] == 'United States':
                rows.append('미국')
            elif tr.findAll('td')[0].text.strip()[:-9] == 'South Korea':
                rows.append('한국')
            elif tr.findAll('td')[0].text.strip()[:-9] == 'Japan':
                rows.append('일본')
            elif tr.findAll('td')[0].text.strip()[:-9] == 'United Kingdom':
                rows.append('영국')                
            else :
                rows.append(tr.findAll('td')[0].text.strip()[:-9])
            rows.append(tr.findAll('td')[1].find('span').text.strip().replace(',',''))
            tmp = dict(zip(cols,rows))
            data.append(tmp)
        res = {
            'meta':{
                'desc':'국가별 시가총액 ',
                'cols':{
                    'country_name_en':'국가영어이름','country_name':'국가이름', 'market_cap(USD mn)':'시가총액(100만 달러)'
                },
                'std_day':cal_std_day(0)
            },
            'data':data
        }
        get_client().write(cls.FILE_DIR+cls.FILE_NAME,json.dumps(res,ensure_ascii=False),overwrite=True,encoding='utf-8')
        
class StockIndexExtractor:
    """
    주요국가의 주가지수를 가져오는 class
    """
    FILE_DIR = '/finance/spot_market/'
    FILE_NAME = 'stock_index_' + str(cal_std_day(0))+'.json'
    URL = 'https://comp.fnguide.com/SVO/WooriRenewal/new_overview.asp'
    
    @classmethod
    def extract_data(cls):
        response_txt = execute_rest_api('get',cls.URL,{},{})
        soup = BeautifulSoup(response_txt, 'html.parser')
        trs = soup.find('div',{'id':'right_column'}).find('tbody').findAll('tr')
        cols = ['country_name','si_name', 'si_price', 'si_fl_rate', 'si_is_rise']
        data = []
        for tr in trs :
            rows=[]
            # 국가 이름 : country_name
            rows.append(tr.findAll('td')[0].text)
            # 지수 이름 : index_name
            if tr.findAll('td')[1].text == '종합주가지수':
                rows.append('코스피')
            else :
                rows.append(tr.findAll('td')[1].text)
            # 종가 :price
            rows.append(tr.findAll('td')[2].text.replace(',',''))
            # 등락율 : fluctuation_rate
            rows.append(tr.findAll('td')[3].text.replace('+',''))
            # 상승여부 : is_rise
            try :
                if tr.findAll('td')[3].text.replace('%','')[0] == '-' :
                    rows.append(0)
                    tmp = dict(zip(cols,rows))
                    data.append(tmp)
                else :
                    rows.append(1)
                    tmp = dict(zip(cols,rows))
                    data.append(tmp)

            except :
                rows.append(0)
                tmp = dict(zip(cols,rows))
                data.append(tmp)
        res = {
            'meta':{
                'desc':'주요 주가 지수',
                'cols':{
                    'country_name':'국가이름','si_name':'주가지수이름', 'si_price':'주가지수종가', 'si_fl_rate':'지수등략률(%)', 'si_is_rise':'지수상승여부'
                },
                'std_day':cal_std_day(0)
            },
            'data':data
        }
        get_client().write(cls.FILE_DIR+cls.FILE_NAME,json.dumps(res,ensure_ascii=False),overwrite=True,encoding='utf-8')


class SovereignYieldExtractor:
    """
    주요국가의 채권수익률 가져오는 Class
    """
    BOND_TABLE_NUMBER = ['cr1', 'bond_table_1', 'bond_table_3', 'bond_table_20']
    BASE_URL = 'https://kr.investing.com/rates-bonds/'
    URL_LIST = ['south-korea-government-bonds', 'americas-government-bonds', 'european-government-bonds','asian-pacific-government-bonds']
    FILE_DIR = '/finance/spot_market/'
    FILE_NAME = 'sovereign_yield_' + str(cal_std_day(0))+'.json'
    
    @classmethod
    def extract_data(cls):
        # 채권 크롤링
        # 모을 연도 = [1,2,3,5,10,30]
        # 모을 국가 한국, 미국, 영국, 일본의 table bond_table_number = [60, 1, 3, 20]
        cols = ['country_name', '1y','2y','3y','5y','10y','30y']
        data = []
        for num in range(4):
            tmp = dict(zip(cols,cls.__get_bond_yield_row(int(num))))
            data.append(tmp)
        res = {
            'meta':{
                'desc':'한국,미국,영국,일본 국채 금리',
                'cols':{
                    'country_name':'국가명','1y':'1년물','2y':'2년물','3y':'3년물','5y':'5년물','10y':'10년물','30y':'30년물'
                },
                'std_day':cal_std_day(0)
            },
            'data':data
        }
        get_client().write(cls.FILE_DIR+cls.FILE_NAME,json.dumps(res,ensure_ascii=False),encoding='utf-8')

    @classmethod
    def __get_ops(cls,num):
        url = cls.BASE_URL + cls.URL_LIST[num]
        response_txt = execute_rest_api('get',url,{},{})
        soup = BeautifulSoup(response_txt, 'html.parser')
        return soup.find('table', {'id':cls.BOND_TABLE_NUMBER[int(num)]})

    
    @classmethod
    def __get_bond_yield_row(cls,num):
        trs = cls.__get_ops(num).findAll('tr')[1:]
        bond_rows = []
        if num == 0:
            bond_rows.append("한국")
        elif num == 1:
            bond_rows.append("미국")
        elif num == 2:
            bond_rows.append("영국")
        elif num == 3:
            bond_rows.append("일본")
        else :
            return None
        for tr in trs:
            price_year = tr.find('a').text
            if (('1년' in price_year) or ('2년' in price_year) or ('3년' in price_year) or ('5년' in price_year) \
                or ('10년' in price_year) or ('30년' in price_year)) and ('12년' not in price_year) and ('15년' not in price_year) and ('25년' not in price_year) :
                bond_rows.append(tr.findAll('td')[3].text.replace(',','')) # 종가 채권수익률
        return bond_rows

class BankInterestExtractor:
    ## 중앙은행의 이자율
    FILE_DIR = '/finance/spot_market/'
    FILE_NAME = 'cental_interest_' + str(cal_std_day(0))+'.json'
    URL = 'https://kr.investing.com/central-banks/'
    
    
    @classmethod
    def extract_data(cls) :
        response_txt = execute_rest_api('get',cls.URL,{},{})
        soup = BeautifulSoup(response_txt, 'html.parser')
        ops = soup.find('table',{'id':'curr_table'}).findAll('tr')[1:]

        cols = ['country_name','ctr_bank', 'ctr_inter', 'ctr_next_conf', 'ctr_cng_date', 'ctr_latest_point', 'ctr_is_rise']
        data = []
        # 중앙은행
        cnts = ['한국','미국','유럽','영국','스위스','호주','캐나다','일본','러시아','인도','중국','브라질']
        for idx,op in enumerate(ops):
            rows = []
            # 국가이름
            rows.append(cnts[idx])
            # 은행이름
            rows.append(op.findAll('td')[1].text)
            # 현재금리, 다음회의, 최근변동일, 최근변동포인트, 상승여부
            rows.append(op.findAll('td')[2].text.replace('%',''))
            rows.append(op.findAll('td')[3].text.replace('년','').replace('월','').replace('일','').replace(' ','-')[1:][:-1])
            rows.append(op.findAll('td')[4].text[:13].replace('년','').replace('월','').replace('일','').replace(' ','-'))
            rows.append(op.findAll('td')[4].text[14:].replace('(','').replace(')','').replace('bp',''))
            if op.findAll('td')[4].text[14:].replace('(','').replace(')','').replace('bp','')[0] == '-':
                rows.append(0)
                tmp = dict(zip(cols,rows))
                data.append(tmp)
            else:
                rows.append(1)
                tmp = dict(zip(cols,rows))
                data.append(tmp)
        res = {
            'meta':{
                'desc':'세계주요국의 중앙은행의 이자율',
                'cols':{
                    'country_name':'국가이름','ctr_bank':'중앙은행', 'ctr_inter':'금리(%)', 'ctr_next_conf':'다음 금리결정회의 날짜', 'ctr_cng_date':'최근 금리 결정 날짜',
                    'ctr_latest_point':'금리 증감 퍼센트포인트', 'ctr_is_rise':'금리상승여부'
                },
                'std_day':cal_std_day(0)
            },
            'data':data
        }
        get_client().write(cls.FILE_DIR+cls.FILE_NAME,json.dumps(res,ensure_ascii=False),encoding='utf-8')


class ExchangeExtractor:
    # 매일매일 환율 가져오기
    # 시장지표 크롤링
    FILE_DIR = '/finance/spot_market/'
    FILE_NAME = 'sale_standard_rate_' + str(cal_std_day(0))+'.json'
    URL = 'https://finance.naver.com/marketindex/?tabSel=exchange#tab_section'

    @classmethod
    def extract_data(cls) :
        response_txt = execute_rest_api('get',cls.URL,{},{})
        soup = BeautifulSoup(response_txt, 'html.parser')
        ops = soup.find('select',{'id':'select_to'}).findAll('option')[2:]
        cols = ['country_name','exr_mont_unit','exr_stad_rate']

        data = []
        for op in ops:
            rows = []
            # 국가, 화폐이름(영어) 만필요
            # 국가이름 : country_name
            rows.append(op.text.split(' ')[0])
            # 화폐이름 : exr_mont_unit
            rows.append(op.text.split(' ')[-1])
            #매매기준율 : exr_stad_rate
            rows.append(op['value'])
            tmp = dict(zip(cols,rows))
            data.append(tmp)

        res = {
            'meta':{
                'desc':'국가별 매매기준율 1국가화폐당 원화',
                'cols':{
                'country_name':'국가명',
                'exr_mont_unit':'화폐이름',
                'exr_stad_rate':'매매기준율',
                },
                'std_day':cal_std_day(0)
            },
            'data':data
        }
        
        get_client().write(cls.FILE_DIR+cls.FILE_NAME,json.dumps(res,ensure_ascii=False),encoding='utf-8')

