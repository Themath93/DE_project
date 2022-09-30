# from infra.jdbc import DataWareHouse, save_data
from string import Template
from infra.spark_session import get_spark_session
from infra.util import cal_std_day
from pyspark.sql.functions import col
from pyspark.sql import Row

class FuturesMarketTransformer:

    # 원자재
    @classmethod
    def transform(cls):
        base_path= '/finance/futures_market/'
        params = ['energy_' + str(cal_std_day(0))+'.json', 
                'non_metal_' + str(cal_std_day(0))+'.json', 
                'agriculture_' + str(cal_std_day(0))+'.json',
                'oil_price_' + str(cal_std_day(0))+'.json',
                'gold_price_' + str(cal_std_day(0))+'.json'        
                ]
        path = base_path + params[0]
        
        data = []
        for idx,param in enumerate(params):
            path = base_path + param     
            futures_market_json = get_spark_session().read.json(path,encoding='UTF-8')
            for r1 in futures_market_json.select(futures_market_json.data, futures_market_json.meta.std_day, futures_market_json.meta.product_line).toLocalIterator():
                for r2 in r1.data:
                    temp = r2.asDict()
                    
                    if idx != 0 and idx != 2:
                        print(idx)
        #                 temp['std_day'] = r1['meta.std_day'] 
        #                 temp['product_line'] = r1['meta.product_line']
        #                 data.append(Row(**temp))
        #             # else : 
        #             #     temp['std_day'] = r1['meta.std_day'] 
        #             #     temp['product_line'] = r1['meta.product_line']
        #             #     del temp['current_month_contract']
        #             #     del temp['futures_exchange_name']
        #             #     data.append(Row(**temp))
        # df = get_spark_session().createDataFrame(data)
        # print(df.show(5))