import unittest
from datajop.etl.extract.futures_market import OilPreciousMetalExtractor, RawMaterialsExtractor

from datajop.etl.extract.spot_market import BankInterestExtractor, ExchangeExtractor, GlobalMarketCapExtractor, SovereignYieldExtractor, StockIndexExtractor


class MTest(unittest.TestCase):

    def test0(self):
        GlobalMarketCapExtractor.extract_data()

    def test1(self):
        StockIndexExtractor.extract_data()

    def test2(self):
        SovereignYieldExtractor.extract_data()

    def test3(self):
        BankInterestExtractor.extract_data()
    
    def test4(self):
        ExchangeExtractor.extract_data()
    
    def test5(self):
        RawMaterialsExtractor.extract_data()
    
    def test6(self):
        OilPreciousMetalExtractor.extract_data()

        
if __name__ == "__main__":
    """ This is executed when run from the command line """
    unittest.main()
