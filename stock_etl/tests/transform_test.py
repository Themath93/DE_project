import unittest

from datajop.etl.tranform.tf_futures_market import FuturesMarketTransformer

class MTest(unittest.TestCase):


    def test1(self):
        FuturesMarketTransformer.transform()


        
if __name__ == "__main__":
    """ This is executed when run from the command line """
    unittest.main()
