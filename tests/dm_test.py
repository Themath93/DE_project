import unittest
from datajob.datamart.co_facility import CoFacility
from datajob.datamart.co_popu_density import CoPopuDensity
from datajob.datamart.co_vaccine import CoVaccine


class MTest(unittest.TestCase):

    def test1(self):
        CoPopuDensity.save()

    def test2(self):
        CoVaccine.save()

    def test3(self):
        CoFacility.save()    
if __name__ == "__main__":
    """ This is executed when run from the command line """
    unittest.main()