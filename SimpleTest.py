import unittest
import Solution as Solution
from Utility.ReturnValue import ReturnValue
from Tests.AbstractTest import AbstractTest

from Business.Apartment import Apartment
from Business.Owner import Owner
from Business.Customer import Customer

'''
    Simple test, create one of your own
    make sure the tests' names start with test
'''


class Test(AbstractTest):
    # def test_customer(self) -> None:
    #    c1 = Customer(1, 'a1')
    #    self.assertEqual(ReturnValue.OK, Solution.add_customer(c1), 'regular customer')
    #    c2 = Customer(2, None)
    #    self.assertEqual(ReturnValue.BAD_PARAMS, Solution.add_customer(c2), 'invalid name')

    def test_owner(self):
        o = Owner(1, 'sahar')
        self.assertEqual(ReturnValue.OK, Solution.add_owner(o), "regular owner")
        o1 = Solution.get_owner(o.get_owner_id())
        self.assertEqual(o, o1, "got owner")
        o = Owner()

    def test_apartment(self):
        a = Apartment(1,"ABC","CITY","SYR",23)
        self.assertEqual(ReturnValue.OK, Solution.add_apartment(a), "regular apartment")
        a1 = Solution.get_apartment(a.get_id())
        self.assertEqual(a, a1, "got apartment")
        a = Apartment(1,"ABC","CITY","SYR",23)
        self.assertEqual(ReturnValue.ALREADY_EXISTS, Solution.add_apartment(a), "exists")

# *** DO NOT RUN EACH TEST MANUALLY ***
if __name__ == '__main__':
    unittest.main(verbosity=2, exit=False)
