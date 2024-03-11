import unittest
import Solution as Solution
from Utility.ReturnValue import ReturnValue
from Tests.AbstractTest import AbstractTest

from Business.Apartment import Apartment
from Business.Owner import Owner
from Business.Customer import Customer

from datetime import date, datetime
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

    def test_mine(self):
        o = Owner(1, 'lior')
        self.assertEqual(ReturnValue.OK, Solution.add_owner(o), "regular owner")
        a = Apartment(1,"ABC","CITY","SYR",23)
        self.assertEqual(ReturnValue.OK, Solution.add_apartment(a), "regular apartment")
        a2 = Apartment(2,"ABC","CITY2","SYR",23)
        self.assertEqual(ReturnValue.OK, Solution.add_apartment(a2), "regular apartment")
        self.assertEqual(ReturnValue.OK, Solution.owner_owns_apartment(1,1), "regular owner owns apartment")
        self.assertEqual(ReturnValue.OK, Solution.owner_owns_apartment(1,2), "regular owner owns apartment")

        o2 = Owner(2, 'lion')
        self.assertEqual(ReturnValue.OK, Solution.add_owner(o2), "regular owner")
        a3 = Apartment(3,"ABC","CITY3","SYR",23)
        self.assertEqual(ReturnValue.OK, Solution.add_apartment(a3), "regular apartment")
        a4 = Apartment(4,"ABC","CITY4","SYR",23)
        self.assertEqual(ReturnValue.OK, Solution.add_apartment(a4), "regular apartment")
        self.assertEqual(ReturnValue.OK, Solution.owner_owns_apartment(2,4), "regular owner owns apartment")
        self.assertEqual(ReturnValue.OK, Solution.owner_owns_apartment(2,3), "regular owner owns apartment")

        a3 = Apartment(5,"ABC","CITY","SYR",23)
        self.assertEqual(ReturnValue.OK, Solution.add_apartment(a3), "regular apartment")
        a4 = Apartment(6,"ABC","CITY2","SYR",23)
        self.assertEqual(ReturnValue.OK, Solution.add_apartment(a4), "regular apartment")

        self.assertEqual(ReturnValue.OK, Solution.owner_owns_apartment(2,5), "regular owner owns apartment")
        print(Solution.get_all_location_owners())
        self.assertEqual(ReturnValue.OK, Solution.owner_owns_apartment(2,6), "regular owner owns apartment")
        print(Solution.get_all_location_owners())
# *** DO NOT RUN EACH TEST MANUALLY ***
if __name__ == '__main__':
    unittest.main(verbosity=2, exit=False)
