import unittest
import Solution as Solution
from Utility.ReturnValue import ReturnValue
from Tests.AbstractTest import AbstractTest
from datetime import date, datetime
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
        try:
            a1 = Apartment(1, "kfar hasmacha 177, technion", "haifa", "israel", 50)
            a2 = Apartment(2, "armonim 18", "kiryat yam", "Israet", 200)
            self.assertEqual(ReturnValue.OK, Solution.add_apartment(a1), 'meonot')
            self.assertEqual(ReturnValue.OK, Solution.add_apartment(a2), 'home')
            c1 = Customer(1, "sahar")
            c2 = Customer(2, "itay")
            self.assertEqual(ReturnValue.OK, Solution.add_customer(c1), 'sahar')
            self.assertEqual(ReturnValue.OK, Solution.add_customer(c2), 'itay')
            self.assertEqual(ReturnValue.OK, Solution.customer_reviewed_apartment(1, 1, date(2024, 3, 11), 8, "this apartment is cool <3" ))
            self.assertEqual(ReturnValue.OK, Solution.customer_reviewed_apartment(2, 1, date(2024, 3, 11), 7, "this apartment is cool <3" ))
            self.assertEqual(ReturnValue.OK, Solution.customer_reviewed_apartment(2, 2, date(2024, 1, 25), 7, "wow!"))
            res = Solution.get_apartment_recommendation(1)
        except Exception as e:
            print(e)
            AbstractTest.tearDown()


# *** DO NOT RUN EACH TEST MANUALLY ***
if __name__ == '__main__':
    unittest.main(verbosity=2, exit=False)
