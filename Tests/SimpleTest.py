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
            a = Apartment(1, "kfar hasmacha 177, technion", "haifa", "israel", 50)
            self.assertEqual(ReturnValue.OK, Solution.add_apartment(a), 'regular apartment')
            c = Customer(1, "sahar")
            self.assertEqual(ReturnValue.OK, Solution.add_customer(c), 'regular customer')
            self.assertEqual(ReturnValue.OK, Solution.customer_made_reservation(1, 1,
                                                                                date(2024, 3, 10), date(2024, 4, 10),
                                                                                1300), 'make reservation')
            # self.assertEqual(ReturnValue.OK, Solution.customer_cancelled_reservation(1, 1, date(2024, 3, 10)))
            self.assertEqual(ReturnValue.OK, Solution.customer_reviewed_apartment(1, 1, date(2024, 3, 11), 8, "this apartment is cool <3" ))
            o = Owner(2, "itay")
            self.assertEqual(ReturnValue.OK, Solution.add_owner(o), "owner")
            self.assertEqual(ReturnValue.OK, Solution.owner_owns_apartment(2, 1), "Owns")
            self.assertEqual(8.0, Solution.get_owner_rating(2))
            res = Solution.reservations_per_owner();
            self.assertEqual(ReturnValue.OK, Solution.best_value_for_money(), "idk")

            res = Solution.get_owner_apartments(2)
        except Exception as e:
            print(e)
            AbstractTest.tearDown()


# *** DO NOT RUN EACH TEST MANUALLY ***
if __name__ == '__main__':
    unittest.main(verbosity=2, exit=False)
