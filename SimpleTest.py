import unittest
import Solution as Solution
from Solution import *
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

    # def test_owner(self):
    #     o = Owner(1, 'sahar')
    #     self.assertEqual(ReturnValue.OK, Solution.add_owner(o), "regular owner")
    #     o1 = Solution.get_owner(o.get_owner_id())
    #     self.assertEqual(o, o1, "got owner")
    #     o = Owner()

    # def test_apartment(self):
    #     a = Apartment(1,"ABC","CITY","SYR",23)
    #     self.assertEqual(ReturnValue.OK, Solution.add_apartment(a), "regular apartment")
    #     a1 = Solution.get_apartment(a.get_id())
    #     self.assertEqual(a, a1, "got apartment")
    #     a = Apartment(1,"ABC","CITY","SYR",23)
    #     self.assertEqual(ReturnValue.ALREADY_EXISTS, Solution.add_apartment(a), "exists")

    # def test_mine(self):
    #     o = Owner(1, 'lior')
    #     self.assertEqual(ReturnValue.OK, Solution.add_owner(o), "regular owner")
    #     a = Apartment(1,"ABC","CITY","SYR",23)
    #     self.assertEqual(ReturnValue.OK, Solution.add_apartment(a), "regular apartment")
    #     a2 = Apartment(2,"ABC","CITY2","SYR",23)
    #     self.assertEqual(ReturnValue.OK, Solution.add_apartment(a2), "regular apartment")
    #     self.assertEqual(ReturnValue.OK, Solution.owner_owns_apartment(1,1), "regular owner owns apartment")
    #     self.assertEqual(ReturnValue.OK, Solution.owner_owns_apartment(1,2), "regular owner owns apartment")

    #     o2 = Owner(2, 'lion')
    #     self.assertEqual(ReturnValue.OK, Solution.add_owner(o2), "regular owner")
    #     a3 = Apartment(3,"ABC","CITY3","SYRi",23)
    #     self.assertEqual(ReturnValue.OK, Solution.add_apartment(a3), "regular apartment")
    #     a4 = Apartment(4,"ABC","CITY4","SYR",23)
    #     self.assertEqual(ReturnValue.OK, Solution.add_apartment(a4), "regular apartment")
    #     self.assertEqual(ReturnValue.OK, Solution.owner_owns_apartment(2,4), "regular owner owns apartment")
    #     self.assertEqual(ReturnValue.OK, Solution.owner_owns_apartment(2,3), "regular owner owns apartment")

    #     a3 = Apartment(5,"ABC","CITY","SYRi",23)
    #     self.assertEqual(ReturnValue.OK, Solution.add_apartment(a3), "regular apartment")
    #     a4 = Apartment(6,"ABC","CITY2","SYRo",23)
    #     self.assertEqual(ReturnValue.OK, Solution.add_apartment(a4), "regular apartment")

    #     self.assertEqual(ReturnValue.OK, Solution.owner_owns_apartment(2,5), "regular owner owns apartment")
    #     print(Solution.get_all_location_owners())
    #     self.assertEqual(ReturnValue.OK, Solution.owner_owns_apartment(2,6), "regular owner owns apartment")


    #     print(Solution.get_all_location_owners())

    def test_add_review(self) -> None:
        c1 = Customer(1, 'one')
        self.assertEqual(ReturnValue.OK, Solution.add_customer(c1), 'add customer')
        apt1 = Apartment(1, 'test_addr', 'test_city', 'test_country', 5)
        self.assertEqual(ReturnValue.OK, Solution.add_apartment(apt1), 'add apartment')
        res1 = {'customer_id': 1, 'apartment_id': 1, 'start_date': date(
            2013, 10, 5), 'end_date': date(2014, 12, 3), 'total_price': 50}
        self.assertEqual(ReturnValue.OK, Solution.customer_made_reservation(**res1), 'add reservation')
        review1 = {'customer_id': 1, 'apartment_id': 1, 'review_date': date(
            2015, 3, 3), 'rating': 5, 'review_text': 'good'}
        self.assertEqual(ReturnValue.OK, Solution.customer_reviewed_apartment(**review1), 'add review')
        self.assertEqual(ReturnValue.ALREADY_EXISTS, Solution.customer_reviewed_apartment(
            **review1), 'duplicate review add')
        review2 = {'customer_id': 1, 'apartment_id': 1, 'review_date': date(
            2011, 3, 3), 'rating': 6, 'review_text': 'good'}
        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.customer_reviewed_apartment(
            **review2), 'invalid date review add')
        review3 = {'customer_id': 1, 'apartment_id': 2, 'review_date': date(
            2015, 3, 3), 'rating': 6, 'review_text': 'good'}
        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.customer_reviewed_apartment(
            **review3), 'invalid apartment review add')
        review4 = {'customer_id': 2, 'apartment_id': 1, 'review_date': date(
            2015, 3, 3), 'rating': 6, 'review_text': 'good'}
        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.customer_reviewed_apartment(
            **review4), 'invalid customer review add')
        review5 = {'customer_id': 1, 'apartment_id': 1, 'review_date': date(
            2015, 3, 3), 'rating': 15, 'review_text': None}
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.customer_reviewed_apartment(
            **review5), 'invalid score review add')
        review6 = {'customer_id': 1, 'apartment_id': 1, 'review_date': date(
            2015, 3, 3), 'rating': 0, 'review_text': None}
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.customer_reviewed_apartment(
            **review6), 'invalid score review add')
        #print(a[0]," imhereeeeee\n\n\n\n\n\n")
# *** DO NOT RUN EACH TEST MANUALLY ***
if __name__ == '__main__':
    unittest.main(verbosity=2, exit=False)
