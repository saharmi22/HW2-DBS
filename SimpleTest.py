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
         
    def test_Advanced_API(self):
        print("Running Test: test_Advanced_API...")
        self.assertEqual(add_owner(Owner(1,"OA")),ReturnValue.OK)
        self.assertEqual(add_owner(Owner(2,"OB")),ReturnValue.OK)
        self.assertEqual(add_owner(Owner(3,"OC")),ReturnValue.OK)
        self.assertEqual(add_owner(Owner(4,"OD")),ReturnValue.OK)
        self.assertEqual(add_customer(Customer(12,"CA")),ReturnValue.OK)
        self.assertEqual(add_customer(Customer(13,"CB")),ReturnValue.OK)
        self.assertEqual(add_customer(Customer(14,"CC")),ReturnValue.OK)
        self.assertEqual(add_customer(Customer(15,"CD")),ReturnValue.OK)
        self.assertEqual(add_apartment(Apartment(5, "RA", "Haifa", "ISR", 80)), ReturnValue.OK) #
        self.assertEqual(add_apartment(Apartment(6, "RB", "Haifa", "ISR", 80)), ReturnValue.OK) #
        self.assertEqual(add_apartment(Apartment(7, "RC", "Akko", "ISR", 80)), ReturnValue.OK) #
        self.assertEqual(add_apartment(Apartment(8, "RD", "Nahariya", "ISR", 80)), ReturnValue.OK) #
        self.assertEqual(add_apartment(Apartment(9, "RE", "Haifa", "Canada", 80)), ReturnValue.OK)  #
        self.assertEqual(add_apartment(Apartment(10, "RF", "Akko", "Canada", 80)), ReturnValue.OK) #
        self.assertEqual(add_apartment(Apartment(11, "RG", "Toronto", "Canada", 80)), ReturnValue.OK) #

        self.assertEqual(get_all_location_owners(),[])
        self.assertEqual(owner_owns_apartment(1,5),ReturnValue.OK)
        self.assertEqual(get_all_location_owners(),[])
        self.assertEqual(owner_owns_apartment(1,7),ReturnValue.OK)
        self.assertEqual(get_all_location_owners(),[])
        self.assertEqual(owner_owns_apartment(1,8),ReturnValue.OK)
        self.assertEqual(get_all_location_owners(),[])
        self.assertEqual(owner_owns_apartment(1,11),ReturnValue.OK)
        self.assertEqual(get_all_location_owners(),[])
        self.assertEqual(owner_owns_apartment(1,10),ReturnValue.OK)
        self.assertEqual(get_all_location_owners(),[])
        self.assertEqual(owner_owns_apartment(1,9),ReturnValue.OK)
        self.assertEqual(get_all_location_owners(),[Owner(1,"OA")])

        self.assertEqual(add_apartment(Apartment(20, "RH", "Toronto", "Canada", 80)), ReturnValue.OK) 
        self.assertEqual(add_apartment(Apartment(21, "RI", "Akko", "Canada", 80)), ReturnValue.OK)
        self.assertEqual(add_apartment(Apartment(22, "RJ", "Akko", "Canada", 80)), ReturnValue.OK)
        self.assertEqual(add_apartment(Apartment(23, "Rk", "Nahariya", "ISR", 80)), ReturnValue.OK) 
        self.assertEqual(add_apartment(Apartment(24, "RL", "Haifa", "Canada", 80)), ReturnValue.OK) 
        self.assertEqual(add_apartment(Apartment(25, "RM", "Akko", "ISR", 80)), ReturnValue.OK) 
        self.assertEqual(owner_owns_apartment(2,6),ReturnValue.OK)
        self.assertEqual(get_all_location_owners(),[Owner(1,"OA")])
        self.assertEqual(owner_owns_apartment(2,20),ReturnValue.OK)
        self.assertEqual(get_all_location_owners(),[Owner(1,"OA")])
        self.assertEqual(owner_owns_apartment(2,21),ReturnValue.OK)
        self.assertEqual(get_all_location_owners(),[Owner(1,"OA")])
        self.assertEqual(owner_owns_apartment(2,23),ReturnValue.OK)
        self.assertEqual(get_all_location_owners(),[Owner(1,"OA")])
        self.assertEqual(owner_owns_apartment(2,25),ReturnValue.OK)
        self.assertEqual(get_all_location_owners(),[Owner(1,"OA")])
        self.assertEqual(owner_owns_apartment(2,24),ReturnValue.OK)
        self.assertEqual(get_all_location_owners(),[Owner(1,"OA"),Owner(2,"OB")])
        self.assertEqual(add_apartment(Apartment(26, "RN", "Metola", "ISR", 80)), ReturnValue.OK) 
        self.assertEqual(get_all_location_owners(),[])
        self.assertEqual(owner_owns_apartment(2,26),ReturnValue.OK)
        self.assertEqual(get_all_location_owners(),[Owner(2,"OB")])
        self.assertEqual(add_apartment(Apartment(27, "RO", "Metola", "ISR", 80)), ReturnValue.OK) 
        self.assertEqual(owner_owns_apartment(1,27),ReturnValue.OK)
        self.assertEqual(get_all_location_owners(),[Owner(1,"OA"),Owner(2,"OB")])

        # --------------------------------------- PROFIT TEST START --------------------------------------- #
        profitPerMonth : List[Tuple[int, float]] = []
        for i in range(1,13):
            profitPerMonth.append((i,0))

        self.assertEqual(profit_per_month(2023),profitPerMonth)
        self.assertEqual(profit_per_month(2024),profitPerMonth)
        # January   
        d1 = date(2023,1,10)
        d2 = date(2023,1,20)
        d3 = date(2023,1,22)
        d4 = date(2023,1,27)
        self.assertEqual(customer_made_reservation(12,5,d1,d2,1000),ReturnValue.OK) #100 per night
        profitPerMonth[0] = (1,1000*0.15)
        self.assertEqual(profit_per_month(2023),profitPerMonth)
        self.assertEqual(best_value_for_money(),Apartment(5, "RA", "Haifa", "ISR", 80))
        self.assertEqual(customer_made_reservation(12,6,d3,d4,2000),ReturnValue.OK) #400 per night
        profitPerMonth[0] = (1,3000*0.15)
        self.assertEqual(profit_per_month(2023),profitPerMonth)

        # March
        d5 = date(2023,3,15)
        d6 = date(2023,3,19)
        self.assertEqual(customer_made_reservation(13,5,d5,d6,2000),ReturnValue.OK) #500 per night
        profitPerMonth[2] = (3,2000*0.15)
        self.assertEqual(profit_per_month(2023),profitPerMonth)

        #April
        d7 = date(2023,4,1)
        d8 = date(2023,4,5)
        d9 = date(2023,4,10)
        d10 = date(2023,4,15)
        d11 = date(2023,4,20)
        d12 = date(2023,5,1)
        self.assertEqual(customer_made_reservation(12,8,d7,d8,4000),ReturnValue.OK) #1000 per night
        profitPerMonth[3] = (4,4000*0.15)
        self.assertEqual(profit_per_month(2023),profitPerMonth)
        self.assertEqual(customer_made_reservation(12,8,d9,d10,3000),ReturnValue.OK) #600 per night
        profitPerMonth[3] = (4,7000*0.15)
        self.assertEqual(profit_per_month(2023),profitPerMonth)
        self.assertEqual(customer_made_reservation(12,8,d11,d12,2000),ReturnValue.OK) #200 per night
        profitPerMonth[4] = (5,2000*0.15)
        self.assertEqual(profit_per_month(2023),profitPerMonth)

        # June + July
        d13 = date(2023,6,10)
        d14 = date(2023,6,15)
        d15 = date(2023,7,15)
        self.assertEqual(customer_made_reservation(12,9,d13,d14,8000),ReturnValue.OK) #1600 per night
        profitPerMonth[5] = (6,8000*0.15)
        self.assertEqual(profit_per_month(2023),profitPerMonth)
        self.assertEqual(customer_made_reservation(12,9,d14,d15,6720),ReturnValue.OK) #224 per night
        profitPerMonth[6] = (7,6720*0.15)
        self.assertEqual(profit_per_month(2023),profitPerMonth)


        #Jan 2024
        d16 = date(2024,1,1)
        d17 = date(2024,1,11)
        self.assertEqual(customer_made_reservation(14,9,d16,d17,10000),ReturnValue.OK) #1000 per night
        self.assertEqual(profit_per_month(2023),profitPerMonth)
        profitPerMonth = []
        for i in range(1,13):
            profitPerMonth.append((i,0))
        profitPerMonth[0] = (1,10000*0.15)
        self.assertEqual(profit_per_month(2024),profitPerMonth)

        # --------------------------------------- PROFIT TEST END --------------------------------------- #
        d18 = date(2025,1,1)
        self.assertEqual(customer_reviewed_apartment(12,6,d18,4,"ok"),ReturnValue.OK)
        self.assertEqual(best_value_for_money(),Apartment(6, "RB", "Haifa", "ISR", 80))
        self.assertEqual(customer_reviewed_apartment(12,9,d18,10,"ok"),ReturnValue.OK)
        self.assertEqual(best_value_for_money(),Apartment(9, "RE", "Haifa", "Canada", 80))
        self.assertEqual(customer_reviewed_apartment(14,9,d18,6,"ok"),ReturnValue.OK)
        self.assertEqual(best_value_for_money(),Apartment(6, "RB", "Haifa", "ISR", 80))
        self.assertEqual(customer_reviewed_apartment(12,8,d18,10,"ok"),ReturnValue.OK)
        self.assertEqual(best_value_for_money(),Apartment(8, "RD", "Nahariya", "ISR", 80))
        self.assertEqual(customer_reviewed_apartment(13,5,d18,8,"ok"),ReturnValue.OK)
        self.assertEqual(best_value_for_money(),Apartment(5, "RA", "Haifa", "ISR", 80))
        self.assertEqual(customer_reviewed_apartment(12,5,d18,10,"ok"),ReturnValue.OK)
        self.assertEqual(best_value_for_money(),Apartment(5, "RA", "Haifa", "ISR", 80))
        self.assertEqual(add_apartment(Apartment(1,'A','A','A',10000)),ReturnValue.OK)
        self.assertEqual(add_customer(Customer(1,'CCCA')),ReturnValue.OK)
        dAAA1 = date(2022,2,2)
        dAAA2 = date(2022,2,5)
        self.assertEqual(customer_made_reservation(1,1,dAAA1,dAAA2,900),ReturnValue.OK)
        self.assertEqual(customer_reviewed_apartment(1,1,dAAA2,10,"YA"),ReturnValue.OK)
        self.assertEqual(best_value_for_money(),Apartment(1,'A','A','A',10000))

        print("// ==== test_Advanced_API: SUCCESS! ==== //")

# *** DO NOT RUN EACH TEST MANUALLY ***
if __name__ == '__main__':
    unittest.main(verbosity=2, exit=False)
