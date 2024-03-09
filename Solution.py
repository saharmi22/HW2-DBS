from typing import List, Tuple
from psycopg2 import sql
from datetime import date, datetime

import Utility.DBConnector as Connector
from Utility.ReturnValue import ReturnValue
from Utility.Exceptions import DatabaseException

from Business.Owner import Owner
from Business.Customer import Customer
from Business.Apartment import Apartment


# ---------------------------------- CRUD API: ----------------------------------

# todo: check if return values according to exceptions are fine

def create_tables():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("CREATE TABLE Costumers(id INTEGER PRIMARY KEY, name TEXT NOT NULL, "
                     "CHECK (id>0))")
        conn.execute("CREATE TABLE Owners(id INTEGER PRIMARY KEY, name TEXT NOT NULL,"
                     "CHECK (id>0))")
        conn.execute("CREATE TABLE Apartments(id INTEGER PRIMARY KEY, "
                     "address TEXT NOT NULL,"
                     "city TEXT NOT NULL,"
                     "country TEXT NOT NULL,"
                     "size INTEGER NOT NULL,"
                     "CHECK (size>0))")
        # todo: add dates start date as keys
        # check how to set keys with foreign keys
        conn.execute("CREATE TABLE Reservations(costumer_id INTEGER, "
                     "apartment_id INTEGER,"
                     "start_date DATE,"
                     "end_date DATE,"
                     "total_price INTEGER,"
                     "CHECK (start_date<end_date),"
                     "FOREIGN KEY (costumer_id) REFERENCES Costumers(id),"
                     "FOREIGN KEY (apartment_id) REFERENCES Apartments(id),"
                     "PRIMARY KEY (costumer_id, apartment_id, start_date),"
                     "UNIQUE (apartment_id, start_date),"
                     "CHECK (start_date<end_date),"
                     "CHECK (total_price>0))")
        conn.execute("CREATE TABLE Reviews(costumer_id INTEGER,"
                     "apartment_id INTEGER,"
                     "review_date DATE NOT NULL,"
                     "rating INTEGER NOT NULL,"
                     "review_text TEXT NOT NULL,"
                     "FOREIGN KEY (costumer_id) REFERENCES Costumers(id),"
                     "FOREIGN KEY (apartment_id) REFERENCES Apartments(id),"
                     "PRIMARY KEY (costumer_id, apartment_id),"
                     "CHECK (rating >= 1),"
                     "CHECK (rating <= 10))")
        conn.execute("CREATE TABLE Owns(owner_id INTEGER, "
                     "apartment_id INTEGER,"
                     "UNIQUE (apartment_id),"
                     "FOREIGN KEY (owner_id) REFERENCES Owners(id),"
                     "FOREIGN KEY (apartment_id) REFERENCES Apartments(id),"
                     "PRIMARY KEY (apartment_id))")
    except Exception as e:
        print(e)
    finally:
        # will happen any way after try termination or exception handling
        conn.close()


def clear_tables():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("DELETE FROM Owns")
        conn.execute("DELETE FROM Reviews")
        conn.execute("DELETE FROM Reservations")
        conn.execute("DELETE FROM Apartments")
        conn.execute("DELETE FROM Owners")
        conn.execute("DELETE FROM Costumers")
    except Exception as e:
        print(e)


def drop_tables():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("DROP TABLE Owns")
        conn.execute("DROP TABLE Reviews")
        conn.execute("DROP TABLE Reservations")
        conn.execute("DROP TABLE Apartments")
        conn.execute("DROP TABLE Owners")
        conn.execute("DROP TABLE Costumers")
    except Exception as e:
        print(e)


def add_owner(owner: Owner) -> ReturnValue:
    conn = None
    ret_val = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        conn.execute("INSERT INTO Owners VALUES (" + str(owner.get_owner_id()) + ", '" + owner.get_owner_name() + "')")
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
        ret_val = ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
        ret_val = ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
        ret_val = ReturnValue.ALREADY_EXISTS
    except Exception as e:
        print(e)
        ret_val = ReturnValue.ERROR
    finally:
        conn.close()
        return ret_val


def get_owner(owner_id: int) -> Owner:
    conn = None
    owner = Owner.bad_owner()
    try:
        conn = Connector.DBConnector()
        n, res = conn.execute("SELECT name FROM Owners WHERE id=" + str(owner_id))
        if res.size() > 0:
            owner = Owner(owner_id, res[0]["name"])
    except Exception as e:
        print(e)
    finally:
        conn.close()
        return owner


def delete_owner(owner_id: int) -> ReturnValue:
    conn = None
    ret_val = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        conn.execute("DELETE FROM Owners WHERE id=" + str(owner_id))
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
        ret_val = ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
        ret_val = ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
        ret_val = ReturnValue.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
        ret_val = ReturnValue.NOT_EXISTS
    except Exception as e:
        print(e)
        ret_val = ReturnValue.ERROR
    finally:
        conn.close()
        return ret_val


#todo: lior
def add_apartment(apartment: Apartment) -> ReturnValue:
    conn = None
    ret_val = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        conn.execute("INSERT INTO Apartments VALUES (" + str(apartment.get_id()) + ", '" + apartment.get_address() + "', '" + apartment.get_city() + "', '" + apartment.get_country() + "', " + str(apartment.get_size()) + ")")
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
        ret_val = ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
        ret_val = ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
        ret_val = ReturnValue.ALREADY_EXISTS
    except Exception as e:
        print(e)
        ret_val = ReturnValue.ERROR
    finally:
        conn.close()
        return ret_val

#todo: lior
def get_apartment(apartment_id: int) -> Apartment:
    # TODO: implement
    conn= None
    apartment = Apartment.bad_apartment()
    try:
        conn = Connector.DBConnector()
        n, res = conn.execute("SELECT * FROM Apartments WHERE id=" + str(apartment_id))
        if res.size() > 0:
            apartment = Apartment(apartment_id, res[0]["address"], res[0]["city"], res[0]["country"], res[0]["size"])
    except Exception as e:
        print(e)
    finally:
        conn.close()
        return apartment


#todo: lior
def delete_apartment(apartment_id: int) -> ReturnValue:
    # TODO: implement
    conn = None
    ret_val = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        n,res= conn.execute("DELETE FROM Apartments WHERE id=" + str(apartment_id))
        if n==0:
            ret_val=ReturnValue.NOT_EXISTS
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
        ret_val = ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
        ret_val = ReturnValue.BAD_PARAMS
    # except DatabaseException.FOREIGN_KEY_VIOLATION as e:
    #     print(e)
    #     ret_val = ReturnValue.NOT_EXISTS
    except Exception as e:
        print(e)
        ret_val = ReturnValue.ERROR
    finally:
        conn.close()
        return ret_val

#todo: sahar
def add_customer(customer: Customer) -> ReturnValue:
    # TODO: implement
    pass

#todo: sahar
def get_customer(customer_id: int) -> Customer:
    # TODO: implement
    pass

#todo: sahar
def delete_customer(customer_id: int) -> ReturnValue:
    # TODO: implement
    pass

#todo: lior
def customer_made_reservation(customer_id: int, apartment_id: int, start_date: date, end_date: date, total_price: float) -> ReturnValue:
    # TODO: implement
    conn = None
    ret_val = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        conn.execute("INSERT INTO Reservations VALUES (" + str(customer_id) + ", " + str(apartment_id) + ", '" + start_date.strftime('%Y-%m-%d') + "', '" + end_date.strftime('%Y-%m-%d') + "', " + str(total_price) + ")")
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
        ret_val = ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
        ret_val = ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
        ret_val = ReturnValue.BAD_PARAMS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
        ret_val = ReturnValue.NOT_EXISTS
    except Exception as e:
        print(e)
        ret_val = ReturnValue.ERROR
    finally:
        conn.close()
        return ret_val
    




#todo: sahar
def customer_cancelled_reservation(customer_id: int, apartment_id: int, start_date: date) -> ReturnValue:
    # TODO: implement
    pass

#todo: lior
def customer_reviewed_apartment(customer_id: int, apartment_id: int, review_date: date, rating: int, review_text: str) -> ReturnValue:
    # TODO: implement
    conn = None
    ret_val = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        conn.execute("INSERT INTO Reviews VALUES (" + str(customer_id) + ", " + str(apartment_id) + ", '" + review_date.strftime('%Y-%m-%d') + "', " + str(rating) + ", '" + review_text + "')")
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
        ret_val = ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
        ret_val = ReturnValue.BAD_PARAMS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
        ret_val = ReturnValue.NOT_EXISTS
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
        ret_val = ReturnValue.ALREADY_EXISTS 
    except Exception as e:
        print(e)
        ret_val = ReturnValue.ERROR
    finally:
        conn.close()
        return ret_val

#todo: sahar
def customer_updated_review(customer_id: int, apartmetn_id: int, update_date: date, new_rating: int, new_text: str) -> ReturnValue:
    # TODO: implement
    pass

#todo: lior
def owner_owns_apartment(owner_id: int, apartment_id: int) -> ReturnValue:
    # TODO: implement
    conn = None
    ret_val = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        conn.execute("INSERT INTO Owns VALUES (" + str(owner_id) + ", " + str(apartment_id) + ")")
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
        ret_val = ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
        ret_val = ReturnValue.BAD_PARAMS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
        ret_val = ReturnValue.NOT_EXISTS
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
        ret_val = ReturnValue.ALREADY_EXISTS
    except Exception as e:
        print(e)
        ret_val = ReturnValue.ERROR
    finally:
        conn.close()
        return ret_val

#todo: sahar
def owner_drops_apartment(owner_id: int, apartment_id: int) -> ReturnValue:
    # TODO: implement
    pass

#todo: lior
def get_apartment_owner(apartment_id: int) -> Owner:
    # TODO: implement
    conn = None
    owner = Owner.bad_owner()
    try:
        conn = Connector.DBConnector()
        n, res = conn.execute("SELECT owner_id FROM Owns WHERE apartment_id=" + str(apartment_id))
        if res.size() > 0:
            owner = get_owner(res[0]["owner_id"])
    except Exception as e:
        print(e)
    finally:
        conn.close()
        return owner

#todo: sahar
def get_owner_apartments(owner_id: int) -> List[Apartment]:
    # TODO: implement
    pass


# ---------------------------------- BASIC API: ----------------------------------

#todo: lior

##Get the average rating across all reviews of apartment.
#must use view for this function
def get_apartment_rating(apartment_id: int) -> float:
    # TODO: implement
    conn = None
    rating = 0

    

#todo: sahar
def get_owner_rating(owner_id: int) -> float:
    # TODO: implement
    pass

#todo: lior
def get_top_customer() -> Customer:
    # TODO: implement
    pass

#todo: sahar
def reservations_per_owner() -> List[Tuple[str, int]]:
    # TODO: implement
    pass


# ---------------------------------- ADVANCED API: ----------------------------------

#todo: lior
def get_all_location_owners() -> List[Owner]:
    # TODO: implement
    pass

#todo: sahar
def best_value_for_money() -> Apartment:
    # TODO: implement
    pass

#todo: lior
def profit_per_month(year: int) -> List[Tuple[int, float]]:
    # TODO: implement
    pass

#todo: sahar
def get_apartment_recommendation(customer_id: int) -> List[Tuple[Apartment, float]]:
    # TODO: implement
    pass
