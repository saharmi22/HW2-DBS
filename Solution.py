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
                     "size INTEGER,"
                     "CHECK (size>0))")
        conn.execute("CREATE TABLE Reservations(costumer_id INTEGER, "
                     "apartment_id INTEGER,"
                     "start_date DATE,"
                     "end_date DATE,"
                     "total_price INTEGER,"
                     "FOREIGN KEY (costumer_id) REFERENCES Costumers(id) ON DELETE CASCADE,"
                     "FOREIGN KEY (apartment_id) REFERENCES Apartments(id) ON DELETE CASCADE,"
                     "PRIMARY KEY (costumer_id, apartment_id, start_date),"
                     "CHECK (total_price>0))")
        conn.execute("CREATE TABLE Reviews(costumer_id INTEGER,"
                     "apartment_id INTEGER,"
                     "review_date DATE,"
                     "rating INTEGER,"
                     "review_text TEXT,"
                     "FOREIGN KEY (costumer_id) REFERENCES Costumers(id) ON DELETE CASCADE,"
                     "FOREIGN KEY (apartment_id) REFERENCES Apartments(id) ON DELETE CASCADE,"
                     "PRIMARY KEY (costumer_id, apartment_id),"
                     "CHECK (rating >= 1),"
                     "CHECK (rating <= 10))")
        conn.execute("CREATE TABLE Owns(owner_id INTEGER, "
                     "apartment_id INTEGER,"
                     "FOREIGN KEY (owner_id) REFERENCES Owners(id) ON DELETE CASCADE,"
                     "FOREIGN KEY (apartment_id) REFERENCES Apartments(id) ON DELETE CASCADE,"
                     "PRIMARY KEY (apartment_id))")
        conn.execute("CREATE VIEW Ratings AS "
                     "SELECT Owns.owner_id, Owns.apartment_id, Reviews.rating "
                     "FROM Owns INNER JOIN Reviews "
                     "ON Owns.apartment_id = Reviews.apartment_id")
        conn.execute("CREATE VIEW ApartmentsOfOwner AS "
                     "SELECT Owns.owner_id, Owns.apartment_id, Owners.name "
                     "FROM Owners INNER JOIN Owns "
                     "ON Owns.owner_id = Owners.id")
        conn.execute("CREATE VIEW OwnerReservations AS "
                     "SELECT Owners.name, Owns.owner_id, Reservations.costumer_id, Reservations.apartment_id, "
                     "Reservations.start_date, Reservations.end_date, Reservations.total_price "
                     "FROM Owns INNER JOIN Reservations "
                     "ON Owns.apartment_id = Reservations.apartment_id "
                     "INNER JOIN Owners "
                     "ON Owns.owner_id = Owners.id")
        conn.execute("CREATE VIEW ReservationNightly AS "
                     "SELECT costumer_id, apartment_id, start_date, end_date, total_price, "
                     "(end_date - start_date) - 1 AS nights,"
                     "total_price / ((end_date - start_date) - 1) AS nightly_price "
                     "FROM Reservations;")
        conn.execute("CREATE VIEW ReviewsVal AS "
                     "SELECT Reviews.costumer_id, Reviews.apartment_id, "
                     "Reviews.rating / ReservationNightly.nightly_price as apar_val "
                     "FROM ReservationNightly INNER JOIN Reviews "
                     "ON Reviews.apartment_id = ReservationNightly.apartment_id")
        conn.execute("CREATE VIEW ApartmentVal AS "
                     "SELECT Apartments.id, Apartments.address, Apartments,city, Apartments.country, "
                     "Apartments.size, ReviewsVal.apar_val "
                     "FROM ReviewsVal INNER JOIN Apartments ON ReviewsVal.apartment_id = Apartments.id")
        conn.execute("CREATE VIEW ApartmentReviews AS "
                     "SELECT * "
                     "FROM Apartments "
                     "INNER JOIN Reviews r1 ON Apartments.id = r1.apartment_id "
                     "INNER JOIN Reviews r2 ON Apartments.id = r2.apartment_id AND r2.costumer_id != r1.costumer_id "
                     "INNER JOIN Reviews r3 ON Apartments.id != r3.apartment_id AND r2.costumer_id = r3.costumer_id ")
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
        conn.execute("DROP TABLE Owns CASCADE")
        conn.execute("DROP TABLE Reviews CASCADE")
        conn.execute("DROP TABLE Reservations CASCADE")
        conn.execute("DROP TABLE Apartments CASCADE")
        conn.execute("DROP TABLE Owners CASCADE")
        conn.execute("DROP TABLE Costumers CASCADE")
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
        # won't delete relevant apartments!
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


# todo: lior
def add_apartment(apartment: Apartment) -> ReturnValue:
    conn = None
    ret_val = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        conn.execute("INSERT INTO Apartments VALUES (" + str(
            apartment.get_id()) + ", '" + apartment.get_address() + "', '" + apartment.get_city() + "', '" + apartment.get_country() + "', " + str(
            apartment.get_size()) + ")")
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


# todo: lior
def get_apartment(apartment_id: int) -> Apartment:
    conn = None
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


# todo: lior
def delete_apartment(apartment_id: int) -> ReturnValue:
    conn = None
    ret_val = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        n, res = conn.execute("DELETE FROM Apartments WHERE id=" + str(apartment_id))
        if n == 0:
            ret_val = ReturnValue.NOT_EXISTS
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


# todo: sahar
def add_customer(customer: Customer) -> ReturnValue:
    conn = None
    ret_val = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        conn.execute("INSERT INTO Costumers VALUES (" + str(customer.get_customer_id()) + ", '"
                     + customer.get_customer_name() + "')")
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


# todo: sahar
def get_customer(customer_id: int) -> Customer:
    conn = None
    costumer = Customer.bad_customer()
    try:
        conn = Connector.DBConnector()
        n, res = conn.execute("SELECT name FROM Costumers WHERE id=" + str(customer_id))
        if res.size() > 0:
            costumer = Customer(customer_id, res[0]["name"])
    except Exception as e:
        print(e)
    finally:
        conn.close()
        return costumer


# todo: sahar
def delete_customer(customer_id: int) -> ReturnValue:
    conn = None
    ret_val = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        conn.execute("DELETE FROM Costumers WHERE id=" + str(customer_id))
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


# todo: lior
def customer_made_reservation(customer_id: int, apartment_id: int, start_date: date, end_date: date,
                              total_price: float) -> ReturnValue:
    conn = None
    ret_val = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        conn.execute("INSERT INTO Reservations VALUES (" + str(customer_id) + ", " + str(
            apartment_id) + ", '" + start_date.strftime('%Y-%m-%d') + "', '" + end_date.strftime(
            '%Y-%m-%d') + "', " + str(total_price) + ")")
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


# todo: sahar
def customer_cancelled_reservation(customer_id: int, apartment_id: int, start_date: date) -> ReturnValue:
    conn = None
    ret_val = ReturnValue.OK
    q = "DELETE FROM Reservations WHERE costumer_id =" + str(customer_id) + " AND apartment_id =" + str(
        apartment_id) + " AND start_date = '" + (start_date.strftime('%Y-%m-%d')) + "'"
    try:
        conn = Connector.DBConnector()
        conn.execute(q)
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


# todo: lior
def customer_reviewed_apartment(customer_id: int, apartment_id: int, review_date: date, rating: int,
                                review_text: str) -> ReturnValue:
    conn = None
    ret_val = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        conn.execute(
            "INSERT INTO Reviews VALUES (" + str(customer_id) + ", " + str(apartment_id) + ", '" + review_date.strftime(
                '%Y-%m-%d') + "', " + str(rating) + ", '" + review_text + "')")
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


# todo: sahar
def customer_updated_review(customer_id: int, apartmetn_id: int, update_date: date, new_rating: int,
                            new_text: str) -> ReturnValue:
    conn = None
    ret_val = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        q = "UPDATE Reviews SET review_date = '" + update_date.strftime('%Y-%m-%d') +\
            "', rating = " + str(new_rating) + \
            ", review_text = '" + new_text + "'" \
            " WHERE costumer_id=" + str(customer_id) + " AND apartment_id = " + str(apartmetn_id) + \
            " AND review_date < '" + update_date.strftime('%Y-%m-%d') +"'"
        conn.execute(q)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
        ret_val = ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
        ret_val = ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
        ret_val = ReturnValue.NOT_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
        ret_val = ReturnValue.NOT_EXISTS
    except Exception as e:
        print(e)
        ret_val = ReturnValue.ERROR
    finally:
        conn.close()
        return ret_val


# todo: lior
def owner_owns_apartment(owner_id: int, apartment_id: int) -> ReturnValue:
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


# todo: sahar
def owner_drops_apartment(owner_id: int, apartment_id: int) -> ReturnValue:
    conn = None
    ret_val = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        if owner_id < 0:
            return ReturnValue.BAD_PARAMS
        conn.execute("DELETE FROM Owns WHERE apartment_id = " + str(apartment_id) + " AND owner_id = " + str(owner_id))
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
        ret_val = ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
        ret_val = ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
        ret_val = ReturnValue.NOT_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
        ret_val = ReturnValue.NOT_EXISTS
    except Exception as e:
        print(e)
        ret_val = ReturnValue.ERROR
    finally:
        conn.close()
        return ret_val


# todo: lior
def get_apartment_owner(apartment_id: int) -> Owner:
    # TODO: implement
    pass


# todo: sahar
def get_owner_apartments(owner_id: int) -> List[Apartment]:
    conn = None
    apartments = []
    try:
        conn = Connector.DBConnector()
        _, res = conn.execute("SELECT id, address, city, country, size "
                              "FROM Owns, Apartments "
                              "WHERE owner_id = " + str(owner_id))
        for row in res:
            apartments.append(Apartment(res["id"], res["address"], res["city"], res["country"], res["size"]))
    except Exception as e:
        print(e)
    finally:
        conn.close()
        return apartments


# ---------------------------------- BASIC API: ----------------------------------

# todo: lior
def get_apartment_rating(apartment_id: int) -> float:
    # TODO: implement
    pass


# todo: sahar
def get_owner_rating(owner_id: int) -> float:
    conn = None
    ret_val = 0
    try:
        conn = Connector.DBConnector()
        res = conn.execute("SELECT AVG(rating) FROM Ratings WHERE owner_id = " + str(owner_id))
        ret_val = res[1]["AVG"][0]
    except Exception as e:
        print(e)
        ret_val = -1
    finally:
        conn.close()
        return ret_val


# todo: lior
def get_top_customer() -> Customer:
    # TODO: implement
    pass


# todo: sahar
def reservations_per_owner() -> List[Tuple[str, int]]:
    conn = None
    ret_val = []
    try:
        conn = Connector.DBConnector()
        _ , res = conn.execute("SELECT name, COUNT(apartment_id) "
                           "FROM OwnerReservations "
                           "GROUP BY name")
        for row in res:
            ret_val.append((row["name"], row["count"]))
    except Exception as e:
        print(e)
        ret_val = []
    finally:
        conn.close()
        return ret_val


# ---------------------------------- ADVANCED API: ----------------------------------

# todo: lior
def get_all_location_owners() -> List[Owner]:
    # TODO: implement
    pass


# todo: sahar
def best_value_for_money() -> Apartment:
    conn = None
    ret_val = None
    try:
        conn = Connector.DBConnector()
        _, res = conn.execute("SELECT * "
                           "FROM ApartmentVal "
                           "ORDER BY apar_val DESC "
                           "LIMIT 1")
        ret_val = Apartment(res[0]["id"], res[0]["address"], res[0]["city"],
                            res[0]["country"], res[0]["size"])
    except Exception as e:
        print(e)
        ret_val = Apartment.bad_apartment()
    finally:
        conn.close()
        return ret_val


# todo: lior
def profit_per_month(year: int) -> List[Tuple[int, float]]:
    # TODO: implement
    pass


# todo: sahar
def get_apartment_recommendation(customer_id: int) -> List[Tuple[Apartment, float]]:
    conn = None
    ret_val = []
    try:
        conn = Connector.DBConnector()
        res = conn.execute(
            "SELECT Apartments.id, Apartments.city, Apartments.address, Apartments.country, Apartments.size, "
            "recommendation"  # choose apartment and recommendation
            "WHERE recommendation IN"
            "(SELECT *, AVG(ratio * r3.rating) AS recommendation"  # calculate avg profit  
            "WHERE ratio IN"
            "(SELECT *, r1.rating / r2.rating AS ratio"  # calculate ratio
            "FROM ApartmentReviews"
            "WHERE r1.costumer_id = " + str(customer_id) +
            "AND NOT EXISTS (SELECT apartment_id, costumer_id FROM Reservations "
            "WHERE apartment_id = r3.apartment_id AND costumer_id =" + str(customer_id) + "))))")
        for row in res:
            ret_val.append((Apartment(row["Apartments.id"], row["Apartments.address"], row["Apartments.city"],
                                      row["Apartments.country"], row["Apartments.size"]), row["recommendation"]))
    except Exception as e:
        print(e)
        ret_val = []
    finally:
        conn.close()
        return ret_val

