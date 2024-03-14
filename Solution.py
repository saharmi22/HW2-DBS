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
def mystr(s):
    if s is None:
        return "NULL"
    elif isinstance(s, str):
        s = "'" + s + "'"
    return str(s)


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
                     "CHECK (size>0), "
                     "CHECK (id>0), "
                     "UNIQUE (address, city, country))")
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
                     "CHECK (total_price>0), "
                     "CHECK (costumer_id>0),"
                     "CHECK (apartment_id>0)) ")
        # conn.execute("CREATE OR REPLACE FUNCTION check_review(review_date DATE, apartment_id INTEGER, customer_id INTEGER) RETURNS BOOLEAN AS $$ "
        #              "BEGIN "
        #              "DECLARE "
        #                 "res_exists BOOLEAN; "
        #              "SELECT EXISTS ( "
        #                 "SELECT 1 "
        #                 "FROM Reservations "
        #                 "WHERE customer_id = $3 "
        #                 "AND apartment_id = $2 "
        #                 "AND end_date < $1"
        #              ") INTO res_exists; "
        #              "RETURN res_exists; "
        #              "END;")
        conn.execute("CREATE TABLE Reviews(costumer_id INTEGER,"
                     "apartment_id INTEGER,"
                     "review_date DATE NOT NULL,"
                     "rating INTEGER NOT NULL,"
                     "review_text TEXT NOT NULL,"
                     "FOREIGN KEY (costumer_id) REFERENCES Costumers(id),"
                     "FOREIGN KEY (apartment_id) REFERENCES Apartments(id),"
                     "PRIMARY KEY (costumer_id, apartment_id),"
                     "CHECK (rating >= 1),"
                     "CHECK (rating <= 10), "
                     "CHECK (costumer_id>0), "
                     "CHECK (apartment_id>0)) ")
        
        conn.execute("CREATE TABLE Owns(owner_id INTEGER, "
                     "apartment_id INTEGER,"
                     "CHECK (apartment_id>0), "
                     "CHECK (owner_id>0),"
                     "UNIQUE (apartment_id),"
                     "FOREIGN KEY (owner_id) REFERENCES Owners(id),"
                     "FOREIGN KEY (apartment_id) REFERENCES Apartments(id),"
                     "PRIMARY KEY (apartment_id))")
        
        conn.execute("CREATE VIEW ApartmentsRatingsAverages AS "
                     "SELECT Apartments.id as apartment_id , COALESCE(AVG(rating),0) as avg_rating "
                     "FROM Apartments LEFT OUTER JOIN Reviews "
                     "ON Apartments.id = Reviews.apartment_id "
                     " GROUP BY Apartments.id")
        
        conn.execute("CREATE VIEW RatingsOwners AS "
                     "SELECT Owners.id as owner_id, COALESCE(AVG(avg_rating),0) as owner_avg "
                     "FROM Owners LEFT OUTER JOIN Owns "
                     "ON Owners.id = Owns.owner_id "
                     "LEFT OUTER JOIN ApartmentsRatingsAverages "
                     "ON ApartmentsRatingsAverages.apartment_id = Owns.apartment_id "
                     "GROUP BY Owners.id")

        conn.execute("CREATE VIEW ApartmentsOfOwner AS "
                     "SELECT Owns.owner_id, Owns.apartment_id, Owners.name "
                     "FROM Owners INNER JOIN Owns "
                     "ON Owns.owner_id = Owners.id")
        # sahar's views :(
        conn.execute("CREATE VIEW OwnerReservations AS "
                     "SELECT Owners.name, Owns.owner_id, Reservations.costumer_id, Reservations.apartment_id, "
                     "Reservations.start_date, Reservations.end_date, Reservations.total_price "
                     "FROM Owns LEFT OUTER JOIN Reservations "
                     "ON Owns.apartment_id = Reservations.apartment_id "
                     "RIGHT OUTER JOIN Owners "
                     "ON Owns.owner_id = Owners.id")
        conn.execute("CREATE VIEW ReservationNightly AS "
                     "SELECT costumer_id, apartment_id, start_date, end_date, total_price, "
                     "(end_date - start_date)::float AS nights,"
                     "total_price / ((end_date - start_date)) AS nightly_price "
                     "FROM Reservations;")
        conn.execute("CREATE VIEW ReviewsVal AS "
                     "SELECT ReservationNightly.apartment_id, "
                     "AVG(ApartmentsRatingsAverages.avg_rating) / AVG(ReservationNightly.nightly_price::float) as apar_val "
                     "FROM ReservationNightly INNER JOIN ApartmentsRatingsAverages "
                     "ON ReservationNightly.apartment_id = ApartmentsRatingsAverages.apartment_id "
                     "GROUP BY ReservationNightly.apartment_id")
        conn.execute("CREATE VIEW ApartmentVal AS "
                     "SELECT Apartments.id, Apartments.address, Apartments,city, Apartments.country, "
                     "Apartments.size, COALESCE(ReviewsVal.apar_val,0) as apar_val "
                     "FROM ReviewsVal INNER JOIN Apartments ON ReviewsVal.apartment_id = Apartments.id")
        conn.execute("CREATE VIEW ApartmentReviews AS "
                     "SELECT * "
                     "FROM Reviews "
                     "LEFT JOIN  Apartments a1 ON a1.id = Reviews.apartment_id ")
        conn.execute("CREATE VIEW ApartmentReviews2 as "
                     "SELECT ApartmentReviews.apartment_id, ApartmentReviews.rating as rating1, "
                     "ApartmentReviews.address, ApartmentReviews.city, ApartmentReviews.country, "
                     "ApartmentReviews.size, r2.costumer_id AS second_reviewer, "
                     "ApartmentReviews.costumer_id AS first_reviewer, r2.rating AS rating2 "
                     "FROM ApartmentReviews "
                     "JOIN Reviews r2 ON ApartmentReviews.apartment_id = r2.apartment_id "
                     "WHERE r2.costumer_id != ApartmentReviews.costumer_id ")
        conn.execute("CREATE VIEW ApartmentReviews3 as "
                     "SELECT ApartmentReviews2.first_reviewer, ApartmentReviews2.second_reviewer, "
                     "ApartmentReviews2.rating1, ApartmentReviews2.rating2, (rating1::float / rating2) as ratio, "
                     "ApartmentReviews.costumer_id, ApartmentReviews.rating as rating3, ApartmentReviews.apartment_id, "
                     "ApartmentReviews.address, ApartmentReviews.city, ApartmentReviews.country, ApartmentReviews.size "
                     "FROM ApartmentReviews JOIN ApartmentReviews2 "
                     "ON ApartmentReviews2.apartment_id != ApartmentReviews.apartment_id "
                     "AND ApartmentReviews2.second_reviewer = ApartmentReviews.costumer_id ")
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
        # conn.execute("DROP VIEW Owner_Citys")
        # conn.execute("DROP VIEW ApartmentReviews3 CASCADE")
        # conn.execute("DROP VIEW ApartmentReviews2 CASCADE")
        # conn.execute("DROP VIEW ApartmentReviews CASCADE")
        # conn.execute("DROP VIEW ApartmentVal CASCADE")
        # conn.execute("DROP VIEW ReviewsVal CASCADE")
        # conn.execute("DROP VIEW ReservationsNightly CASCADE")
        # conn.execute("DROP VIEW OwnerReservations CASCADE")
        # conn.execute("DROP VIEW ApartmentsOfOwner CASCADE")
        # conn.execute("DROP VIEW Ratings CASCADE")
        conn.execute("DROP TABLE Owns CASCADE")
        conn.execute("DROP TABLE Reviews CASCADE")
        conn.execute("DROP TABLE Reservations CASCADE")
        conn.execute("DROP TABLE Apartments CASCADE")
        conn.execute("DROP TABLE Owners CASCADE")
        conn.execute("DROP TABLE Costumers CASCADE")
    except Exception as e:
        print(e)


# def sub_date() :
#     conn=None
#     try:
#         conn = Connector.DBConnector()
#         n,res=conn.execute("SELECT * "
#                               "FROM ReviewsVal")
#         #print("x:",(res),":x")
#     except Exception as e:
#         print(e)
#     finally:
#         conn.close()
#         #return d

def add_owner(owner: Owner) -> ReturnValue:
    conn = None
    ret_val = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        conn.execute(
            "INSERT INTO Owners VALUES (" + mystr(owner.get_owner_id()) + ", " + mystr(owner.get_owner_name()) + ")")
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
        n, res = conn.execute("SELECT name FROM Owners WHERE id=" + mystr(owner_id))
        if res.size() > 0:
            owner = Owner(owner_id, res[0]["name"])
    except Exception as e:
        print(e)
    finally:
        conn.close()
        return owner


def delete_owner(owner_id: int) -> ReturnValue:
    if owner_id <= 0:
        return ReturnValue.BAD_PARAMS
    conn = None
    ret_val = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        n,res= conn.execute("DELETE FROM Owners WHERE id=" + mystr(owner_id))
        if n == 0:
            ret_val = ReturnValue.NOT_EXISTS
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
        conn.execute("INSERT INTO Apartments VALUES (" + mystr(apartment.get_id()) + ", " + mystr(
            apartment.get_address()) + ", " + mystr(apartment.get_city()) + ", " + mystr(
            apartment.get_country()) + ", " + mystr(apartment.get_size()) + ")")
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
    # TODO: implement
    conn = None
    apartment = Apartment.bad_apartment()
    try:
        conn = Connector.DBConnector()
        n, res = conn.execute("SELECT * FROM Apartments WHERE id=" + mystr(apartment_id))
        if res.size() > 0:
            apartment = Apartment(apartment_id, res[0]["address"], res[0]["city"], res[0]["country"], res[0]["size"])
    except Exception as e:
        print(e)
    finally:
        conn.close()
        return apartment


# todo: lior
def delete_apartment(apartment_id: int) -> ReturnValue:
    # TODO: implement
    if apartment_id <= 0:
        return ReturnValue.BAD_PARAMS
    conn = None
    ret_val = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        n, res = conn.execute("DELETE FROM Apartments WHERE id=" + mystr(apartment_id))
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
        conn.execute("INSERT INTO Costumers VALUES (" + mystr(customer.get_customer_id()) + ", "
                     + mystr(customer.get_customer_name()) + ")")
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
    customer=Customer.bad_customer()
    try:
        conn = Connector.DBConnector()
        n, res = conn.execute("SELECT name FROM Costumers WHERE id=" + mystr(customer_id))
        if res.size() > 0:
            customer = Customer(customer_id, res[0]["name"])
    except Exception as e:
        print(e)
    finally:
        conn.close()
        return customer


# todo: sahar
def delete_customer(customer_id: int) -> ReturnValue:
    if customer_id <= 0:
        return ReturnValue.BAD_PARAMS
    conn = None
    ret_val = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        n,res= conn.execute("DELETE FROM Costumers WHERE id=" + mystr(customer_id))
        if n == 0:
            ret_val = ReturnValue.NOT_EXISTS
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
    # TODO: implement
    conn = None
    ret_val = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        n,_=conn.execute(
            "INSERT INTO Reservations(costumer_id, apartment_id, start_date, end_date,total_price) "
            "SELECT " + mystr(customer_id) + ", " + mystr(apartment_id) + ", " + mystr(start_date.strftime('%Y-%m-%d')) + ", " + mystr(end_date.strftime('%Y-%m-%d')) + ", " + mystr(total_price)+
            " WHERE NOT EXISTS ("
            "SELECT * "
            "FROM Reservations "
            "WHERE apartment_id = " + mystr(apartment_id) + " AND start_date < " + mystr(end_date.strftime('%Y-%m-%d')) + " AND end_date > " + mystr(start_date.strftime('%Y-%m-%d')) + ")")
        if n == 0:
            #print("Reservation already exists\n\n\n\n\n")
            ret_val= ReturnValue.BAD_PARAMS
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
    if customer_id <= 0 or apartment_id <= 0:
        return ReturnValue.BAD_PARAMS
    conn = None
    ret_val = ReturnValue.OK
    q = "DELETE FROM Reservations WHERE costumer_id =" + mystr(customer_id) + " AND apartment_id =" + mystr(
        apartment_id) + " AND start_date = " + mystr(start_date.strftime('%Y-%m-%d')) + ""
    try:
        conn = Connector.DBConnector()
        n,res=conn.execute(q)
        if n == 0:
            ret_val = ReturnValue.NOT_EXISTS
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
    # TODO: implement
    conn = None
    ret_val = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        #x=conn.execute("SELECT * FROM Reservations WHERE costumer_id = " + mystr(customer_id) + " AND apartment_id = " + mystr(apartment_id) + " AND end_date <= " + mystr(review_date.strftime('%Y-%m-%d')))
        #print(type(x[1]), x[1]["end_date"])
        n, res = conn.execute("INSERT INTO Reviews (costumer_id,apartment_id ,review_date ,rating, review_text) "
                              "SELECT " + mystr(customer_id) + ", " + mystr(apartment_id) + ", " + mystr(review_date.strftime('%Y-%m-%d')) + ", " + mystr(rating) + ", " + mystr(review_text)
                                + " WHERE EXISTS (SELECT * FROM Reservations WHERE costumer_id = " + mystr(customer_id) + " AND apartment_id = " + mystr(apartment_id) + " AND end_date <= " + mystr(review_date.strftime('%Y-%m-%d')) + ")"
                                " OR "+mystr(customer_id)+"<=0 OR "+mystr(apartment_id)+"<=0 OR "+mystr(rating)+"<1 OR "+mystr(rating)+">10")
        #print("\n",review_date,n)
        if n == 0:
            ret_val=ReturnValue.NOT_EXISTS
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
def customer_updated_review(customer_id: int, apartment_id: int, update_date: date, new_rating: int,
                            new_text: str) -> ReturnValue:
    conn = None
    ret_val = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        q = "UPDATE Reviews SET costumer_id ="+mystr(customer_id)+", apartment_id = "+mystr(apartment_id) +", review_date = " + mystr(update_date.strftime('%Y-%m-%d')) + \
            ", rating = " + mystr(new_rating) + \
            ", review_text = " + mystr(new_text) + \
            " WHERE (costumer_id=" + mystr(customer_id) + " AND apartment_id = " + mystr(apartment_id) + \
            " AND review_date <= " + mystr(update_date.strftime('%Y-%m-%d'))+") OR "+mystr(customer_id)+"<=0 OR "+mystr(apartment_id)+"<=0 OR "+mystr(new_rating)+"<1 OR "+mystr(new_rating)+">10"
        #print(q)
        n,res=conn.execute(q)
        if n == 0:
            ret_val = ReturnValue.NOT_EXISTS
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
    # TODO: implement
    conn = None
    ret_val = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        conn.execute("INSERT INTO Owns VALUES (" + mystr(owner_id) + ", " + mystr(apartment_id) + ")")
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


# todo: lior
def get_apartment_owner(apartment_id: int) -> Owner:
    # TODO: implement
    conn = None
    owner = Owner.bad_owner()
    try:
        conn = Connector.DBConnector()
        n, res = conn.execute("SELECT owner_id FROM Owns WHERE apartment_id=" + mystr(apartment_id))
        if res.size() > 0:
            owner = get_owner(res[0]["owner_id"])
    except Exception as e:
        print(e)
    finally:
        conn.close()
        return owner


def owner_drops_apartment(owner_id: int, apartment_id: int) -> ReturnValue:
    if owner_id <= 0 or apartment_id <= 0:
        return ReturnValue.BAD_PARAMS
    conn = None
    ret_val = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        if owner_id < 0:
            return ReturnValue.BAD_PARAMS
        n,res=conn.execute(
            "DELETE FROM Owns WHERE owner_id = " + mystr(owner_id)+" AND apartment_id = " + mystr(apartment_id))
        if n == 0:
            ret_val = ReturnValue.NOT_EXISTS
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


# todo: sahar
def get_owner_apartments(owner_id: int) -> List[Apartment]:
    conn = None
    apartments = []
    try:
        conn = Connector.DBConnector()
        _, res = conn.execute("SELECT T.id, T.address, T.city, T.country, T.size "
                              "FROM (SELECT * FROM Owns INNER JOIN Apartments ON Owns.apartment_id = Apartments.id ) T "
                              "WHERE T.owner_id = " + str(owner_id))
        #print("0",res[0],"1:",res[1],"\n\n\n\n")
        for row in res:
            #print(row)
            #print(Apartment(row["id"], row["address"], row["city"], row["country"], row["size"]),"\n")
            apartments.append(Apartment(row["id"], row["address"], row["city"], row["country"], row["size"]))
    except Exception as e:
        print(e)
    finally:
        conn.close()
        return apartments


# ---------------------------------- BASIC API: ----------------------------------

# todo: lior

##Get the average rating across all reviews of apartment.
# must use view for this function
def get_apartment_rating(apartment_id: int) -> float:
    # TODO: implement
    conn = None
    rating = 0
    try:
        conn = Connector.DBConnector()
        n,res = conn.execute("SELECT avg_rating FROM ApartmentsRatingsAverages WHERE apartment_id = " + mystr(apartment_id))
        # WHERE apartment_id = " + mystr(apartment_id))
        #print(type(res))
        #print(n)
        rating = float(res["avg_rating"][0])
    except Exception as e:
        print(e)
    finally:
        conn.close()
        return rating


# todo: sahar
def get_owner_rating(owner_id: int) -> float:
    conn = None
    ret_val = 0
    try:
        conn = Connector.DBConnector()
        #print(conn.execute("SELECT * FROM ApartmentsRatingsAverages")[1])
        n,res = conn.execute("SELECT * FROM RatingsOwners WHERE owner_id = " + mystr(owner_id))

        #print(res)         
        #print(res[1],res[1]["AVG"])
        ret_val = float(res["owner_avg"][0])
        if ret_val is None:
            ret_val = 0
    except Exception as e:
        print(e)
        ret_val = -1
    finally:
        conn.close()
        return ret_val


# todo: lior
def get_top_customer() -> Customer:
    # TODO: implement
    conn = None
    customer = Customer.bad_customer()
    try:
        conn = Connector.DBConnector()
        n, res = conn.execute("SELECT id, name "
                              "FROM Costumers INNER JOIN Reservations "
                              "ON Costumers.id = Reservations.costumer_id "
                              "GROUP BY id, name "
                              "ORDER BY COUNT(id) DESC, id ASC")
        if res.size() > 0:
            customer = Customer(res[0]["id"], res[0]["name"])
    except Exception as e:
        print(e)
    finally:
        conn.close()
        return customer


# todo: sahar
def reservations_per_owner() -> List[Tuple[str, int]]:
    conn = None
    ret_val = []
    try:
        conn = Connector.DBConnector()
        _, res = conn.execute("SELECT name, COALESCE(COUNT(apartment_id),0) AS count "
                              "FROM OwnerReservations GROUP BY name")
        #print("X:",res,":X")
        for row in res:
            #print(row,"hereeeeeeeee")
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
    conn = None
    owners = []
    try:
        conn = Connector.DBConnector()
        n, res = conn.execute("SELECT T.owner_id, T.name, T.count "
                              "FROM ( "
                              "SELECT owner_id, name, COUNT(DISTINCT (city,country)) AS count "
                              "FROM ApartmentsOfOwner INNER JOIN Apartments "
                              "ON ApartmentsOfOwner.apartment_id = Apartments.id "
                              "GROUP BY owner_id, name) AS T WHERE T.count = (SELECT COUNT(DISTINCT (city,country)) FROM Apartments) ")
        #print(res)
        for row in res:
            owners.append(Owner(row["owner_id"], row["name"]))
            #print(Owner(row))
    except Exception as e:
        print(e)
    finally:
        conn.close()
        return owners


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
        #print(ret_val)  
    except Exception as e:
        print(e)
        ret_val = Apartment.bad_apartment()
    finally:
        conn.close()
        return ret_val


# todo: lior
def profit_per_month(year: int) -> List[Tuple[int, float]]:
    # TODO: implement
    conn = None
    profits = [0.0 for _ in range(13)]
    try:
        conn = Connector.DBConnector()
        n, res = conn.execute("SELECT EXTRACT(MONTH FROM end_date) AS month, 0.15 * SUM(total_price) AS profit "
                              "FROM Reservations "
                              "WHERE EXTRACT(YEAR FROM end_date) = " + mystr(year) + " "
                                                                                     "GROUP BY EXTRACT(MONTH FROM end_date) ")
        for row in res:
            profits[int(row["month"])] = float(row["profit"])
    except Exception as e:
        print(e)
    finally:
        conn.close()
        return list(enumerate(profits))[1:]


# todo: sahar
def get_apartment_recommendation(customer_id: int) -> List[Tuple[Apartment, float]]:
    conn = None
    ret_val = []
    try:
        conn = Connector.DBConnector()
        #q = "SELECT apartment_id, address, city, country, size, AVG(avg_ratio * rating3) as recommendation FROM ApartmentReview3 LEFT JOIN (SELECT first reviewer, second_reviewer, avg(ratio) as avg_ratio FROM ApartmentReview3 GROUP BY first_reviewer, second_reviewer) as avg_ratios ON avg_ratios.second_reviewer = ApartmentReview3.second_reviewer WHERE first_reviewer = " + str(customer_id) + " GROUP BY apartment_id, address, city, country, size"
        q = "SELECT dd.apartment_id as id, dd.address, dd.city, dd.country, dd.size, AVG(GREATEST(LEAST(avg_ratio*rating, 10::float),1::float)) AS recommendation " \
            " FROM(" \
                " SELECT apartmentreviews3.apartment_id, apartmentreviews3.second_reviewer, avg_ratio, apartmentreviews3.address, apartmentreviews3.city, apartmentreviews3.country, apartmentreviews3.size ,AVG(rating3) as rating" \
                " FROM apartmentreviews3 LEFT JOIN (" \
                    " SELECT first_reviewer, second_reviewer, avg(ratio) as avg_ratio " \
                    " FROM apartmentreviews3 " \
                    " GROUP BY first_reviewer, second_reviewer" \
                " ) as avg_ratios " \
                " ON avg_ratios.second_reviewer = apartmentreviews3.second_reviewer AND avg_ratios.first_reviewer = apartmentreviews3.first_reviewer" \
                " WHERE apartmentreviews3.first_reviewer = "+mystr(customer_id)+" and apartmentreviews3.apartment_id not in(" \
                    " SELECT apartment_id FROM Reviews WHERE costumer_id = "+mystr(customer_id)+")" \
                " GROUP BY apartmentreviews3.apartment_id, apartmentreviews3.second_reviewer, avg_ratio, apartmentreviews3.address, apartmentreviews3.city, apartmentreviews3.country, apartmentreviews3.size) as dd" \
            " GROUP BY dd.apartment_id, dd.address, dd.city, dd.country, dd.size"
        _, res = conn.execute(q)

        # _, res = conn.execute("SELECT apartment_id, address, city, country, size,"
        #                      "AVG(ratio * rating3) as recommendation "
        #                      "FROM ApartmentReviews3 "
        #                      "WHERE first_reviewer = " + str(customer_id) +
        #                      " AND apartment_id NOT IN("
        #                      "SELECT apartment_id "
        #                      "FROM Reviews "
        #                      "WHERE costumer_id = " + str(customer_id) +
        #                      ")"
        #                      "GROUP BY apartment_id, address, city, country, size")
        for row in res:
            ret_val.append((Apartment(row["id"], row["address"], row["city"],
                                      row["country"], row["size"]), row["recommendation"]))
    except Exception as e:
        print(e)
        ret_val = []
    finally:
        conn.close()
        return ret_val
