import psycopg2 as psycopg2
from config import config
import numpy as np
import faker as faker
from generated_data.suppliers_data import suppliers_data
from generated_data.customers import customers
from generated_data.products import products_data
from generated_data.orders import orders2, shipments


def populate_suppliers_table():
    command=(
        """
        INSERT INTO suppliers (name, contact_info, country)
        VALUES (%s, %s, %s);
        """)
    try:
        with psycopg2.connect(**config()) as conn:
             with conn.cursor() as cur:
                  cur.executemany(command, suppliers_data)
                  conn.commit()
                  print("Suppliers table populated successfully")
    except (psycopg2.DatabaseError, Exception) as error:
            print(error)

def populate_products_table():
    command=(
        """
        INSERT INTO products (name, category, price, supplier_id, stock_quantity)
        VALUES (%s, %s, %s, %s, %s);
        """)
    try:
        with psycopg2.connect(**config()) as conn:
             with conn.cursor() as cur:
                  cur.executemany(command, products_data)
                  conn.commit()
                  print("Products table populated successfully")
    except (psycopg2.DatabaseError, Exception) as error:
            print(error)


def populate_order_items(num_items=1000):
    try:
        with psycopg2.connect(**config()) as conn:
            with conn.cursor() as cur:
                # Fetch all product ids and prices
                cur.execute("SELECT id, price FROM products;")
                products = cur.fetchall()

                for i in range(1, num_items + 1):
                    # Choose a random product
                    random_product = products[np.random.randint(0, len(products))]
                    product_id, price = random_product

                    order_id=i
                    quantity = np.random.randint(1, 11)  # Quantity 1 to 10
                    price_at_purchase = quantity * price

                    # Insert into order_items
                    insert_command = """
                        INSERT INTO order_items (order_id, product_id, quantity, price_at_purchase)
                        VALUES (%s, %s, %s, %s);
                    """
                    cur.execute(insert_command, (order_id, product_id, quantity, price_at_purchase))
        conn.commit() 
    except (psycopg2.Error, Exception) as error:
        print(error)


def populate_customers_table():
    con = None
    try:
        con = psycopg2.connect(**config())
        cursor = con.cursor()
        # Insert the customer data into the database
        SQL = '''
        INSERT INTO customers (name, location, email)
        VALUES (%s, %s, %s)
        '''
        cursor.executemany(SQL, customers)
        con.commit()
    except Exception as e:
        print("An error occurred:", e)
    finally:
        if con is not None:
            con.close()

def populate_orders_table():
    con = None
    try:
        con = psycopg2.connect(**config())
        cursor = con.cursor()
        # Insert the customer data into the database
        SQL = '''
        INSERT INTO Orders (customer_id, order_date, order_status)
        VALUES (%s, %s, %s)
        '''
        cursor.executemany(SQL, orders2)
        con.commit()
    except Exception as e:
        print("An error occurred:", e)
    finally:
        if con is not None:
            con.close()

def populate_shipments_table():
     command=(
        """
        INSERT INTO shipments (order_id, shipped_date, delivery_date, shipping_cost)
        VALUES (%s, %s, %s, %s);
        """)
     try:
         with psycopg2.connect(**config()) as conn:
             with conn.cursor() as cur:
                  cur.executemany(command, shipments)
                  conn.commit()
                  print("Shipments table populated successfully")
     except (psycopg2.DatabaseError, Exception) as error:
             print(error)

def main():
    populate_suppliers_table()
    populate_products_table()
    populate_customers_table()
    populate_orders_table()
    populate_order_items()
    populate_shipments_table()
    

if __name__ == "__main__":
     main()