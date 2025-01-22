import psycopg2 as psycopg2
from config import config


# Create products table
def create_products_table():
        command = (
        """
        CREATE TABLE IF NOT EXISTS products (
            id SERIAL PRIMARY KEY,
            name VARCHAR(50) NOT NULL,
            category VARCHAR(50) NOT NULL,
            price DECIMAL (10,2) NOT NULL,
            supplier_id INT NOT NULL,
            stock_quantity INTEGER NOT NULL,
            CONSTRAINT fk_suppliers FOREIGN KEY (supplier_id) REFERENCES suppliers(id) ON DELETE CASCADE  
        );
        """)
        try:
            with psycopg2.connect(**config()) as conn:
                with conn.cursor() as cur:
                    cur.execute(command)
                conn.commit()
        except (psycopg2.DatabaseError, Exception) as error:
            print(error)
            
# Create suppliers table
def create_suppliers_table():
        command = (
        """
        CREATE TABLE IF NOT EXISTS suppliers (
            id SERIAL PRIMARY KEY,
            name VARCHAR(50) NOT NULL,
            contact_info VARCHAR(50) NOT NULL,
            country VARCHAR(50) NOT NULL     
        );
        """)
        try:
            with psycopg2.connect(**config()) as conn:
                with conn.cursor() as cur:
                    cur.execute(command)
                conn.commit()
        except (psycopg2.DatabaseError, Exception) as error:
            print(error)

# Create orders table
def create_orders_table():
        command = (
        """
        CREATE TABLE IF NOT EXISTS orders (
            id SERIAL PRIMARY KEY,
            customer_id INTEGER NOT NULL,
            order_date DATE NOT NULL,
            order_status VARCHAR(50) NOT NULL, 
            CONSTRAINT fk_customers FOREIGN KEY (customer_id) REFERENCES customers(id) ON DELETE CASCADE   
        );
        """)
        try:
            with psycopg2.connect(**config()) as conn:
                with conn.cursor() as cur:
                    cur.execute(command)
                conn.commit()
        except (psycopg2.DatabaseError, Exception) as error:
            print(error)

# Create order_items table
def create_order_items_table():
    con = None
    try:
        con = psycopg2.connect(**config())
        cursor = con.cursor()
        SQL = (
            """
            CREATE TABLE IF NOT EXISTS order_items
            (id SERIAL PRIMARY KEY,
            order_id INT NOT NULL,
            product_id INT NOT NULL,
            quantity INT NOT NULL,
            price_at_purchase DECIMAL(10,2) NOT NULL,
            CONSTRAINT fk_products FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE CASCADE,
            CONSTRAINT fk_orders FOREIGN KEY (order_id) REFERENCES orders(id) ON DELETE CASCADE);
            """)
        cursor.execute(SQL)
        con.commit()
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if con is not None:
            con.close()

# Create customers table
def create_customers_table():
    con = None
    try:
        con = psycopg2.connect(**config())
        cursor = con.cursor()
        SQL = (
            """
            CREATE TABLE IF NOT EXISTS customers
            (id SERIAL PRIMARY KEY,
            name VARCHAR(50) NOT NULL,
            location VARCHAR(50) NOT NULL,
            email VARCHAR(50) NOT NULL);
            """)
        cursor.execute(SQL)
        con.commit()
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if con is not None:
            con.close()

# Create shipments table
def create_shipments_table():
    con = None
    try:
        con = psycopg2.connect(**config())
        cursor = con.cursor()
        SQL = (
            """
            CREATE TABLE IF NOT EXISTS shipments
            (id SERIAL PRIMARY KEY,
            order_id INT NOT NULL,
            shipped_date DATE NOT NULL,
            delivery_date DATE NOT NULL,
            shipping_cost DECIMAL(10,2) NOT NULL,
            CONSTRAINT fk_orders FOREIGN KEY (order_id) REFERENCES orders(id) ON DELETE CASCADE);
            """)
        cursor.execute(SQL)
        con.commit()
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if con is not None:
            con.close()
            
            

def main(): 
    create_suppliers_table()
    create_products_table()
    create_customers_table()
    create_orders_table()
    create_order_items_table()
    create_shipments_table()

if __name__ == "__main__":
    main()