import psycopg2 as psycopg2
from config import config
from datetime import datetime

# 1. Basic counts and sums
def get_all_orders():
    command = "SELECT COUNT(*) FROM orders"
    try:
        with psycopg2.connect(**config()) as conn:
            with conn.cursor() as cur:
                cur.execute(command)
                orders= cur.fetchone()
                print(f"Number of orders: {orders[0]}")
    except (psycopg2.DatabaseError, Exception) as error:
        print(error)


def get_total_sales():
    command = "SELECT SUM(price_at_purchase) FROM order_items"
    try:
        with psycopg2.connect(**config()) as conn:
            with conn.cursor() as cur:
                cur.execute(command)
                sum_of_orders = cur.fetchone()[0]
                if sum_of_orders:
                    print(f"Total sales: {sum_of_orders}")
    except (psycopg2.DatabaseError, Exception) as error:
        print(error)

def get_low_stock_products():
    command = "SELECT COUNT(*) FROM products WHERE stock_quantity < 10"
    try:
        with psycopg2.connect(**config()) as conn:
            with conn.cursor() as cur:
                cur.execute(command)
                products_count = cur.fetchone()
                print(f"Number of products with low quantity: {products_count[0]}")
    except (psycopg2.DatabaseError, Exception) as error:
        print(error)

# 2. Grouping and Aggregations
def get_total_sales_per_category():
    con = None
    try:
        con = psycopg2.connect(**config())
        cursor = con.cursor()
        SQL = """ 
        SELECT p.category AS category,
        SUM(oi.price_at_purchase) AS total_sales
        FROM order_items oi
        JOIN products p
        ON oi.product_id = p.id
        GROUP BY category
        ORDER BY total_sales DESC;
        """
        cursor.execute(SQL)
        total_sales = cursor.fetchall()
        for row in total_sales:
            print(f"Category: {row[0]}, Total Sales: {row[1]}")
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if con is not None:
            con.close()

def get_average_order_value():
    con = None
    try:
        con = psycopg2.connect(**config())
        cursor = con.cursor()
        SQL = """ 
        SELECT AVG(price_at_purchase) AS Average_order_value
        FROM order_items;
        """
        cursor.execute(SQL)
        avg_order = cursor.fetchall()
        print(f"Average order value: {avg_order[0][0]:.2f}")
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if con is not None:
            con.close()

def get_monthly_breakdown():
    con = None
    try:
        con = psycopg2.connect(**config())
        cursor = con.cursor()
        SQL = """
        SELECT DATE_TRUNC('month', o.order_date) AS order_month,
        COUNT(DISTINCT o.id) AS total_orders,
        SUM(oi.quantity * oi.price_at_purchase) AS total_sales
        FROM orders o
        JOIN order_items oi
        ON o.id = oi.order_id
        GROUP BY order_month
        ORDER BY order_month;
        """
        cursor.execute(SQL)
        monthly_report = cursor.fetchall()
        print("Monthly Breakdown:")
        for row in monthly_report:
            order_month = row[0].strftime('%Y-%m')  # Format month as YYYY-MM
            total_orders = row[1]
            total_sales = row[2]
            print(f"Month: {order_month}, Orders: {total_orders}, Sales: {total_sales}")
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if con is not None:
            con.close()

# 3. Joins ands Multi-table Queries
def get_list_of_orders():
    command = """SELECT o.id, c.name, oi.price_at_purchase
    FROM orders o
    JOIN customers c
    ON o.customer_id = c.id
    JOIN order_items oi
    ON o.id = oi.order_id
    ORDER BY o.id
    LIMIT 10;
    """
    try:
        with psycopg2.connect(**config()) as conn:
            with conn.cursor() as cur:
                cur.execute(command)
                list_of_orders=cur.fetchall()
                for row in list_of_orders:
                    print(f"Order ID: {row[0]}, Customer: {row[1]}, Total price: {row[2]}")
    except (psycopg2.DatabaseError, Exception) as error:
        print(error)


def top_5_customers():
    command = """
    SELECT c.name, SUM(oi.price_at_purchase) AS total_spent
    FROM customers c
    JOIN orders o
    ON c.id = o.customer_id
    JOIN order_items oi
    ON o.id = oi.order_id
    GROUP BY c.name
    ORDER BY total_spent DESC
    LIMIT 5;
    """
    try:
        with psycopg2.connect(**config()) as conn:
            with conn.cursor() as cur:
                cur.execute(command)
                top_customers = cur.fetchall()
                for row in top_customers:
                    print(f"Customer: {row[0]}, Total Spent: {row[1]}")
    except (psycopg2.DatabaseError, Exception) as error:
        print(error)
        
def top_suppliers():
    command = """
    SELECT s.name,
    COUNT(p.id) AS products_supplied
    FROM suppliers s
    JOIN products p
    ON s.id = p.supplier_id
    GROUP BY s.name
    ORDER BY products_supplied DESC;
    """
    try:
        with psycopg2.connect(**config()) as conn:
            with conn.cursor() as cur:
                cur.execute(command)
                top_suppliers = cur.fetchall()
                for row in top_suppliers:
                    print(f"Supplier: {row[0]}, Products supplied: {row[1]}")
    except (psycopg2.DatabaseError, Exception) as error:
        print(error)

# 4. Nested Queries and Subqueries
def customers_with_orders_over_threshold(threshold:int):
    command = f"""
    SELECT c.name, total_spent
    FROM customers c
    JOIN (
        SELECT customer_id, SUM(price_at_purchase) AS total_spent
        FROM orders o
        JOIN order_items oi
        ON o.id = oi.order_id
        GROUP BY customer_id
    ) AS customer_orders
    ON c.id = customer_orders.customer_id
    WHERE total_spent > {threshold}
    ORDER BY total_spent DESC;
    """
    try:
        with psycopg2.connect(**config()) as conn:
            with conn.cursor() as cur:
                cur.execute(command)
                customers = cur.fetchall()
                for row in customers:
                    print(f"Customer: {row[0]}, Total Spent: {row[1]}")
    except (psycopg2.DatabaseError, Exception) as error:
        print(error)

def get_highest_number_of_items(items:int):
    command = f"""
    SELECT o.id, oi.quantity
    FROM orders o
    JOIN order_items oi
    ON o.id = oi.order_id
    WHERE oi.quantity = {items}
    ORDER BY o.id
    """
    try:
        with psycopg2.connect(**config()) as conn:
            with conn.cursor() as cur:
                cur.execute(command)
                order_items = cur.fetchall()
                for row in order_items:
                    print(f"Order ID: {row[0]}, Quantity: {row[1]}")
    except (psycopg2.DatabaseError, Exception) as error:
        print(error)

def main():
    #get_all_orders()
    #get_total_sales()
    #get_low_stock_products()
    #print("\nGrouping and Aggregations:")
    #get_total_sales_per_category()
    #get_average_order_value()
    #print("\nMonthly Breakdown:")
    #get_monthly_breakdown()
    #print("\nJoins and Multi-table Queries:")
    #get_list_of_orders()
    #top_5_customers()
    #top_suppliers()
    #print("\nNested Queries and Subqueries:")
    #customers_with_orders_over_threshold(9000)
    #get_highest_number_of_items(10)


# if __name__ == '__main__':
#     main()