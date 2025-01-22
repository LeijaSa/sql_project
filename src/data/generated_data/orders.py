import random
from datetime import datetime, timedelta, date
from faker import Faker
import psycopg2

# Initialize Faker
fake = Faker()

# Number of records
NUM_ORDERS = 1000
NUM_CUSTOMERS = 1000

# Date range for orders
start_date = date(2023, 1, 1)
end_date = date(2025, 1, 22)

# Order statuses
order_statuses = ["ordered", "shipped", "delivered"]


    # Generate Orders data
orders = []
for order_id in range(1, NUM_ORDERS + 1):
    customer_id = random.randint(1, NUM_CUSTOMERS)  # Random customer_id
    order_date = fake.date_between(start_date=start_date, end_date=end_date)
        

    orders.append((customer_id, order_date))

    # Sort orders by order_date (from oldest to newest)
    orders.sort(key=lambda x: x[1])


    # Generate Shipments data
shipments = []
for order_id, order in enumerate(orders, start=1):
    customer_id, order_date = order

    # Shipments logic
    shipped_date = order_date + timedelta(days=random.randint(2, 5))  # 2-5 business days
    delivery_date = shipped_date + timedelta(days=random.randint(2, 5))  # 2-5 business days from shipped
   
        
    # Constant shipping cost
    shipping_cost = 5.90
    shipments.append((order_id, shipped_date, delivery_date, shipping_cost))

    # Insert Shipments data
    # insert_shipments_query = '''
    #     INSERT INTO Shipments (order_id, shipped_date, delivery_date, shipping_cost)
    #     VALUES (%s, %s, %s, %s)
    # '''
    # cursor.executemany(insert_shipments_query, shipments)
    # connection.commit()

    # print(f"Inserted {NUM_ORDERS} orders and shipments into the database.")
i =0
orders2 = []
for order in orders:
    orderlist = [order[0], order[1]]
    if shipments[i][1] <= end_date and shipments[i][2] >= end_date:
        orderlist.append("shipped")
    elif shipments[i][2] < end_date:
        orderlist.append("delivered")
    else:
        orderlist.append("ordered")
    orders2.append(tuple(orderlist))
    i +=1

