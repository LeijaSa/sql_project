
import random
from faker import Faker

fake = Faker()


european_countries = [
    "Albania", "Andorra", "Austria", "Belarus", "Belgium", "Bosnia and Herzegovina", "Bulgaria",
    "Croatia", "Cyprus", "Czech Republic", "Denmark", "Estonia", "Finland", "France", "Germany",
    "Greece", "Hungary", "Iceland", "Ireland", "Italy", "Kosovo", "Latvia", "Liechtenstein",
    "Lithuania", "Luxembourg", "Malta", "Moldova", "Monaco", "Montenegro", "Netherlands", 
    "North Macedonia", "Norway", "Poland", "Portugal", "Romania", "San Marino", "Serbia",
    "Slovakia", "Slovenia", "Spain", "Sweden", "Switzerland", "Ukraine", "United Kingdom", "Vatican City"
    ]


customers = []
NUM_CUSTOMERS = 1000
for _ in range(NUM_CUSTOMERS):
    name = fake.name()
    location = random.choice(european_countries)
    # Generate an email tied to the name
    email = f"{name.replace(' ', '.').lower()}@{fake.free_email_domain()}"
    customers.append((name, location, email))

