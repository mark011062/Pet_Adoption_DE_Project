import csv
import uuid
import random
from datetime import datetime, timedelta
import os

# ------------------------
# CONFIGURATION
# ------------------------
RAW_FOLDER = "raw"
NUM_RECORDS = 444 
TODAY = datetime.today()

# Pet types and expanded breeds
PET_TYPES = ["Dog", "Cat"]

BREEDS = {
    "Dog": [
        "Labrador Retriever", "Golden Retriever", "Beagle", "Bulldog",
        "Poodle", "Rottweiler", "German Shepherd", "Boxer",
        "Dachshund", "Yorkshire Terrier", "Shih Tzu", "Siberian Husky",
        "Great Dane", "Doberman", "Chihuahua", "Cocker Spaniel",
        "Australian Shepherd", "Border Collie", "Mastiff", "Pomeranian"
    ],
    "Cat": [
        "Siamese", "Persian", "Maine Coon", "Sphynx",
        "Bengal", "Ragdoll", "British Shorthair", "Scottish Fold",
        "Abyssinian", "Oriental Shorthair", "Birman", "Devon Rex",
        "Norwegian Forest Cat", "Russian Blue", "Turkish Van", "Himalayan"
    ]
}

# Other possible values
GENDERS = ["Male", "Female"]
SIZES = ["Small", "Medium", "Large"]
STATUSES = ["Available", "Pending", "Adopted"]
SHELTER_NAMES = ["Happy Tails Shelter", "Furry Friends Rescue", "Safe Haven Animal Shelter",
                 "Paws & Claws Rescue", "Forever Homes"]

# ------------------------
# HELPER FUNCTIONS
# ------------------------
def random_date(start_days_ago=180):
    """Return a random date within the past `start_days_ago` days."""
    delta = random.randint(0, start_days_ago)
    return (TODAY - timedelta(days=delta)).date()

def random_adopted_date(arrival_date, max_days_after=90):
    """Return adopted date after arrival or None."""
    if random.random() < 0.3:  # 30% chance the pet is adopted
        delta = random.randint(1, max_days_after)
        return arrival_date + timedelta(days=delta)
    return None

# ------------------------
# MAIN
# ------------------------
if not os.path.exists(RAW_FOLDER):
    os.makedirs(RAW_FOLDER)

snapshot_filename = f"animals_snapshot_{TODAY.strftime('%Y%m%d_%H%M%S')}.csv"
snapshot_path = os.path.join(RAW_FOLDER, snapshot_filename)

with open(snapshot_path, mode="w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    # Header
    writer.writerow([
        "pet_id", "name", "age", "gender", "size", "status",
        "date_arrived", "adopted_date", "type", "breed",
        "shelter_name", "city", "state", "snapshot_date"
    ])
    
    for _ in range(NUM_RECORDS):
        pet_type = random.choice(PET_TYPES)
        breed = random.choice(BREEDS[pet_type])
        date_arrived = random_date()
        adopted_date = random_adopted_date(date_arrived)
        writer.writerow([
            str(uuid.uuid4()),  # pet_id
            f"Pet_{random.randint(1000,9999)}",  # name
            random.randint(0, 15),  # age
            random.choice(GENDERS),
            random.choice(SIZES),
            random.choice(STATUSES),
            date_arrived,
            adopted_date if adopted_date else "",
            pet_type,
            breed,
            random.choice(SHELTER_NAMES),
            "SomeCity",
            "SomeState",
            TODAY.date()
        ])

print(f"✅ Generated {NUM_RECORDS} mock pet records: {snapshot_path}")
