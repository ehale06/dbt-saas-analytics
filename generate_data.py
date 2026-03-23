import duckdb
import random
from datetime import datetime, timedelta
from faker import Faker

fake = Faker()
Faker.seed(42)
random.seed(42)

# Connect to DuckDB
conn = duckdb.connect('clearpath.duckdb')

# ── Accounts (B2B companies using Clearpath) ──
print("Creating accounts...")
conn.execute("""
CREATE OR REPLACE TABLE raw_accounts (
    account_id VARCHAR,
    account_name VARCHAR,
    plan VARCHAR,
    mrr DECIMAL(10,2),
    industry VARCHAR,
    employee_count INTEGER,
    created_at TIMESTAMP,
    churned_at TIMESTAMP
)
""")

plans = ['starter', 'growth', 'enterprise']
plan_mrr = {'starter': 49.00, 'growth': 199.00, 'enterprise': 799.00}
industries = ['Technology', 'Healthcare', 'Finance', 'Retail', 'Education', 'Manufacturing']

accounts = []
for i in range(1, 201):
    plan = random.choice(plans)
    created = fake.date_time_between(start_date='-2y', end_date='-3m')
    churned = None
    if random.random() < 0.15:
        churned = created + timedelta(days=random.randint(30, 365))
    accounts.append((
        f'ACC{str(i).zfill(4)}',
        fake.company(),
        plan,
        plan_mrr[plan],
        random.choice(industries),
        random.choice([10, 25, 50, 100, 250, 500, 1000]),
        created,
        churned
    ))

conn.executemany("INSERT INTO raw_accounts VALUES (?,?,?,?,?,?,?,?)", accounts)
print(f"  {len(accounts)} accounts created")

# ── Users ──
print("Creating users...")
conn.execute("""
CREATE OR REPLACE TABLE raw_users (
    user_id VARCHAR,
    account_id VARCHAR,
    email VARCHAR,
    role VARCHAR,
    created_at TIMESTAMP,
    last_login_at TIMESTAMP
)
""")

roles = ['admin', 'member', 'viewer']
users = []
user_id = 1
for acc in accounts:
    num_users = random.randint(2, 15)
    for _ in range(num_users):
        created = acc[6] + timedelta(days=random.randint(0, 30))
        last_login = created + timedelta(days=random.randint(0, 180))
        users.append((
            f'USR{str(user_id).zfill(5)}',
            acc[0],
            fake.email(),
            random.choice(roles),
            created,
            last_login
        ))
        user_id += 1

conn.executemany("INSERT INTO raw_users VALUES (?,?,?,?,?,?)", users)
print(f"  {len(users)} users created")

# ── Events ──
print("Creating events...")
conn.execute("""
CREATE OR REPLACE TABLE raw_events (
    event_id VARCHAR,
    user_id VARCHAR,
    account_id VARCHAR,
    event_type VARCHAR,
    occurred_at TIMESTAMP
)
""")

event_types = [
    'login', 'create_project', 'create_task', 'complete_task',
    'invite_member', 'view_dashboard', 'export_report',
    'update_settings', 'comment', 'attach_file'
]

events = []
event_id = 1
for user in users[:500]:
    num_events = random.randint(5, 50)
    for _ in range(num_events):
        occurred = user[4] + timedelta(days=random.randint(0, 180))
        events.append((
            f'EVT{str(event_id).zfill(7)}',
            user[0],
            user[1],
            random.choice(event_types),
            occurred
        ))
        event_id += 1

conn.executemany("INSERT INTO raw_events VALUES (?,?,?,?,?)", events)
print(f"  {len(events)} events created")

# ── Subscriptions ──
print("Creating subscriptions...")
conn.execute("""
CREATE OR REPLACE TABLE raw_subscriptions (
    subscription_id VARCHAR,
    account_id VARCHAR,
    plan VARCHAR,
    mrr DECIMAL(10,2),
    started_at TIMESTAMP,
    ended_at TIMESTAMP,
    status VARCHAR
)
""")

subscriptions = []
for i, acc in enumerate(accounts):
    status = 'churned' if acc[7] else 'active'
    subscriptions.append((
        f'SUB{str(i+1).zfill(4)}',
        acc[0],
        acc[2],
        acc[3],
        acc[6],
        acc[7],
        status
    ))

conn.executemany("INSERT INTO raw_subscriptions VALUES (?,?,?,?,?,?,?)", subscriptions)
print(f"  {len(subscriptions)} subscriptions created")

conn.close()
print("\nAll raw tables created successfully in clearpath.duckdb")