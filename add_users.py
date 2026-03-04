"""One-time script to bulk-insert users into the users table."""
import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL not set")

USERS = [
    {"name": "Keven Eleven",         "phone": "919158478997"},
    {"name": "Pradeepkumar Surname", "phone": "918235355360"},
    {"name": "ASHMEER MALLIK",       "phone": "919971648417"},
    {"name": "Simar Singh",          "phone": "919999779599"},
    {"name": "Akshit Bhatnagar",     "phone": "919352897598"},
    {"name": "Romeo Simte",          "phone": "919362299011"},
    {"name": "Tunji Ram",            "phone": "917984193437"},
    {"name": "Pramod Singh",         "phone": "919924712863"},
    {"name": "Pintu Kumar",          "phone": "917015813460"},
    {"name": "Amit Kumar Prasad",    "phone": "916299196725"},
    {"name": "Frederic Maliakkal",   "phone": "919400546822"},
    {"name": "Vishal Mittal",        "phone": "919882323992"},
]

def main():
    conn = psycopg2.connect(DATABASE_URL, sslmode="require")
    conn.autocommit = True
    cur = conn.cursor()

    inserted = 0
    skipped = 0

    for u in USERS:
        try:
            cur.execute(
                """
                INSERT INTO users (phone, name, is_active, receive_all_updates, onboarding_complete, created_at, last_login_at, updated_at)
                VALUES (%s, %s, TRUE, TRUE, FALSE, NOW(), NOW(), NOW())
                ON CONFLICT (phone) DO UPDATE SET
                    name = EXCLUDED.name,
                    receive_all_updates = TRUE,
                    updated_at = NOW()
                RETURNING id, phone, name
                """,
                (u["phone"], u["name"]),
            )
            row = cur.fetchone()
            print(f"  ✅ {row[2]:25s}  phone={row[1]}  id={row[0]}")
            inserted += 1
        except Exception as e:
            print(f"  ❌ {u['name']}: {e}")
            skipped += 1

    cur.close()
    conn.close()
    print(f"\nDone: {inserted} inserted/updated, {skipped} failed.")

if __name__ == "__main__":
    main()
