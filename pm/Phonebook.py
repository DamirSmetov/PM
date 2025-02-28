import csv
import psycopg2
from psycopg2.extras import execute_values

conn = psycopg2.connect(host = "localhost", dbname = "postgres", user="postgres", password = "damir2005d", port = "5432")
conn.autocommit = True

cur = conn.cursor()

cur.execute("""
    CREATE TEMP TABLE temp_codes (code TEXT);
""")
conn.commit()

filename = 'MRL double mix extra 1 mio_csv.csv'
codes = []

with open(filename, "r") as csvfile:
    csvreader = csv.reader(csvfile)
    for row in csvreader:
        codes.append((row[0],))  

execute_values(cur, "INSERT INTO temp_codes (code) VALUES %s", codes)
conn.commit()

brand = "MRL"
cur.execute(f"""
    UPDATE analytics
    SET brand = %s
    FROM temp_codes
    WHERE analytics.code = temp_codes.code;
""", (brand,))
conn.commit()

sku = "Marlboro double mix"
cur.execute(f"""
    UPDATE analytics
    SET sku = %s
    FROM temp_codes
    WHERE analytics.code = temp_codes.code;
""", (sku,))
conn.commit()

cur.execute("DROP TABLE temp_codes;")
cur.close()
conn.close()

            


