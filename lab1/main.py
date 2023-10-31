import psycopg2
from threading import Thread
import time

conn = psycopg2.connect(
    host="localhost",
    database="lab2",
    user="postgres",
    password="1234")

def create_table():
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE user_counter (user_id serial PRIMARY KEY, counter integer, version integer);")
    for i in range(1,5):
        cursor.execute("INSERT INTO user_counter (user_id, counter, version) VALUES (%s, %s, %s);", (i, 0, 0))
    conn.commit()
    print("Table created")


def select_all():
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM user_counter")
    records = cursor.fetchall()
    for row in records:
        print(row)


def reset_values():
    cursor = conn.cursor()
    cursor.execute('UPDATE user_counter SET counter = 0;')
    cursor.execute('UPDATE user_counter SET version = 0;')
    conn.commit()
    cursor.close()
    print("DB was reset")
    select_all()


def lost_update():
    cursor = conn.cursor()
    for i in range(10000):
        counter = cursor.execute("SELECT counter FROM user_counter WHERE user_id = 1")
        counter = cursor.fetchone()[0]
        counter += 1
        cursor.execute("UPDATE user_counter SET counter = %s WHERE user_id = %s", (counter, '1'))
        conn.commit()
    cursor.close()


def in_place():
    cursor = conn.cursor()
    for i in range(10000):
        cursor.execute("UPDATE user_counter SET counter = counter + 1 WHERE user_id = %s;", '2')
        conn.commit()


def r_level():
    for i in range(10000):
        conn = psycopg2.connect(
            host="localhost",
            database="lab2",
            user="postgres",
            password="1234")
        cur = conn.cursor()
        counter = cur.execute("SELECT counter FROM user_counter WHERE user_id = %s FOR UPDATE;", ('3'))
        counter = cur.fetchone()[0]
        counter += 1
        cur.execute(("update user_counter set counter = %s where user_id = %s"), (counter, 3))
        conn.commit()
        cur.close()
        conn.close()


def optimistic():
    cur = conn.cursor()
    for i in range(10000):
        while True:
            cur.execute("SELECT counter, version FROM user_counter WHERE user_id = %s", ('4'))
            counter, version = cur.fetchone()
            counter += 1
            cur.execute(("update user_counter set counter = %s, version = %s where user_id = %s and version = %s"),
                        (counter, version + 1, 4, version))
            conn.commit()
            count = cur.rowcount
            if (count > 0):
                break
    cur.close()

if __name__ == '__main__':
    reset_values()
    print("----------------FIRST--TASK----------------")
    start=time.time()
    threads = [Thread(target=lost_update, args=[]) for _ in range(10)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    print("Time: %s seconds" % (time.time() - start))
    select_all()

    print("----------------SECOND--TASK----------------")
    start = time.time()
    threads = [Thread(target=in_place, args=[]) for _ in range(10)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    print("Time: %s seconds" % (time.time() - start))
    select_all()

    print("----------------THIRD--TASK----------------")
    start = time.time()
    threads = [Thread(target=r_level, args=[]) for _ in range(10)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    print("Time: %s seconds" % (time.time() - start))
    select_all()

    print("----------------FOURTH--TASK----------------")
    start = time.time()
    threads = [Thread(target=optimistic, args=[]) for _ in range(10)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    print("Time: %s seconds" % (time.time() - start))
    select_all()
