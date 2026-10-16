"""
Частина I: способи оновлення лічильника в PostgreSQL + вимірювання часу.
Кожен варіант: 10 потоків, кожен зі СВОЇМ підключенням і курсором, по 10 000 ітерацій,
кожна ітерація = окрема транзакція (commit). Потоки стартують одночасно (Barrier).
Запуск (PostgreSQL має працювати):
    python task2_part1.py                       # DSN з --dsn або змінної PG_DSN
    python task2_part1.py --iterations 1000     # швидка перевірка
"""
import argparse
import os
import threading
import time

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_SERIALIZABLE, TransactionRollbackError

from app.counters import DEFAULT_DSN, USER_ID, init_table, reset_row

SELECT = "SELECT counter FROM user_counter WHERE user_id = %s"

def read_modify_write(conn, cur, select_sql=SELECT) -> None:
    """Одна ітерація «прочитати -> +1 -> записати» з commit"""
    cur.execute(select_sql, (USER_ID,))
    counter = cur.fetchone()[0] + 1
    cur.execute("UPDATE user_counter SET counter = %s WHERE user_id = %s", (counter, USER_ID))
    conn.commit()


def lost_update(conn, n):
    """READ COMMITTED: між SELECT і UPDATE інший потік встигає змінити значення -> оновлення губляться"""
    with conn.cursor() as cur:
        for _ in range(n):
            read_modify_write(conn, cur)
    return n, 0


def serializable_naive(conn, n):
    """Той самий код під SERIALIZABLE. PostgreSQL не дозволяє закомітити конфліктну транзакцію
    і видає помилку (SQLSTATE 40001)"""
    errors = 0
    with conn.cursor() as cur:
        for _ in range(n):
            try:
                read_modify_write(conn, cur)
            except TransactionRollbackError:
                conn.rollback()
                errors += 1
    return n - errors, errors


def serializable_retry(conn, n):
    """SERIALIZABLE + повтор усієї транзакції після помилки 40001 -> кінцеве значення завжди коректне"""
    retries = 0
    with conn.cursor() as cur:
        for _ in range(n):
            while True:
                try:
                    read_modify_write(conn, cur)
                    break
                except TransactionRollbackError:
                    conn.rollback()
                    retries += 1
    return n, retries


def in_place(conn, n):
    """Інкремент повністю на боці БД: UPDATE блокує рядок, паралельні UPDATE-и виконуються по черзі."""
    with conn.cursor() as cur:
        for _ in range(n):
            cur.execute("UPDATE user_counter SET counter = counter + 1 WHERE user_id = %s", (USER_ID,))
            conn.commit()
    return n, 0


def row_lock(conn, n):
    """SELECT ... FOR UPDATE блокує рядок до commit, тож «прочитати -> +1 -> записати» стає неподільним"""
    with conn.cursor() as cur:
        for _ in range(n):
            read_modify_write(conn, cur, SELECT + " FOR UPDATE")
    return n, 0


def optimistic(conn, n):
    """Без блокувань: UPDATE спрацює лише якщо version не змінилась; інакше (rowcount = 0) — повтор"""
    retries = 0
    with conn.cursor() as cur:
        for _ in range(n):
            while True:
                cur.execute("SELECT counter, version FROM user_counter WHERE user_id = %s", (USER_ID,))
                counter, version = cur.fetchone()
                cur.execute("UPDATE user_counter SET counter = %s, version = %s "
                            "WHERE user_id = %s AND version = %s", (counter + 1, version + 1, USER_ID, version))
                updated = cur.rowcount
                conn.commit()
                if updated > 0:
                    break
                retries += 1
    return n, retries


VARIANTS = [  # (назва, функція, рівень ізоляції; None = READ COMMITTED за замовчуванням)
    ("Lost-update", lost_update, None),
    ("Serializable (без повтору)", serializable_naive, ISOLATION_LEVEL_SERIALIZABLE),
    ("Serializable + retry", serializable_retry, ISOLATION_LEVEL_SERIALIZABLE),
    ("In-place update", in_place, None),
    ("Row-level locking", row_lock, None),
    ("Optimistic concurrency", optimistic, None),
]


# -------------------------------------------------------------------- запуск ---
def run_variant(dsn, admin, name, worker, isolation, threads, iterations) -> dict:
    reset_row(admin)
    conns = [psycopg2.connect(dsn) for _ in range(threads)]  # окреме підключення на потік
    if isolation is not None:
        for c in conns:
            c.set_isolation_level(isolation)

    barrier = threading.Barrier(threads + 1)
    results = [None] * threads

    def run(i):
        barrier.wait()
        try:
            results[i] = worker(conns[i], iterations)
        except Exception as e:  # не губити помилку потоку
            results[i] = e

    pool = [threading.Thread(target=run, args=(i,)) for i in range(threads)]
    for t in pool:
        t.start()
    barrier.wait()
    t0 = time.perf_counter()
    for t in pool:
        t.join()
    seconds = time.perf_counter() - t0
    for c in conns:
        c.close()

    for r in results:
        if isinstance(r, Exception):
            raise r
    with admin.cursor() as cur:
        cur.execute(SELECT, (USER_ID,))
        final = cur.fetchone()[0]
    admin.commit()

    committed = sum(r[0] for r in results)
    return {"name": name, "seconds": seconds, "rate": committed / seconds, "final": final,
            "lost": committed - final, "rollbacks": sum(r[1] for r in results)}


def format_table(rows, expected) -> str:
    lines = [f"| Варіант | Час, с | Інкрементів/с | Кінцеве значення (очікується {expected}) "
             f"| Втрачено оновлень | Відкатів / повторів |",
             "|---|---:|---:|---:|---:|---:|"]
    for r in rows:
        lines.append(f"| {r['name']} | {r['seconds']:.2f} | {r['rate']:.0f} | {r['final']} "
                     f"| {r['lost']} | {r['rollbacks']} |")
    return "\n".join(lines)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dsn", default=DEFAULT_DSN, help="рядок підключення до PostgreSQL (або змінна PG_DSN)")
    ap.add_argument("--threads", type=int, default=10)
    ap.add_argument("--iterations", type=int, default=10_000, help="ітерацій на потік")
    args = ap.parse_args()

    admin = psycopg2.connect(args.dsn)  # службове підключення: створення таблиці, скидання, перевірка
    init_table(admin)
    rows = []
    for name, worker, isolation in VARIANTS:
        r = run_variant(args.dsn, admin, name, worker, isolation, args.threads, args.iterations)
        print(f"{name:<28} {r['seconds']:8.2f} с  final={r['final']}  втрачено={r['lost']}  "
              f"відкатів/повторів={r['rollbacks']}", flush=True)
        rows.append(r)
    admin.close()

    table = format_table(rows, args.threads * args.iterations)
    print("\n" + table)
    os.makedirs("results", exist_ok=True)
    with open("results/part1.md", "w", encoding="utf-8") as f:
        f.write(table + "\n")


if __name__ == "__main__":
    main()
