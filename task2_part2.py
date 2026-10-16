"""
Частина II: web-counter із Завдання 1 на трьох сховищах — in-memory, disk (fsync), PostgreSQL (in-place update).
Для кожного сховища скрипт сам запускає сервер, 10 клієнтів одночасно роблять по 10 000 викликів /inc,
перевіряється кінцеве значення (/count і напряму у сховищі = 100 000), сервер зупиняється.
Далі друкується порівняльна таблиця (береться медіанний замір) і зберігається у results/part2.md.
Запуск (PostgreSQL має працювати):
    python task2_part2.py                    # повний прогін: 3 повтори на сховище
    python task2_part2.py --quick            # швидка перевірка: 1000 запитів, 1 повтор
"""
import argparse
import os

from app.counters import DEFAULT_DSN
from tools.bench import RESULTS_DIR, median_run, run_mode, save_csv

SCENARIOS = (("memory", "In-memory"), ("disk", "Disk (fsync)"), ("postgres", "PostgreSQL (in-place update)"))


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dsn", default=DEFAULT_DSN, help="рядок підключення до PostgreSQL (або змінна PG_DSN)")
    ap.add_argument("--clients", type=int, default=10)
    ap.add_argument("--requests", type=int, default=10_000, help="запитів НА КОЖНОГО клієнта")
    ap.add_argument("--repeats", type=int, default=3, help="повторів на сховище (береться медіана)")
    ap.add_argument("--port", type=int, default=8080)
    ap.add_argument("--quick", action="store_true", help="1000 запитів, 1 повтор")
    args = ap.parse_args()
    if args.quick:
        args.requests, args.repeats = 1000, 1

    all_rows, lines = [], ["| Test scenarios | Total time | Throughput |", "|---|---:|---:|"]
    for mode, title in SCENARIOS:
        runs = run_mode(mode, args.dsn, args.clients, args.requests, args.repeats, args.port)
        all_rows += runs
        m = median_run(runs)
        lines.append(f"| {title} | {m['wall_seconds']:.2f} s | {m['rps']:.0f} req/s |")

    table = "\n".join(lines)
    print(f"\n{args.clients} клієнтів x {args.requests} запитів = {args.clients * args.requests} (медіана за RPS):\n")
    print(table)
    save_csv(all_rows, os.path.join(RESULTS_DIR, "part2.csv"))
    with open(os.path.join(RESULTS_DIR, "part2.md"), "w", encoding="utf-8") as f:
        f.write(table + "\n")


if __name__ == "__main__":
    main()
