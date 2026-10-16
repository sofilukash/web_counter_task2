# Task 2 — лічильник у PostgreSQL (Python)

Побудовано на коді Task 1 (`app/`, `tools/loadgen.py`): додано `PostgresCounter`, режим `--mode postgres`
у сервері та два скрипти. Без ORM, драйвер — `psycopg2`. Кожен запис — окрема транзакція (`commit`).

## Підготовка

```bash
python3 -m venv .venv && source .venv/bin/activate     # Windows: .venv\Scripts\activate
pip install -r requirements.txt
docker run --name pg -e POSTGRES_PASSWORD=postgres -p 5432:5432 -d postgres:16   # або свій PostgreSQL
# інші параметри підключення (за замовчуванням: host=localhost dbname=postgres user=postgres password=postgres):
export PG_DSN="host=... port=5432 dbname=... user=... password=..."
```

Таблиця `user_counter (user_id PK, counter, version)` створюється автоматично; рядок `user_id=1` скидається перед кожним замірами.

## Запуск

| Що | Команда | Результат |
|---|---|---|
| Частина I (6 замірів) | `python task2_part1.py` | таблиця в консолі + `results/part1.md` |
| Частина II (3 сховища) | `python task2_part2.py` | таблиця в консолі + `results/part2.md`, `results/part2.csv` |
| Швидка перевірка | `python task2_part1.py --iterations 1000` · `python task2_part2.py --quick` | |
| Автотести | `python -m unittest -v tests.test_counters` | тести PostgreSQL пропускаються, якщо БД недоступна (вони змінюють рядок `user_id=1`) |

## Частина I — варіанти (10 потоків × 10 000, окреме підключення на потік)

| Варіант | Суть | Очікуваний підсумок |
|---|---|---|
| Lost-update | `SELECT` → `+1` → `UPDATE counter = %s` (READ COMMITTED) | **< 100 000**, оновлення губляться |
| Serializable (без повтору) | той самий код, рівень SERIALIZABLE | помилки 40001, **< 100 000**, але без «тихих» втрат |
| Serializable + retry | при 40001: `rollback` і повтор усієї транзакції | 100 000 |
| In-place update | `UPDATE ... SET counter = counter + 1` | 100 000 |
| Row-level locking | `SELECT ... FOR UPDATE` → `+1` → `UPDATE` | 100 000 |
| Optimistic concurrency | `UPDATE ... WHERE version = <прочитана>`; `rowcount = 0` → повтор | 100 000 |

Колонки таблиці: час, інкрементів/с, кінцеве значення, **втрачено оновлень** (= закомічено − кінцеве значення), відкатів/повторів.

### Відповіді на питання про SERIALIZABLE

* **Чи буде втрата значень?** Ні, «тихої» втрати немає: PostgreSQL не дозволяє закомітити транзакцію, що конфліктує з
  іншою, тож кінцеве значення = кількості успішно закомічених інкрементів (колонка «Втрачено оновлень» = 0).
* **Чи будуть помилки?** Так: `could not serialize access due to concurrent update` (SQLSTATE `40001`,
  `psycopg2.errors.SerializationFailure`). Невдалі інкременти без обробки не виконуються, тому кінцеве значення < 100 000.
* **Чи можна виправити?** Так — ловити помилку, робити `rollback` і **повторювати всю транзакцію** (`serializable_retry`).
  Результат завжди 100 000; ціна — багато повторів і більший час.

## Частина II

`task2_part2.py` для кожного сховища (in-memory, disk з `fsync`, PostgreSQL in-place update) сам запускає сервер,
10 клієнтів одночасно роблять по 10 000 `/inc`, перевіряється `/count` **і** значення напряму у сховищі (файл / таблиця) = 100 000.
У таблицю потрапляє медіанний (за RPS) замір з 3 повторів: `Total time` = час від спільного старту до завершення
останнього клієнта, `Throughput` = 100 000 / `Total time`.

Надійність: `commit` у PostgreSQL скидає WAL на диск (`synchronous_commit = on` за замовчуванням) — це аналог `fsync` у disk-варіанті,
тому налаштування не змінювати. Сервер, клієнти й PostgreSQL на одній машині ділять CPU — порівнюйте співвідношення, а не абсолютні числа.
