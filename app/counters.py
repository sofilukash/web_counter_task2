"""
Три варіанти лічильника з однаковим інтерфейсом: inc() / get() / reset() / close().
 MemoryCounter   — число в оперативній пам'яті;
 DiskCounter     — число у файлі, кожен інкремент записується на диск (fsync);
 PostgresCounter — число в таблиці PostgreSQL, in-place update (Завдання 2, Частина II).
Усі класи потокобезпечні: «прочитати -> +1 -> записати» виконується як одна неподільна
операція (у Memory/Disk — через threading.Lock, у PostgreSQL — через блокування рядка в UPDATE).
"""
import os
import queue
import threading
from contextlib import contextmanager

try:  # Linux / macOS
    import fcntl
except ImportError:  # Windows
    fcntl = None
try:  # Windows
    import msvcrt
except ImportError:  # Linux / macOS
    msvcrt = None

_HAS_PREAD = hasattr(os, "pread") and hasattr(os, "pwrite")  # є на Linux/macOS, немає на Windows


class MemoryCounter: #Лічильник у пам'яті

    def __init__(self):
        self._lock = threading.Lock()
        self._value = 0

    def inc(self) -> int:
        with self._lock:
            self._value += 1
            return self._value

    def get(self) -> int:
        with self._lock:
            return self._value

    def reset(self) -> None:
        with self._lock:
            self._value = 0

    def close(self) -> None:
        pass


class DiskCounter: #Лічильник у файлі з гарантією запису на диск

    RECORD_LEN = 21  # 20 цифр + символ нового рядка

    def __init__(self, path: str, fsync: bool = True):
        self._lock = threading.Lock()
        self._fsync = fsync  # False використовуємо для експериментів
        flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_BINARY", 0)
        self._fd = os.open(path, flags, 0o644)
        try:
            self._lock_file()
        except OSError as e:
            os.close(self._fd)
            raise RuntimeError(f"Файл {path} уже використовує інший процес") from e
        if os.fstat(self._fd).st_size < self.RECORD_LEN:
            self._write(0)

    # внутрішні методи
    def _lock_file(self) -> None:
        if fcntl is not None:
            fcntl.flock(self._fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        elif msvcrt is not None:
            os.lseek(self._fd, 0, os.SEEK_SET)
            msvcrt.locking(self._fd, msvcrt.LK_NBLCK, 1)

    def _read(self) -> int:
        if _HAS_PREAD:  # POSIX: читання за позицією
            data = os.pread(self._fd, self.RECORD_LEN, 0)
        else:  # Windows: переміщуємо курсор вручну
            os.lseek(self._fd, 0, os.SEEK_SET)
            data = os.read(self._fd, self.RECORD_LEN)
        try:
            return int(data)
        except ValueError as e:
            raise RuntimeError(f"Файл лічильника пошкоджено: {data!r}") from e

    def _write(self, value: int) -> None:
        record = b"%020d\n" % value
        if _HAS_PREAD:
            os.pwrite(self._fd, record, 0)
        else:
            os.lseek(self._fd, 0, os.SEEK_SET)
            os.write(self._fd, record)
        if self._fsync:
            os.fsync(self._fd)  # <-- гарантія запису на диск

    # публічний інтерфейс
    def inc(self) -> int:
        with self._lock:
            value = self._read() + 1
            self._write(value)
            return value

    def get(self) -> int:
        with self._lock:  # щоб не прочитати значення посеред чужого запису
            return self._read()

    def reset(self) -> None:
        with self._lock:
            self._write(0)

    def close(self) -> None:
        if msvcrt is not None:  # Windows: явно зняти блокування, не чекаючи на ОС
            try:
                os.lseek(self._fd, 0, os.SEEK_SET)
                msvcrt.locking(self._fd, msvcrt.LK_UNLCK, 1)
            except OSError:
                pass
        os.close(self._fd)


# ---------------------------------------------------------------- PostgreSQL ---
DEFAULT_DSN = os.environ.get("PG_DSN", "host=localhost dbname=postgres user=postgres password=postgres")
USER_ID = 1


def init_table(conn) -> None: #Створює таблицю user_counter і рядок user_id=1
    with conn.cursor() as cur:
        cur.execute("CREATE TABLE IF NOT EXISTS user_counter ("
                    "user_id INTEGER PRIMARY KEY, "
                    "counter INTEGER NOT NULL DEFAULT 0, "
                    "version INTEGER NOT NULL DEFAULT 0)")
        cur.execute("INSERT INTO user_counter (user_id) VALUES (%s) ON CONFLICT DO NOTHING", (USER_ID,))
    conn.commit()


def reset_row(conn) -> None: #Обнуляє counter і version у рядка user_id=1
    with conn.cursor() as cur:
        cur.execute("UPDATE user_counter SET counter = 0, version = 0 WHERE user_id = %s", (USER_ID,))
    conn.commit()


class PostgresCounter: #Лічильник у PostgreSQL: in-place update `counter = counter + 1

    def __init__(self, dsn: str = DEFAULT_DSN, pool_size: int = 16):
        import psycopg2  # імпортуємо, щоб memory/disk працювали без psycopg2
        self._conns = [psycopg2.connect(dsn) for _ in range(pool_size)]
        self._pool = queue.Queue()
        for c in self._conns:
            self._pool.put(c)
        init_table(self._conns[0])

    @contextmanager
    def _conn(self):
        conn = self._pool.get()
        try:
            yield conn
        except Exception:
            conn.rollback()
            raise
        finally:
            self._pool.put(conn)

    def inc(self) -> int:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute("UPDATE user_counter SET counter = counter + 1 WHERE user_id = %s RETURNING counter",
                        (USER_ID,))
            value = cur.fetchone()[0]
            conn.commit()
            return value

    def get(self) -> int:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT counter FROM user_counter WHERE user_id = %s", (USER_ID,))
            value = cur.fetchone()[0]
            conn.commit()  # завершити транзакцію, щоб з'єднання не висіло «idle in transaction»
            return value

    def reset(self) -> None:
        with self._conn() as conn:
            reset_row(conn)

    def close(self) -> None:
        for c in self._conns:
            c.close()
