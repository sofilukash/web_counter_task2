"""
Веб-застосунок «лічильник». Лише стандартна бібліотека Python.
Ендпоінти:
    GET /inc     збільшити лічильник на 1
    GET /count   повернути поточне значення
    GET /reset   обнулити (службовий, потрібен клієнту-бенчмарку для чистого старту)
Запуск:
    python -m app.server --mode memory            # лічильник у пам'яті
    python -m app.server --mode disk              # лічильник у файлі
    python -m app.server --mode postgres          # лічильник у PostgreSQL
"""
import argparse
import os
import signal
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from app.counters import DEFAULT_DSN, DiskCounter, MemoryCounter, PostgresCounter

def make_handler(counter): #Створює клас-обробник запитів, прив'язаний до конкретного лічильника

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"
        disable_nagle_algorithm = True

        def _reply(self, status: int, body: bytes) -> None:
            self.send_response(status)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            try:
                if self.path == "/inc":
                    counter.inc()
                    self._reply(200, b"ok\n")
                elif self.path == "/count":
                    self._reply(200, b"%d\n" % counter.get())
                elif self.path == "/reset":
                    counter.reset()
                    self._reply(200, b"0\n")
                else:
                    self._reply(404, b"not found\n")
            except Exception as e:
                self._reply(500, str(e).encode() + b"\n")

        def log_message(self, *args):
            pass

    return Handler

class CounterServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True
    request_queue_size = 256


def main() -> None:
    ap = argparse.ArgumentParser(description="Web-counter: /inc і /count")
    ap.add_argument("-m", "--mode", choices=["memory", "disk", "postgres"], default="memory",
                    help="де зберігати лічильник (за замовчуванням: memory)")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("-p", "--port", type=int, default=8080)
    ap.add_argument("-f", "--file", default="counter.dat", help="файл лічильника (режим disk)")
    ap.add_argument("--dsn", default=DEFAULT_DSN, help="рядок підключення до PostgreSQL (режим postgres)")
    ap.add_argument("--nofsync", action="store_true",
                    help="режим disk БЕЗ fsync — лише для експериментів, порушує гарантію запису!")
    args = ap.parse_args()

    if args.mode == "memory":
        counter = MemoryCounter()
    elif args.mode == "disk":
        counter = DiskCounter(args.file, fsync=not args.nofsync)
    else:
        counter = PostgresCounter(args.dsn)

    server = CounterServer((args.host, args.port), make_handler(counter))

    def stop(*_):
        # shutdown() не можна викликати з того ж потоку, де працює serve_forever()
        threading.Thread(target=server.shutdown, daemon=True).start()

    signal.signal(signal.SIGINT, stop)  # Ctrl+C
    signal.signal(signal.SIGTERM, stop)

    print(f"mode={args.mode} http://{args.host}:{args.port} pid={os.getpid()}", flush=True)
    try:
        server.serve_forever()
    finally:
        server.server_close()
        counter.close()


if __name__ == "__main__":
    main()
