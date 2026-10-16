"""
HTTP-клієнт, який створює навантаження на веб-застосунок і міряє час.
Окремий запуск (сервер має вже працювати):
    python -m tools.loadgen --clients 10 --requests 10000
Формула з завдання:  RPS = кількість_запитів / час.
Клієнти працюють паралельно, тому «час» = від старту бар'єра до завершення
останнього клієнта (wall-clock).
"""
import argparse
import http.client
import multiprocessing as mp
import queue
import statistics
import sys
import time
from urllib.parse import urlparse


def _worker(host, port, n_requests, barrier, out_q, idx):
    #Код одного клієнта (виконується в окремому процесі)
    try:
        conn = http.client.HTTPConnection(host, port, timeout=120)
        conn.connect()
        conn.request("GET", "/count")
        conn.getresponse().read()

        latencies, errors = [], 0
        barrier.wait(timeout=120)  # <-- усі клієнти стартують разом
        t_start = time.monotonic()
        perf = time.perf_counter
        for _ in range(n_requests):
            t0 = perf()
            try:
                conn.request("GET", "/inc")
                resp = conn.getresponse()
                resp.read()  #  прочитати тіло
                if resp.status != 200:
                    errors += 1
                    continue
            except (OSError, http.client.HTTPException):
                errors += 1
                conn.close()
                conn = http.client.HTTPConnection(host, port, timeout=120)  # перепідключитись
                continue
            latencies.append(perf() - t0)
        t_end = time.monotonic()
        conn.close()
        out_q.put((idx, t_start, t_end, errors, latencies))
    except Exception as e:
        out_q.put((idx, None, None, 1, repr(e)))


def http_get(host, port, path):
    conn = http.client.HTTPConnection(host, port, timeout=60)
    try:
        conn.request("GET", path)
        resp = conn.getresponse()
        body = resp.read().decode().strip()
        if resp.status != 200:
            raise RuntimeError(f"{path}: HTTP {resp.status} {body}")
        return body
    finally:
        conn.close()


def run_load(url: str, clients: int, requests: int) -> dict:
    #Один замір: `clients` клієнтів × `requests` запитів. Повертає словник з результатами
    u = urlparse(url)
    host, port = u.hostname, u.port or 80

    http_get(host, port, "/reset")

    ctx = mp.get_context("spawn")  # однаково працює на Linux, macOS і Windows
    barrier = ctx.Barrier(clients)
    out_q = ctx.Queue()
    procs = [ctx.Process(target=_worker, args=(host, port, requests, barrier, out_q, i)) for i in range(clients)]
    for p in procs:
        p.start()

    results = []
    while len(results) < clients:
        try:
            results.append(out_q.get(timeout=1))
        except queue.Empty:
            if any(p.exitcode not in (None, 0) for p in procs):
                raise RuntimeError("Клієнтський процес аварійно завершився")
    for p in procs:
        p.join()

    failed = [r for r in results if r[1] is None]
    if failed:
        raise RuntimeError(f"Клієнт не зміг працювати: {failed[0][4]}")

    wall = max(r[2] for r in results) - min(r[1] for r in results)
    latencies = sorted(x for r in results for x in r[4])
    errors = sum(r[3] for r in results)
    expected = clients * requests
    final = int(http_get(host, port, "/count"))

    return {
        "clients": clients,
        "requests_per_client": requests,
        "total_requests": expected,
        "errors": errors,
        "wall_seconds": wall,
        "rps": (expected - errors) / wall,
        "avg_ms": statistics.fmean(latencies) * 1e3,
        "p50_ms": latencies[int(len(latencies) * 0.50)] * 1e3,
        "p99_ms": latencies[int(len(latencies) * 0.99)] * 1e3,
        "final_count": final,
        "expected_count": expected,
        "ok": final == expected and errors == 0,
    }


def format_run(label: str, r: dict) -> str:
    return (f"{label:<8} клієнтів={r['clients']:<2} запитів={r['total_requests']:<7} час={r['wall_seconds']:8.3f}с  "
            f"RPS={r['rps']:10.1f}  avg={r['avg_ms']:.3f}мс p99={r['p99_ms']:.3f}мс  "
            f"count={r['final_count']} (очікувалось {r['expected_count']})  {'OK' if r['ok'] else 'ПОМИЛКА'}")


def main() -> None:
    ap = argparse.ArgumentParser(description="Клієнт-навантажувач для web-counter")
    ap.add_argument("-u", "--url", default="http://127.0.0.1:8080")
    ap.add_argument("-c", "--clients", type=int, default=1, help="кількість одночасних клієнтів")
    ap.add_argument("-r", "--requests", type=int, default=10_000, help="запитів НА КОЖНОГО клієнта")
    args = ap.parse_args()
    res = run_load(args.url, args.clients, args.requests)
    print(format_run("", res))
    sys.exit(0 if res["ok"] else 2)


if __name__ == "__main__":
    main()
