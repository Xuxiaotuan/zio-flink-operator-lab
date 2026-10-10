#!/usr/bin/env python3
"""对控制面执行有界只读验收；不代表 Flink 吞吐或写操作性能。"""
import argparse
import concurrent.futures
import json
import math
import time
import urllib.request


def run(base_urls, seconds, concurrency, interval):
    started = time.monotonic()
    deadline = started + seconds

    def worker(index):
        samples = []
        n = index
        while time.monotonic() < deadline:
            base = base_urls[n % len(base_urls)]
            path = '/healthz' if n % 5 else '/v1/state?namespace=bigdata-lab'
            tick = time.monotonic()
            error = None
            try:
                with urllib.request.urlopen(base + path, timeout=10) as response:
                    body = json.load(response)
                    if path == '/healthz':
                        assert body.get('status') == 'ok', 'health body is not OK'
                    else:
                        assert body.get('namespace') == 'bigdata-lab' and isinstance(body.get('items'), list), 'invalid state body'
            except Exception as exc:
                error = str(exc)
            samples.append((path.split('?')[0], (time.monotonic() - tick) * 1000, error))
            n += 1
            if interval:
                time.sleep(min(interval, max(0, deadline - time.monotonic())))
        return samples

    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
        samples = [item for batch in pool.map(worker, range(concurrency)) for item in batch]
    elapsed = time.monotonic() - started
    errors = [error for _, _, error in samples if error is not None]
    summary = {'durationSeconds': round(elapsed, 2), 'concurrency': concurrency,
               'requests': len(samples), 'errors': len(errors), 'errorExamples': errors[:3],
               'requestsPerSecond': round(len(samples) / elapsed, 2), 'paths': {}}
    for path in sorted({path for path, _, _ in samples}):
        values = sorted(ms for p, ms, _ in samples if p == path)
        summary['paths'][path] = {'count': len(values), **{
            key: round(values[max(0, math.ceil(len(values) * fraction) - 1)], 2)
            for key, fraction in [('p50Ms', .5), ('p95Ms', .95), ('p99Ms', .99), ('maxMs', 1)]}}
    summary['passed'] = bool(samples) and len(errors) / len(samples) <= .01 and all(
        value['p95Ms'] <= (500 if path == '/healthz' else 2000)
        for path, value in summary['paths'].items())
    return summary


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--base-url', action='append', required=True)
    parser.add_argument('--seconds', type=int, default=60)
    parser.add_argument('--concurrency', type=int, default=16)
    parser.add_argument('--interval', type=float, default=0)
    args = parser.parse_args()
    if not (1 <= args.seconds <= 3600 and 1 <= args.concurrency <= 32 and args.interval >= 0):
        parser.error('seconds must be 1..3600, concurrency 1..32, interval >= 0')
    result = run(args.base_url, args.seconds, args.concurrency, args.interval)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    raise SystemExit(0 if result['passed'] else 1)
