"""GET /jobs 逐条发送的端到端验证（真实 HTTP 服务）。

默认自行起一个 in-process 服务（模拟后端）并预置长历史，验证：
  1. 长列表：完整、按接单先后从新到旧、字段不变；首块只是响应的一部分
  2. 慢读：客户端慢读时，POST /jobs、POST /jobs/{id}/stop 照常受理，
     槽位照常交还，慢读查询最终正常完成
  3. 中途断开：本次查询的视图/快照立即释放，之后的查询照常
  4. 受理时刻语义：受理后接单的作业不在本次名单；发送期间才结束的
     作业仍报受理时的状态、归因与日志尾部

也可对已运行的服务做基础验证：python3 scripts/streamtest.py --base-url URL
（此时跳过需要预置历史的用例）。
"""
from __future__ import annotations

import argparse
import json
import socket
import sys
import threading
import time
from pathlib import Path

import httpx


# ---------------------------------------------------------------- 测试数据

def build_manager(history: int = 3000):
    """造长历史：seed00000（最旧）…seedN（最新），全部已正常结束。"""
    from app.cgroups import SimulatedBackend
    from app.jobs import Job, JobManager

    mgr = JobManager(SimulatedBackend(slots=2, limit_mib=256, reserve_mib=96))
    base = time.time() - history
    for i in range(history):
        job = Job(
            id=f"seed{i:05d}",
            slot=i % 2,
            memory_mib=8,
            hold_seconds=1,
            state="finished",
            verdict="normal_exit",
            verdict_reason="进程自行退出，退出码 0",
            created_at=base + i,
            finished_at=base + i + 1,
            prev_id=mgr._newest_id,
        )
        for n in range(3):
            job.output_tail.append(f"[sample-job] seeded line {n} of {job.id}")
        mgr.jobs[job.id] = job
        mgr._newest_id = job.id
    return mgr


def serve(app, port: int):
    import uvicorn

    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port,
                                           log_level="warning"))
    threading.Thread(target=server.run, daemon=True).start()
    deadline = time.monotonic() + 15
    while not server.started:
        if time.monotonic() > deadline:
            raise RuntimeError("服务未能启动")
        time.sleep(0.02)
    return server


# ---------------------------------------------------------------- 裸套接字读

def raw_get(host: str, port: int, path: str = "/jobs", *, read_delay: float = 0.0,
            max_reads: int | None = None, rcvbuf: int | None = None):
    """读取 chunked 响应体，返回 (body_chunks, t_first_body, elapsed, status, headers)。

    max_reads 到达即关闭连接（模拟中途断开）；rcvbuf 调小接收缓冲，
    使首个网络读只读到响应的一部分。
    """
    start = time.monotonic()
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    if rcvbuf is not None:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, rcvbuf)
    sock.settimeout(15)
    sock.connect((host, port))
    try:
        sock.sendall(
            f"GET {path} HTTP/1.1\r\nHost: {host}:{port}\r\n"
            "Accept: application/json\r\nConnection: close\r\n\r\n".encode()
        )
        chunks: list[bytes] = []
        t_first = 0.0
        reads = 0
        buf = b""
        status = 0
        headers: dict[str, str] = {}
        in_body = False
        while True:
            data = sock.recv(65536)
            if not data:
                break
            buf += data
            if not in_body:
                if b"\r\n\r\n" not in buf:
                    continue
                head, _, buf = buf.partition(b"\r\n\r\n")
                lines = head.split(b"\r\n")
                parts = lines[0].split()
                status = int(parts[1]) if len(parts) > 1 else 0
                for line in lines[1:]:
                    name, _, value = line.partition(b":")
                    headers[name.decode().strip().lower()] = value.decode().strip()
                in_body = True
                if not buf:
                    continue
            if not chunks:
                t_first = time.monotonic() - start
            chunks.append(buf)
            buf = b""
            reads += 1
            if max_reads is not None and reads >= max_reads:
                break
            if read_delay:
                time.sleep(read_delay)
        return chunks, t_first, time.monotonic() - start, status, headers
    finally:
        sock.close()


def decode_chunked(chunks: list[bytes]) -> bytes:
    raw = b"".join(chunks)
    out = bytearray()
    while raw:
        line, _, rest = raw.partition(b"\r\n")
        if not line:
            break
        try:
            size = int(line.split(b";")[0], 16)
        except ValueError:
            break
        if size == 0:
            break
        out += rest[:size]
        raw = rest[size + 2:]
    return bytes(out)


def collect_manager_stream(mgr, view) -> bytes:
    import asyncio

    async def run():
        out = []
        async for chunk in mgr.stream(view):
            out.append(chunk)
        return b"".join(out)

    return asyncio.run(run())


# ---------------------------------------------------------------- 主流程

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base-url", default=None,
                    help="外部已运行的服务；不传则本地起 in-process 服务")
    ap.add_argument("--history", type=int, default=3000)
    ap.add_argument("--port", type=int, default=8799)
    args = ap.parse_args()

    results: list[tuple[str, bool, str]] = []

    def check(name: str, cond: bool, detail: str = "") -> None:
        results.append((name, bool(cond), detail))
        print(f"[{'PASS' if cond else 'FAIL'}] {name}" + (f"  -- {detail}" if detail else ""))

    external = args.base_url is not None
    if external:
        base = args.base_url.rstrip("/")
        url = httpx.URL(base)
        host = url.host
        port = url.port or (443 if url.scheme == "https" else 80)
        expected_ids = None
        mgr = server = None
    else:
        # 允许直接 `python3 scripts/streamtest.py`：把仓库根加入模块搜索路径
        repo_root = str(Path(__file__).resolve().parent.parent)
        if repo_root not in sys.path:
            sys.path.insert(0, repo_root)
        from app.main import create_app

        mgr = build_manager(args.history)
        server = serve(create_app(lambda: mgr), args.port)
        base = f"http://127.0.0.1:{args.port}"
        host, port = "127.0.0.1", args.port
        expected_ids = [f"seed{i:05d}" for i in reversed(range(args.history))]

    fields = {
        "job_id", "slot", "state", "memory_mib", "hold_seconds", "pid",
        "exit_status", "exit_signal", "stop_requested", "oom_kill_before",
        "oom_kill_after", "oom_kill_delta", "verdict", "verdict_reason",
        "created_at", "finished_at", "output_tail",
    }

    try:
        with httpx.Client(timeout=20) as client:
            # 服务可能刚启动（compose 里 depends_on 不保证就绪）：等它就绪
            st = None
            deadline = time.monotonic() + 60
            while time.monotonic() < deadline:
                try:
                    st = client.get(f"{base}/status").json()
                    break
                except Exception:
                    time.sleep(1)
            if st is None:
                print(f"服务不可达: {base}")
                return 2
            print(f"== mode={st['mode']} accepting={st['accepting_jobs']} "
                  f"slots_free={st['slots']['free']} ==")

            # 外部服务可能还没有历史：补两轮短作业，让长列表/快照用例有数据
            if external and not client.get(f"{base}/jobs").json():
                slot_mib = st["slots"]["memory_max_mib"] or 64
                small = max(1, min(8, slot_mib // 8))
                created = []
                for _ in range(2):
                    r = client.post(f"{base}/jobs",
                                    json={"memory_mib": small, "hold_seconds": 1})
                    if r.status_code == 201:
                        created.append(r.json()["job_id"])
                    time.sleep(1.2)
                deadline = time.monotonic() + 30
                while time.monotonic() < deadline:
                    rows = client.get(f"{base}/jobs").json()
                    if len(rows) >= len(created) and all(
                            x["state"] == "finished" for x in rows[:len(created)]):
                        break
                    time.sleep(0.2)
                if created:
                    # 外部模式按“刚接单的在前”校验顺序
                    rows = client.get(f"{base}/jobs").json()
                    expected_ids = [x for x in reversed(created)
                                    if x in {r["job_id"] for r in rows}]

            # ---- 1) 长列表 / 逐条发送 ----
            chunks, t_first, elapsed, status, headers = raw_get(host, port)
            body = decode_chunked(chunks)
            rows = json.loads(body)
            check("GET /jobs 返回 200", status == 200, str(status))
            check("完整响应是 JSON 数组", isinstance(rows, list), f"{len(rows)} 条")
            if expected_ids is not None:
                check("按接单先后从新到旧",
                      [r["job_id"] for r in rows] == expected_ids,
                      f"first={rows[0]['job_id']} last={rows[-1]['job_id']} "
                      f"count={len(rows)}")
            if rows:
                check("每条记录保留原字段", set(rows[0]) == fields,
                      f"missing={sorted(fields - set(rows[0]))}")
            else:
                check("空历史时是空数组", body.strip() == b"[]", body[:40].decode())

            # 调小接收缓冲：首个网络读只拿到响应的一部分，说明没等整份拼完
            part_chunks, t_part, _, _, part_headers = raw_get(host, port, rcvbuf=4096)
            first_bytes = len(part_chunks[0]) if part_chunks else 0
            check("响应以 chunked 逐段发出",
                  part_headers.get("transfer-encoding") == "chunked",
                  str(part_headers.get("transfer-encoding")))
            check("首块只是响应的一部分（未等整份拼完）",
                  0 < first_bytes < len(body),
                  f"首块 {first_bytes}B / 整体 {len(body)}B，首块用时 {t_part:.4f}s")

            # ---- 2) 慢读时其他请求照常 ----
            slow = {"done": False, "elapsed": 0.0, "body": None}

            def slow_reader():
                # 每读一块睡 0.1s：整个查询被拉长，期间可验证其他请求不受阻
                ch, _, el, stc, _ = raw_get(host, port, read_delay=0.1)
                slow.update(done=True, elapsed=el,
                            body=decode_chunked(ch) if stc == 200 else None)

            thread = threading.Thread(target=slow_reader)
            thread.start()
            time.sleep(0.4)
            t0 = time.monotonic()
            r = client.post(f"{base}/jobs", json={"memory_mib": 4, "hold_seconds": 2})
            start_latency = time.monotonic() - t0
            check("慢读期间 POST /jobs 照常接单", r.status_code == 201,
                  f"{r.status_code} in {start_latency:.3f}s")
            if r.status_code == 201:
                job_id = r.json()["job_id"]
                t0 = time.monotonic()
                rs = client.post(f"{base}/jobs/{job_id}/stop")
                check("慢读期间强停照常受理", rs.status_code == 200,
                      f"{rs.status_code} in {time.monotonic() - t0:.3f}s")
                free0 = client.get(f"{base}/status").json()["slots"]["free"]
                deadline = time.monotonic() + 20
                free1 = free0
                while time.monotonic() < deadline and free1 <= free0:
                    free1 = client.get(f"{base}/status").json()["slots"]["free"]
                    time.sleep(0.1)
                check("慢读期间槽位照常交还", free1 > free0, f"free {free0} -> {free1}")
            thread.join(timeout=60)
            check("慢读查询最终正常完成且完整",
                  slow["done"] and slow["body"] is not None
                  and json.loads(slow["body"]) is not None,
                  f"耗时 {slow['elapsed']:.2f}s")
            if not external:
                check("慢读确实在读（查询被拉长）", slow["elapsed"] >= 0.3,
                      f"{slow['elapsed']:.2f}s")

            # ---- 3) 中途断开 ----
            raw_get(host, port, max_reads=2)          # 读两下就关连接
            time.sleep(0.5)
            after = client.get(f"{base}/status").json()
            check("断开后本次查询视图已释放",
                  after["active_list_streams"] == 0,
                  f"active={after['active_list_streams']}")
            r = client.get(f"{base}/jobs")
            check("断开后其他查询照常", r.status_code == 200 and bool(r.json()),
                  f"{r.status_code} {len(r.json())} 条")
            check("断开不占槽位", after["slots"]["free"] >= st["slots"]["free"],
                  f"free={after['slots']['free']}")

            # ---- 4) 受理时刻语义（需能直接操作 manager）----
            if not external:
                view = mgr.list_view()           # 受理查询
                rnew = client.post(f"{base}/jobs",
                                   json={"memory_mib": 4, "hold_seconds": 5})
                new_id = rnew.json()["job_id"] if rnew.status_code == 201 else None
                check("受理后仍可接单", rnew.status_code == 201, f"{rnew.status_code}")
                sent = json.loads(collect_manager_stream(mgr, view))
                check("受理后接单的作业不在本次名单",
                      new_id not in {x["job_id"] for x in sent},
                      f"later={new_id} count={len(sent)}")

                # 让该作业处于运行中，再受理一次；随后模拟“发送期间结束”
                deadline = time.monotonic() + 10
                while time.monotonic() < deadline and new_id not in mgr._running:
                    time.sleep(0.05)
                if new_id in mgr._running:
                    running = mgr._running[new_id]
                    running.output_tail.append("tail-at-accept")
                    view2 = mgr.list_view()
                    # 发送期间结束：直接改内存记录（模拟作业收尾）
                    running.state = "finished"
                    running.verdict = "oom_kill"
                    running.verdict_reason = "changed during send"
                    running.finished_at = time.time()
                    running.output_tail.clear()
                    running.output_tail.append("tail-after-finish")
                    mgr._running.pop(new_id, None)
                    sent2 = json.loads(collect_manager_stream(mgr, view2))
                    rec = next(x for x in sent2 if x["job_id"] == new_id)
                    check("发送期间结束仍报受理时状态", rec["state"] == "running",
                          rec["state"])
                    check("发送期间结束仍报受理时归因", rec["verdict"] is None,
                          str(rec["verdict"]))
                    check("发送期间结束仍报受理时日志尾部",
                          rec["output_tail"][-1] == "tail-at-accept",
                          str(rec["output_tail"][-2:]))
                else:
                    check("发送期间结束语义（作业未能进入运行态）", False, "skip")
    finally:
        if server is not None:
            server.should_exit = True

    failed = [n for n, ok, _ in results if not ok]
    print(f"\n== 逐条发送自测：{len(results) - len(failed)}/{len(results)} 项通过 ==")
    if failed:
        print("失败项: " + "; ".join(failed))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
