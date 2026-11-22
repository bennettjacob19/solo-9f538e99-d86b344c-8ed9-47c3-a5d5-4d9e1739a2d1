"""作业调度：固定数量（默认 2）的并行槽位。

关键约束：槽位 = 一轮实验环境（独立 cgroup）。只有上一轮给出归因结论
（verdict 落定）后，槽位才交还、供下一轮使用。
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
import time
import uuid
import weakref
from collections import deque
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from .cgroups import BackendStatus, SlotHandle
from .verdict import decide_verdict

SAMPLE_JOB = Path(__file__).resolve().parent / "sample_job.py"
OUTPUT_TAIL_LINES = 50
OOM_SETTLE_SECONDS = 0.1  # 进程回收后稍等再读 oom_kill，确保内核已落账


def _iso(ts: float | None) -> str | None:
    if ts is None:
        return None
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


@dataclass
class Job:
    id: str
    slot: int
    memory_mib: int
    hold_seconds: float
    state: str = "running"  # running | finished
    pid: int | None = None
    returncode: int | None = None       # subprocess 约定：负数 = -信号
    exit_signal: int | None = None
    stop_requested: bool = False
    stop_requested_at: float | None = None
    oom_kill_before: int | None = None
    oom_kill_after: int | None = None
    oom_kill_delta: int | None = None
    verdict: str | None = None
    verdict_reason: str | None = None
    created_at: float = field(default_factory=time.time)
    finished_at: float | None = None
    output_tail: deque = field(default_factory=lambda: deque(maxlen=OUTPUT_TAIL_LINES))
    # 接单链：指向更早接单的作业。GET /jobs 从最新一条沿链向旧走，
    # 首条记录无需先扫历史；新作业只改自身与链头，不扰动正在进行的遍历。
    prev_id: str | None = field(default=None, repr=False)

    def to_dict(self) -> dict:
        # 快照式：整份字典在这里一次成型，调用方拿到的都是普通值，之后作业
        # 状态再变也不会影响已发出的内容（列表流式响应依赖这一点）。
        return {
            "job_id": self.id,
            "slot": self.slot,
            "state": self.state,
            "memory_mib": self.memory_mib,
            "hold_seconds": self.hold_seconds,
            "pid": self.pid,
            "exit_status": self.returncode,
            "exit_signal": self.exit_signal,
            "stop_requested": self.stop_requested,
            "oom_kill_before": self.oom_kill_before,
            "oom_kill_after": self.oom_kill_after,
            "oom_kill_delta": self.oom_kill_delta,
            "verdict": self.verdict,
            "verdict_reason": self.verdict_reason,
            "created_at": _iso(self.created_at),
            "finished_at": _iso(self.finished_at),
            "output_tail": list(self.output_tail),
        }


class NotAccepting(Exception):
    """内存上限未生效或 cgroup 不可用，拒绝接作业。"""


class NoSlotAvailable(Exception):
    """两轮槽位都在占用中。"""


@dataclass(eq=False)
class JobListView:
    """一次 GET /jobs 查询的只读视图。

    - head：受理这次查询时的接单链头。之后新接单的作业立在更前面，
      不在这条链上，留到下次查询。
    - saved：受理时仍在跑（之后可能还会变）的作业记录快照。已结束的
      作业不再变化，无需拷贝；仍在跑的至多等于槽位数，故视图大小与
      历史条数无关。查询期间才结束的作业，返回的仍是受理时的状态、
      归因与日志尾部。
    """

    head: str | None
    saved: dict[str, dict] = field(default_factory=dict)

    def release(self) -> None:
        """查询结束（含客户端中途断开）时归还视图占用的快照。"""
        self.saved.clear()


class JobManager:
    def __init__(self, backend):
        self.backend = backend
        self.jobs: dict[str, Job] = {}
        self._handles: dict[int, SlotHandle] = {h.index: h for h in backend.handles}
        self._free: asyncio.Queue[int] = asyncio.Queue()
        for h in backend.handles:
            self._free.put_nowait(h.index)
        self._procs: dict[str, asyncio.subprocess.Process] = {}
        self._tasks: set[asyncio.Task] = set()
        # 接单链头（最新接单的作业号）。GET /jobs 从链头向旧逐条走：
        # 发出首条不必先扫历史，遍历期间新接单也只是另立新链头，不改旧链。
        self._newest_id: str | None = None
        # 仍在跑的作业按作业号索引：受理查询时只需为这些作业取快照。
        self._running: dict[str, Job] = {}
        # 正在发送中的查询视图（弱引用，只用于观测）：正常发完、客户端断开
        # 或出错时由 stream() 的 finally 移除、快照释放；弱引用保证即使
        # 生成器未能及时收尾，也不会把视图（及其快照）长期留住。
        self._views: weakref.WeakSet[JobListView] = weakref.WeakSet()

    # ---------- 查询 ----------

    @property
    def status(self) -> BackendStatus:
        return self.backend.status

    def slots_snapshot(self) -> dict:
        busy = sorted(j.slot for j in self.jobs.values() if j.state == "running")
        return {
            "count": len(self._handles),
            "free": len(self._handles) - len(busy),
            "busy_slots": busy,
            "memory_max_bytes": self.status.slot_memory_max_bytes,
            "memory_max_mib": (
                self.status.slot_memory_max_bytes // (1024 * 1024)
                if self.status.slot_memory_max_bytes
                else None
            ),
        }

    def get(self, job_id: str) -> Job | None:
        return self.jobs.get(job_id)

    def list(self) -> list[Job]:
        """全部作业，接单先后从新到旧（供非流式调用，如自测）。"""
        out: list[Job] = []
        jid = self._newest_id
        while jid is not None:
            job = self.jobs.get(jid)
            if job is None:  # 已被清理（当前不清理，留作链路自愈）
                break
            out.append(job)
            jid = job.prev_id
        return out

    def list_view(self) -> JobListView:
        """受理一次 GET /jobs：登记名单与各作业的受理时证据。

        - 名单：受理时的接单链头；之后新接单的作业立在更前面，不在此链上。
        - 证据：仍在跑的作业各留一份记录快照（状态、归因、日志尾部），
          所以查询期间才结束的作业，仍按受理时的样子返回。
        已结束的作业不再变化，不必拷贝；仍在跑的至多等于槽位数。
        """
        view = JobListView(
            head=self._newest_id,
            saved={jid: job.to_dict() for jid, job in self._running.items()},
        )
        self._views.add(view)
        return view

    @property
    def active_list_views(self) -> int:
        """仍在发送中的名单查询数（/status 观测用）。"""
        return len(self._views)

    def _record(self, view: JobListView, job: Job) -> dict:
        snap = view.saved.get(job.id)
        if snap is not None:
            return snap  # 受理时仍在跑：返回查询开始时的状态/归因/日志尾部
        return job.to_dict()

    async def stream(self, view: JobListView) -> AsyncIterator[bytes]:
        """按接单先后从新到旧，逐条产出作业记录的 JSON 数组。

        - 响应契约不变：整份响应仍是原字段的 JSON 数组。
        - 名单与证据以受理这次查询时为准：只沿受理时的接单链向旧走，
          查询期间新接单的作业留到下次查；名单内仍在跑的作业在受理时
          已取快照，发送期间才结束也仍返回受理时的状态、归因与日志尾部。
        - 发出首条不必先扫历史：从链头取一条、编码、发送，再走 prev_id；
          遍历位置只有一个作业号，编码缓冲只放当前这一条。
        - 额外内存 = 遍历位置 + 单条记录编码缓冲 + 受理时对运行中作业的
          快照（≤ 槽位数），不随历史条数增长。
        - 客户端断开时 Starlette 取消/关闭本生成器，finally 释放本次查询
          的视图（视图里的快照随之丢弃）；不占槽位，不影响其他请求。
        """
        try:
            yield b"["
            sep = b""
            jid = view.head
            while jid is not None:
                job = self.jobs.get(jid)
                if job is None:  # 记录不再存在（当前不清理）：到此为止
                    break
                prev_id = job.prev_id  # 先记下一步，再取快照、编码、发送
                try:
                    # 与 FastAPI 默认 JSONResponse 相同的编码参数（紧凑无空格、
                    # 中文不转义），保证逐条拼出的数组与整份序列化等价。
                    chunk = json.dumps(
                        self._record(view, job), ensure_ascii=False,
                        allow_nan=False, separators=(",", ":"),
                    ).encode("utf-8")
                except (TypeError, ValueError):  # 单条坏记录不拖垮整份响应
                    jid = prev_id
                    continue
                yield sep + chunk
                sep = b","
                jid = prev_id
            yield b"]"
        finally:
            # 正常发完、客户端中途断开（生成器被关闭/取消）或其他异常都会
            # 走到这里：归还本次查询占用的视图，其快照立即释放。
            self._views.discard(view)
            view.release()

    # ---------- 起作业 ----------

    async def start(self, memory_mib: int, hold_seconds: float) -> Job:
        if not self.backend.status.accepting:
            raise NotAccepting
        try:
            slot = self._free.get_nowait()
        except asyncio.QueueEmpty:
            raise NoSlotAvailable
        job = Job(
            id=uuid.uuid4().hex[:12],
            slot=slot,
            memory_mib=memory_mib,
            hold_seconds=hold_seconds,
            prev_id=self._newest_id,  # 接到接单链头之前：新的在前
        )
        self.jobs[job.id] = job
        self._running[job.id] = job
        self._newest_id = job.id
        task = asyncio.create_task(self._run(job))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return job

    async def _run(self, job: Job) -> None:
        handle = self._handles[job.slot]
        try:
            inner = [
                sys.executable, "-I", "-u", str(SAMPLE_JOB),
                "--mib", str(job.memory_mib),
                "--hold", str(job.hold_seconds),
            ]
            argv = self.backend.child_argv(handle, inner)
            env = {**os.environ, **self.backend.child_env(handle)}
            job.oom_kill_before = self.backend.read_oom_kill(handle)
            try:
                proc = await asyncio.create_subprocess_exec(
                    *argv,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.STDOUT,
                    env=env,
                )
            except OSError as exc:
                job.output_tail.append(f"spawn failed: {exc}")
                return
            self._procs[job.id] = proc
            job.pid = proc.pid
            # 强停请求可能先于 spawn 到达
            if job.stop_requested and proc.returncode is None:
                try:
                    proc.kill()
                except ProcessLookupError:
                    pass

            async def drain() -> None:
                assert proc.stdout is not None
                async for line in proc.stdout:
                    job.output_tail.append(line.decode(errors="replace").rstrip())

            reader = asyncio.create_task(drain())
            rc = await proc.wait()
            await reader
            job.returncode = rc
            job.exit_signal = -rc if rc is not None and rc < 0 else None
            await asyncio.sleep(OOM_SETTLE_SECONDS)
            job.oom_kill_after = self.backend.read_oom_kill(handle)
        except Exception as exc:  # 兜底：任何异常都不能卡住槽位
            job.output_tail.append(f"runner error: {exc!r}")
        finally:
            self._finalize(job, handle)

    def _finalize(self, job: Job, handle: SlotHandle) -> None:
        if job.oom_kill_before is not None and job.oom_kill_after is not None:
            job.oom_kill_delta = job.oom_kill_after - job.oom_kill_before
        job.verdict, job.verdict_reason = decide_verdict(
            returncode=job.returncode,
            oom_kill_before=job.oom_kill_before,
            oom_kill_after=job.oom_kill_after,
            stop_requested=job.stop_requested,
        )
        job.state = "finished"
        job.finished_at = time.time()
        self._procs.pop(job.id, None)
        self._running.pop(job.id, None)
        # 归因结论已出 —— 本轮实验环境（槽位）此刻才交还，可供下一轮使用
        self._free.put_nowait(job.slot)

    # ---------- 强停 ----------

    async def stop(self, job_id: str) -> Job | None:
        job = self.jobs.get(job_id)
        if job is None or job.state != "running":
            return job
        job.stop_requested = True
        job.stop_requested_at = time.time()
        proc = self._procs.get(job_id)
        if proc is not None and proc.returncode is None:
            try:
                proc.kill()  # SIGKILL
            except ProcessLookupError:
                pass
        return job

    async def shutdown(self) -> None:
        for view in list(self._views):
            self._views.discard(view)
            view.release()
        for job in list(self.jobs.values()):
            if job.state == "running":
                await self.stop(job.id)
        if self._tasks:
            await asyncio.wait(self._tasks, timeout=5)
