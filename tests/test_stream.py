"""GET /jobs 逐条发送的行为单测（纯 stdlib + 项目依赖）。

覆盖：
  * 接单顺序（新的在前），且不依赖 created_at 排序
  * 完整响应仍是原字段的 JSON 数组
  * 名单以受理时刻为准：之后接单的作业留到下次
  * 证据以受理时刻为准：发送期间才结束的作业仍报受理时的状态/归因/日志尾部
  * 首条记录无需先扫完历史（惰性）
  * 额外内存不随历史条数增长
  * 客户端断开（关闭生成器）后视图与快照立即释放
"""
import json
import tracemalloc
import unittest

from app.jobs import Job, JobManager


class FakeBackend:
    mode = "fake"

    def __init__(self, slots: int = 2):
        from app.cgroups import BackendStatus, SlotHandle
        from pathlib import Path

        self.status = BackendStatus(mode=self.mode, accepting=True)
        self.handles = [
            SlotHandle(i, Path(f"/tmp/fake-slot{i}"), Path(f"/tmp/fake-slot{i}/memory.events"), 1)
            for i in range(slots)
        ]

    def read_oom_kill(self, handle):
        return 0

    def child_argv(self, handle, inner):
        return list(inner)

    def child_env(self, handle):
        return {}

    def cleanup(self):
        pass


def seed(mgr: JobManager, count: int, *, created_at=None, tail_lines: int = 0,
         running_ids=()) -> list[Job]:
    """按接单顺序造记录（编号继续递增），直接放进管理器。"""
    start = getattr(mgr, "_test_seq", 0)
    mgr._test_seq = start + count
    made = []
    for i in range(start, start + count):
        job = Job(
            id=f"job{i:05d}",
            slot=i % max(1, len(mgr._handles)),
            memory_mib=8,
            hold_seconds=1,
            prev_id=mgr._newest_id,
        )
        if created_at is not None:
            job.created_at = created_at(i - start)
        if (i - start) in running_ids:
            job.state = "running"
        else:
            job.state = "finished"
            job.verdict = "normal_exit"
            job.verdict_reason = "进程自行退出，退出码 0"
            job.finished_at = job.created_at + 1
        for n in range(tail_lines):
            job.output_tail.append(f"[sample-job] line {n} of {job.id}")
        mgr.jobs[job.id] = job
        if job.state == "running":
            mgr._running[job.id] = job
        mgr._newest_id = job.id
        made.append(job)
    return made


async def collect(gen) -> bytes:
    out = []
    async for chunk in gen:
        out.append(chunk)
    return b"".join(out)


class OrderAndContractTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.mgr = JobManager(FakeBackend())

    async def test_newest_first_and_original_fields(self):
        seed(self.mgr, 5)
        view = self.mgr.list_view()
        body = await collect(self.mgr.stream(view))
        rows = json.loads(body)
        self.assertEqual(
            [r["job_id"] for r in rows],
            ["job00004", "job00003", "job00002", "job00001", "job00000"],
        )
        # 原字段一个不少，且没有流式实现引入的额外字段
        self.assertEqual(set(rows[0]), set(self.mgr.jobs["job00004"].to_dict()))
        self.assertIsInstance(rows, list)
        self.assertEqual(body, body.strip())

    async def test_acceptance_order_not_created_at(self):
        # created_at 故意与接单顺序相反：仍须按接单先后（新的在前）
        seed(self.mgr, 4, created_at=lambda i: 1000 - i)
        rows = json.loads(await collect(self.mgr.stream(self.mgr.list_view())))
        self.assertEqual(
            [r["job_id"] for r in rows],
            ["job00003", "job00002", "job00001", "job00000"],
        )

    async def test_empty_history_is_empty_array(self):
        body = await collect(self.mgr.stream(self.mgr.list_view()))
        self.assertEqual(json.loads(body), [])

    async def test_large_output_tail_preserved(self):
        seed(self.mgr, 1, tail_lines=50)
        rows = json.loads(await collect(self.mgr.stream(self.mgr.list_view())))
        self.assertEqual(len(rows[0]["output_tail"]), 50)


class AcceptTimeSemanticsTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.mgr = JobManager(FakeBackend())

    async def test_job_accepted_after_query_excluded(self):
        seed(self.mgr, 3)
        view = self.mgr.list_view()          # 受理查询
        seed(self.mgr, 2)                    # 受理之后又接单
        rows = json.loads(await collect(self.mgr.stream(view)))
        ids = [r["job_id"] for r in rows]
        self.assertEqual(ids, ["job00002", "job00001", "job00000"])
        self.assertNotIn("job00003", ids)
        self.assertNotIn("job00004", ids)

        # 下一次查询就能看到后来接单的作业
        rows2 = json.loads(await collect(self.mgr.stream(self.mgr.list_view())))
        self.assertEqual([r["job_id"] for r in rows2][:2], ["job00004", "job00003"])

    async def test_job_finishing_during_send_reports_accept_time_evidence(self):
        seed(self.mgr, 2, running_ids=(1,))
        running = self.mgr.jobs["job00001"]
        running.output_tail.append("[sample-job] target reached, holding")

        view = self.mgr.list_view()          # 受理：此刻仍在跑

        # 发送期间作业结束：状态、归因、日志尾部都变了
        running.state = "finished"
        running.verdict = "oom_kill"
        running.verdict_reason = "进程被 SIGKILL，本轮 cgroup oom_kill +1"
        running.oom_kill_delta = 1
        running.finished_at = running.created_at + 2
        running.output_tail.clear()
        running.output_tail.append("[sample-job] SIMULATED-OOM")
        self.mgr._running.pop(running.id, None)

        rows = json.loads(await collect(self.mgr.stream(view)))
        sent = next(r for r in rows if r["job_id"] == "job00001")
        self.assertEqual(sent["state"], "running")
        self.assertIsNone(sent["verdict"])
        self.assertIsNone(sent["finished_at"])
        self.assertEqual(
            sent["output_tail"], ["[sample-job] target reached, holding"]
        )

    async def test_job_finishing_avoids_partial_record(self):
        # 快照是整份拍下的：一条记录不可能混入半新半旧的字段
        seed(self.mgr, 1, running_ids=(0,))
        job = self.mgr.jobs["job00000"]
        job.pid = 4242
        view = self.mgr.list_view()
        job.state = "finished"
        job.verdict = "normal_exit"
        job.pid = None
        rows = json.loads(await collect(self.mgr.stream(view)))
        self.assertEqual(rows[0]["state"], "running")
        self.assertEqual(rows[0]["pid"], 4242)


class LazinessAndMemoryTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.mgr = JobManager(FakeBackend())

    async def test_first_record_without_scanning_history(self):
        seed(self.mgr, 5000)
        calls = {"n": 0}
        original = Job.to_dict

        def counting(self):
            calls["n"] += 1
            return original(self)

        Job.to_dict = counting
        try:
            gen = self.mgr.stream(self.mgr.list_view())
            self.assertEqual(await gen.__anext__(), b"[")
            first = await gen.__anext__()      # 首条记录
            self.assertIn(b"job04999", first)
            self.assertLessEqual(calls["n"], 2)  # 没有把 5000 条都先转一遍
            await gen.aclose()
        finally:
            Job.to_dict = original

    async def test_extra_memory_does_not_grow_with_history(self):
        async def peak_for(n: int):
            mgr = JobManager(FakeBackend())
            seed(mgr, n, tail_lines=2)
            view = mgr.list_view()
            tracemalloc.start()
            try:
                # 只消费、不累积：峰值反映的是查询本身的占用，不是测试端缓冲
                async for _chunk in mgr.stream(view):
                    pass
                _, peak = tracemalloc.get_traced_memory()
            finally:
                tracemalloc.stop()
                view.release()
            return peak

        peak_small = await peak_for(200)
        peak_large = await peak_for(20000)
        # 历史扩大 100 倍，本次查询的额外峰值内存不随之线性增长
        self.assertLess(peak_large, 2 * 1024 * 1024, f"peak={peak_large}")
        self.assertLess(peak_large, peak_small + 1024 * 1024,
                        f"small={peak_small} large={peak_large}")


class ReleaseTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.mgr = JobManager(FakeBackend())

    async def test_aclose_releases_view_and_snapshots(self):
        seed(self.mgr, 3, running_ids=(2,))
        view = self.mgr.list_view()
        self.assertTrue(view.saved)
        self.assertEqual(self.mgr.active_list_views, 1)

        gen = self.mgr.stream(view)
        await gen.__anext__()       # "["
        await gen.__anext__()       # 首条
        await gen.aclose()          # 客户端中途断开

        self.assertEqual(view.saved, {})
        self.assertEqual(self.mgr.active_list_views, 0)

    async def test_full_consumption_releases_view(self):
        seed(self.mgr, 3, running_ids=(0,))
        view = self.mgr.list_view()
        await collect(self.mgr.stream(view))
        self.assertEqual(view.saved, {})
        self.assertEqual(self.mgr.active_list_views, 0)


if __name__ == "__main__":
    unittest.main()
