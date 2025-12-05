import asyncio
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from functools import cached_property
from typing import Any, AsyncIterable

import pandas as pd

CONCURRENCY = 500
_semaphore = asyncio.Semaphore(CONCURRENCY)


async def run_with_guard(task):
    async with _semaphore:
        await task


@dataclass
class BenchmarkResult:
    bench_name: str
    requests: int
    seconds: float


@dataclass
class Benchmark(ABC):
    client: str

    benchmark_size: int = 5_000
    namespace: str = "default"
    resource_name_prefix: str = "client-bench-"
    results: list[BenchmarkResult] = field(default_factory=list)

    def build_bench_labels(self, name: str) -> dict[str, str]: return {f"app/{name}": f"{self.namespace}-{name}"}

    def check_bench_labels(self, name: str, labels: dict):
        bench_labels = self.build_bench_labels(name)
        for k, v in bench_labels.items():
            assert labels.get(k) == v

    @cached_property
    def all_objects_names(self) -> list[str]:
        return [f"{self.resource_name_prefix}{i:06d}" for i in range(self.benchmark_size)]

    async def run(self) -> list[BenchmarkResult]:
        print(f"Running {self.client} client benchmark for {self.benchmark_size} objects...")
        await self.init_client()

        bench = "POST"
        print(f"Starting {bench} benchmark...")
        t0 = time.perf_counter()
        await self.create_batch()
        t1 = time.perf_counter()
        self.results.append(BenchmarkResult(bench, self.benchmark_size, t1 - t0))

        bench = "GET"
        print(f"Starting {bench} benchmark...")
        await self.get_batch()
        t2 = time.perf_counter()
        self.results.append(BenchmarkResult(bench, self.benchmark_size, t2 - t1))

        bench = "Watch"
        print(f"Starting {bench} benchmark...")
        t2 = time.perf_counter()
        await self._bench_watch()
        t3 = time.perf_counter()
        self.results.append(BenchmarkResult(bench, self.benchmark_size, t3 - t2))

        bench = "DELETE"
        print(f"Starting {bench} benchmark...")
        await self.delete_batch()
        t4 = time.perf_counter()
        self.results.append(BenchmarkResult(bench, self.benchmark_size, t4 - t3))

        self.print_results()
        return self.results

    def print_results(self):
        rows = []
        for res in self.results:
            rows.append({
                "Benchmark": res.bench_name,
                "Objects": res.requests,
                "Seconds": res.seconds,
                "Obj/s": res.requests / res.seconds if res.seconds else 0.0,
            })
        df = pd.DataFrame(rows, columns=["Benchmark", "Objects", "Seconds", "Obj/s"])
        print("-" * 40)
        print(df.to_string(
            index=False,
            formatters={
                "Seconds": lambda v: f"{v:.2f}",
                "Obj/s": lambda v: f"{v:.1f}"
            }
        ))
        print("-" * 40)

    @abstractmethod
    async def init_client(self): raise NotImplementedError()
    @abstractmethod
    async def get_one(self, name: str): raise NotImplementedError()
    @abstractmethod
    async def create_one(self, name: str): raise NotImplementedError()
    @abstractmethod
    async def delete_one(self, name: str): raise NotImplementedError()
    @abstractmethod
    async def watch_all(self) -> AsyncIterable[Any]: yield NotImplementedError()

    async def delete_batch(self):
        await asyncio.gather(*[run_with_guard(self.delete_one(name)) for name in self.all_objects_names])

    async def get_batch(self):
        await asyncio.gather(*[run_with_guard(self.get_one(name)) for name in self.all_objects_names])
        
    async def create_batch(self) -> list[Any]:
        return await asyncio.gather(*[run_with_guard(self.create_one(name)) for name in self.all_objects_names])

    async def _bench_watch(self):
        count = 0
        async for obj in self.watch_all():
            self.check_bench_labels(obj.metadata.name, obj.metadata.labels)
            count += 1
            if count == self.benchmark_size:
                return
