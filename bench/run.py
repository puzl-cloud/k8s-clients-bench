import sys
import asyncio
from pathlib import Path

if sys.platform.startswith("linux"):
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    print("Running on uvloop")

from .benchmark import BenchmarkResult
from .output import print_combined_results, plot_benchmarks_histogram
from ._kubesdk import KubesdkBenchmark
from ._kubernetes_asyncio import KubernetesAsyncioBenchmark
from ._kr8s_async import Kr8sAsyncBenchmark
from ._lightkube_async import LightkubeAsyncBenchmark
from ._official_client import OfficialClientBenchmark


async def run(output_dir: str | Path) -> None:
    benchmark_size = 5_000
    results: list[BenchmarkResult] = []
    kubesdk = KubesdkBenchmark(benchmark_size=benchmark_size)

    # Clean ns, first
    await kubesdk.init_client()
    await kubesdk.cleanup()

    # 3, 2, 1... bench!
    results += await kubesdk.run()

    k8s_asyncio = KubernetesAsyncioBenchmark(benchmark_size=benchmark_size)
    results += await k8s_asyncio.run()

    kr8s = Kr8sAsyncBenchmark(benchmark_size=benchmark_size)
    results += await kr8s.run()

    lightkube = LightkubeAsyncBenchmark(benchmark_size=benchmark_size)
    results += await lightkube.run()

    official = OfficialClientBenchmark(benchmark_size=benchmark_size)
    results += await official.run()

    await kubesdk.cleanup()
    _all = [kubesdk, k8s_asyncio, kr8s, lightkube, official]
    print_combined_results(_all)
    plot_benchmarks_histogram(_all, output_dir)
