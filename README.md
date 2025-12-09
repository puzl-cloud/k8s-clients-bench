# Kubernetes Client Benchmarks (Python)

This repo benchmarks five Python Kubernetes clients using 5000 `Deployment` resources. Each client implements the same async methods: `create_one`, `get_one`, `watch_all`, `delete_one`. The suite focuses on end-to-end latency and overhead for create/get/watch/delete paths rather than application logic.

The resource under test is a `Deployment` with a deliberately large `Pod` template designed to stress model construction and serialization across clients.

## Results

Benchmark results in this repo were collected against **[kind](https://github.com/kubernetes-sigs/kind) (Kubernetes in Docker)**, which provides a fast, consistent local environment for comparing client overhead under the same cluster conditions. 

![python_kubernetes_clients_benchmark.png](./python_kubernetes_clients_benchmark.png)

Combined benchmark results (objects per second).

| Client             | Objects |  POST |   GET |  Watch | DELETE |
| ------------------ | ------: | ----: | ----: | -----: | -----: |
| kubesdk            |    5000 | 297.7 | 883.0 | 4222.3 | 2912.0 |
| kubernetes_asyncio |    5000 | 425.2 | 705.9 |  862.3 | 1586.1 |
| kr8s (async)       |    5000 |  45.3 |  53.0 |   74.4 |   47.8 |
| lightkube (async)  |    5000 |  44.2 |  55.9 | 3406.4 |   57.8 |
| official           |    5000 |  38.1 |  52.4 |  507.4 | 1382.6 |
