# Kubernetes Client Benchmarks (Python)

This repo benchmarks five Python Kubernetes clients using 5000 `Deployment` resources. Each client implements the same async methods: `create_one`, `get_one`, `watch_all`, `delete_one`. The suite focuses on end-to-end latency and overhead for create/get/watch/delete paths rather than application logic.

The resource under test is a `Deployment` with a deliberately large `Pod` template designed to stress model construction and serialization across clients.

## Results

Benchmark results in this repo were collected against **[kind](https://github.com/kubernetes-sigs/kind) (Kubernetes in Docker)**, which provides a fast, consistent local environment for comparing client overhead under the same cluster conditions. 

![python_kubernetes_clients_benchmark.png](./python_kubernetes_clients_benchmark.png)
