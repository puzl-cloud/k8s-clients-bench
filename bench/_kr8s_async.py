from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Any, AsyncIterable

from kr8s.asyncio import api
from kr8s.asyncio.objects import Deployment

from .benchmark import Benchmark


@dataclass
class Kr8sAsyncBenchmark(Benchmark):
    client: str = "kr8s (async)"

    api = None

    @staticmethod
    def _large_pod_template(name: str) -> dict[str, Any]:
        common_env = [{"name": f"ENV_{i}", "value": f"value_{i}"} for i in range(50)]

        c1 = {
            "name": "c1",
            "image": "busybox:stable",
            "command": ["/bin/sh", "-c"],
            "args": ["sleep 3600"],
            "env": common_env + [{"name": "C1_ONLY", "value": "x"}],
            "volumeMounts": [{"name": "work", "mountPath": "/work"}],
            "resources": {
                "limits": {"cpu": "100m", "memory": "128Mi"},
                "requests": {"cpu": "50m", "memory": "64Mi"},
            },
            "livenessProbe": {
                "exec": {"command": ["/bin/true"]},
                "initialDelaySeconds": 5,
                "periodSeconds": 30,
            },
        }
        c2 = {
            "name": "c2",
            "image": "busybox:stable",
            "command": ["/bin/sh", "-c"],
            "args": ["sleep 3600"],
            "env": common_env + [{"name": "C2_ONLY", "value": "y"}],
            "volumeMounts": [{"name": "work", "mountPath": "/data"}],
            "resources": {
                "limits": {"cpu": "100m", "memory": "128Mi"},
                "requests": {"cpu": "50m", "memory": "64Mi"},
            },
        }
        c3 = {
            "name": "c3",
            "image": "busybox:stable",
            "command": ["/bin/sh", "-c"],
            "args": ["sleep 3600"],
            "env": [{"name": "IMPORTANT", "value": "bench_value"}] + common_env,
            "volumeMounts": [{"name": "work", "mountPath": "/cache"}],
        }

        return {
            "metadata": {"labels": {"app": name}},
            "spec": {
                "containers": [c1, c2, c3],
                "volumes": [{"name": "work", "emptyDir": {}}],
            },
        }

    async def init_client(self):
        kubeconfig = os.getenv("KUBECONFIG")
        kwargs: dict[str, Any] = {}
        if kubeconfig:
            kwargs["kubeconfig"] = kubeconfig
        self.api = await api(**kwargs)

        # Don't misbehave
        for name in ("lightkube", "lightkube.core", "httpx", "urllib3", "websockets"):
            logging.getLogger(name).setLevel(logging.WARNING)

    async def create_one(self, name: str):
        body = {
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {
                "name": name,
                "namespace": self.namespace,
                "labels": self.build_bench_labels(name),
            },
            "spec": {
                "replicas": 0,
                "selector": {"matchLabels": {"app": name}},
                "template": self._large_pod_template(name),
            },
        }
        dep = await Deployment(body, namespace=self.namespace)
        await dep.create()
        return dep

    async def get_one(self, name: str):
        dep = await Deployment.get(name, namespace=self.namespace)
        labels = dict(dep.labels)
        self.check_bench_labels(name, labels)
        return dep

    async def delete_one(self, name: str):
        dep = await Deployment.get(name, namespace=self.namespace)
        await dep.delete()

    async def watch_all(self) -> AsyncIterable[Any]:
        async for dep in Deployment.list(namespace=self.namespace):
            yield dep
