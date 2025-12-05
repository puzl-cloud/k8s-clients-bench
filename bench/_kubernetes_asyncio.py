from __future__ import annotations

from dataclasses import dataclass
from typing import Any, AsyncIterable

from kubernetes_asyncio import client, config, watch
from kubernetes_asyncio.client import (
    V1ObjectMeta,
    V1EnvVar,
    V1Container,
    V1PodSpec,
    V1PodTemplateSpec,
    V1LabelSelector,
    V1Deployment,
    V1DeploymentSpec,
    V1Volume,
    V1EmptyDirVolumeSource,
    V1VolumeMount,
    V1Probe,
    V1ExecAction,
    V1ResourceRequirements,
)

from .benchmark import Benchmark


@dataclass
class KubernetesAsyncioBenchmark(Benchmark):
    client: str = "kubernetes_asyncio"

    api_client = None
    apps_client = None

    @staticmethod
    def _large_pod_template(name: str) -> V1PodTemplateSpec:
        common = [V1EnvVar(name=f"ENV_{i}", value=f"value_{i}") for i in range(50)]
        c1 = V1Container(
            name="c1",
            image="busybox:stable",
            command=["/bin/sh", "-c"],
            args=["sleep 3600"],
            env=common + [V1EnvVar(name="C1_ONLY", value="x")],
            volume_mounts=[V1VolumeMount(name="work", mount_path="/work")],
            resources=V1ResourceRequirements(
                limits={"cpu": "100m", "memory": "128Mi"},
                requests={"cpu": "50m", "memory": "64Mi"},
            ),
            liveness_probe=V1Probe(
                _exec=V1ExecAction(command=["/bin/true"]),
                initial_delay_seconds=5,
                period_seconds=30,
            ),
        )
        c2 = V1Container(
            name="c2",
            image="busybox:stable",
            command=["/bin/sh", "-c"],
            args=["sleep 3600"],
            env=common + [V1EnvVar(name="C2_ONLY", value="y")],
            volume_mounts=[V1VolumeMount(name="work", mount_path="/data")],
            resources=V1ResourceRequirements(
                limits={"cpu": "100m", "memory": "128Mi"},
                requests={"cpu": "50m", "memory": "64Mi"},
            ),
        )
        c3 = V1Container(
            name="c3",
            image="busybox:stable",
            command=["/bin/sh", "-c"],
            args=["sleep 3600"],
            env=[V1EnvVar(name="IMPORTANT", value="bench_value")] + common,
            volume_mounts=[V1VolumeMount(name="work", mount_path="/cache")],
        )
        vols = [V1Volume(name="work", empty_dir=V1EmptyDirVolumeSource())]
        return V1PodTemplateSpec(
            metadata=V1ObjectMeta(labels={"app": name}),
            spec=V1PodSpec(containers=[c1, c2, c3], volumes=vols),
        )

    async def init_client(self):
        try:
            await config.load_kube_config()
        except Exception:
            config.load_incluster_config()
        self.api_client = client.ApiClient()
        self.apps_client = client.AppsV1Api(self.api_client)

    async def create_one(self, name: str):
        body = V1Deployment(
            metadata=V1ObjectMeta(
                name=name,
                namespace=self.namespace,
                labels=self.build_bench_labels(name),
            ),
            spec=V1DeploymentSpec(
                replicas=0,
                selector=V1LabelSelector(match_labels={"app": name}),
                template=self._large_pod_template(name),
            ),
        )
        return await self.apps_client.create_namespaced_deployment(namespace=self.namespace, body=body)

    async def get_one(self, name: str):
        dep = await self.apps_client.read_namespaced_deployment(name=name, namespace=self.namespace)
        self.check_bench_labels(name, dep.metadata.labels)
        return dep

    async def delete_one(self, name: str):
        await self.apps_client.delete_namespaced_deployment(name=name, namespace=self.namespace)

    async def watch_all(self) -> AsyncIterable[Any]:
        watcher = watch.Watch()
        async for event in watcher.stream(self.apps_client.list_namespaced_deployment, namespace=self.namespace):
            deploy = event["object"]
            yield deploy
