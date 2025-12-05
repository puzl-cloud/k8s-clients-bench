import time
import asyncio
import os
from dataclasses import dataclass

from kube_models.api_v1.io.k8s.api.core.v1 import *
from kube_models.apis_apps_v1.io.k8s.api.apps.v1 import *
from kube_models.api_v1.io.k8s.apimachinery.pkg.apis.meta.v1 import *
from kube_models.api_v1.io.k8s.api.core.v1 import Container

from kubesdk.login import login
from kubesdk.client import *

from .benchmark import Benchmark


@dataclass
class KubesdkBenchmark(Benchmark):
    client: str = "kubesdk"

    @staticmethod
    def _large_pod_template(name: str) -> PodTemplateSpec:
        common_env = [EnvVar(name=f"ENV_{i}", value=f"value_{i}") for i in range(50)]

        c1 = Container(
            name="c1",
            image="busybox:stable",
            command=["/bin/sh", "-c"],
            args=["sleep 3600"],
            env=common_env + [EnvVar(name="C1_ONLY", value="x")],
            volumeMounts=[VolumeMount(name="work", mountPath="/work")],
            resources=ResourceRequirements(limits={"cpu": "100m", "memory": "128Mi"}, requests={"cpu": "50m", "memory": "64Mi"}),
            livenessProbe=Probe(exec=ExecAction(command=["/bin/true"]), initialDelaySeconds=5, periodSeconds=30),
        )
        c2 = Container(
            name="c2",
            image="busybox:stable",
            command=["/bin/sh", "-c"],
            args=["sleep 3600"],
            env=common_env + [EnvVar(name="C2_ONLY", value="y")],
            volumeMounts=[VolumeMount(name="work", mountPath="/data")],
            resources=ResourceRequirements(limits={"cpu": "100m", "memory": "128Mi"}, requests={"cpu": "50m", "memory": "64Mi"}),
        )
        c3 = Container(
            name="c3",
            image="busybox:stable",
            command=["/bin/sh", "-c"],
            args=["sleep 3600"],
            env=[EnvVar(name="IMPORTANT", value="bench_value")] + common_env,
            volumeMounts=[VolumeMount(name="work", mountPath="/cache")],
        )

        vols = [Volume(name="work", emptyDir=EmptyDirVolumeSource())]

        pod_spec = PodSpec(containers=[c1, c2, c3], volumes=vols)
        labels = {"app": name}
        return PodTemplateSpec(metadata=ObjectMeta(labels=labels), spec=pod_spec)

    async def create_one(self, name: str):
        return await create_k8s_resource(
            Deployment(
                metadata=ObjectMeta(name=name, namespace=self.namespace, labels=self.build_bench_labels(name)),
                spec=DeploymentSpec(
                    replicas=0,
                    selector=LabelSelector(matchLabels={"app": name}),
                    template=self._large_pod_template(name))))

    async def get_one(self, name: str):
        deploy = await get_k8s_resource(Deployment, name, self.namespace)
        self.check_bench_labels(name, deploy.metadata.labels)

    async def watch_all(self) -> AsyncIterable[Any]:
        async for event in watch_k8s_resources(Deployment, namespace=self.namespace):
            deploy = event.object
            yield deploy

    async def init_client(self):
        await login()

    async def delete_one(self, name: str): await delete_k8s_resource(Deployment, name, self.namespace)

    # We use this to have clean namespace in the beginning
    async def cleanup(self): await delete_k8s_resource(Deployment, namespace=self.namespace)
