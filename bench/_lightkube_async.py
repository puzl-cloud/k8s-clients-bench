from __future__ import annotations

import os
import tempfile
import logging
from pathlib import Path
from dataclasses import dataclass
from typing import Any, AsyncIterable, Optional

import yaml
from lightkube import AsyncClient, ApiError
from lightkube.resources.apps_v1 import Deployment
from lightkube.models.meta_v1 import ObjectMeta, LabelSelector
from lightkube.models.apps_v1 import DeploymentSpec
from lightkube.models.core_v1 import (
    PodTemplateSpec,
    PodSpec,
    Container,
    EnvVar,
    Volume,
    EmptyDirVolumeSource,
    VolumeMount,
    Probe,
    ExecAction,
    ResourceRequirements,
)

from .benchmark import Benchmark


# We do this stuff ONLY to skip TLS without breaking our normal config
def _patch_kubeconfig_file(
    kubeconfig: Optional[str],
    *,
    insecure_skip_tls_verify: bool,
    verify_path: Optional[str],
) -> Optional[str]:
    """Return path to a temp kubeconfig with CA fields removed and/or skip-verify set.

    If kubeconfig is None, try the default (~/.kube/config on Win/Linux/Mac).
    """
    if not (insecure_skip_tls_verify or verify_path):
        return kubeconfig

    src = kubeconfig or os.environ.get("KUBECONFIG")
    if not src:
        # nothing to patch; lightkube will in-cluster/auto-load
        return None

    src = Path(src).expanduser().resolve()
    with open(src, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    # patch all clusters
    for c in (data.get("clusters") or []):
        cl = c.get("cluster", {})
        # drop CA references (prevents ssl from opening temp cafile)
        cl.pop("certificate-authority-data", None)
        cl.pop("certificate-authority", None)
        if insecure_skip_tls_verify:
            cl["insecure-skip-tls-verify"] = True

    # direct httpx/ssl to provided CA bundle if any
    if verify_path:
        os.environ["SSL_CERT_FILE"] = verify_path

    fd, tmp = tempfile.mkstemp(prefix="kubeconfig_", suffix=".yaml")
    os.close(fd)
    with open(tmp, "w", encoding="utf-8") as f:
        yaml.safe_dump(data, f, sort_keys=False)
    return tmp


def _build_client(
    *,
    namespace: str,
    insecure_skip_tls_verify: bool,
    trust_env: bool = True,
    verify_path: Optional[str] = None,
    kubeconfig: Optional[str] = None
) -> AsyncClient:
    # Prepare a patched kubeconfig to avoid CA file permission issues on Windows
    patched = _patch_kubeconfig_file(
        kubeconfig,
        insecure_skip_tls_verify=insecure_skip_tls_verify,
        verify_path=verify_path,
    )
    if patched:
        os.environ["KUBECONFIG"] = patched
    return AsyncClient(namespace=namespace, trust_env=trust_env)


@dataclass
class LightkubeAsyncBenchmark(Benchmark):
    client: str = "lightkube (async)"

    api_client = None
    verify_path: str | None = None
    trust_env: bool = True

    @staticmethod
    def _large_pod_template(name: str) -> PodTemplateSpec:
        common = [EnvVar(name=f"ENV_{i}", value=f"value_{i}") for i in range(50)]

        c1 = Container(
            name="c1",
            image="busybox:stable",
            command=["/bin/sh", "-c"],
            args=["sleep 3600"],
            env=common + [EnvVar(name="C1_ONLY", value="x")],
            volumeMounts=[VolumeMount(name="work", mountPath="/work")],
            resources=ResourceRequirements(
                limits={"cpu": "100m", "memory": "128Mi"},
                requests={"cpu": "50m", "memory": "64Mi"},
            ),
            livenessProbe=Probe(
                exec=ExecAction(command=["/bin/true"]),
                initialDelaySeconds=5,
                periodSeconds=30,
            ),
        )
        c2 = Container(
            name="c2",
            image="busybox:stable",
            command=["/bin/sh", "-c"],
            args=["sleep 3600"],
            env=common + [EnvVar(name="C2_ONLY", value="y")],
            volumeMounts=[VolumeMount(name="work", mountPath="/data")],
            resources=ResourceRequirements(
                limits={"cpu": "100m", "memory": "128Mi"},
                requests={"cpu": "50m", "memory": "64Mi"},
            ),
        )
        c3 = Container(
            name="c3",
            image="busybox:stable",
            command=["/bin/sh", "-c"],
            args=["sleep 3600"],
            env=[EnvVar(name="IMPORTANT", value="bench_value")] + common,
            volumeMounts=[VolumeMount(name="work", mountPath="/cache")],
        )

        vols = [Volume(name="work", emptyDir=EmptyDirVolumeSource())]

        return PodTemplateSpec(
            metadata=ObjectMeta(labels={"app": name}),
            spec=PodSpec(containers=[c1, c2, c3], volumes=vols),
        )

    async def init_client(self):
        self.api_client = _build_client(
            namespace=self.namespace,
            insecure_skip_tls_verify=True,
            verify_path=self.verify_path,
            trust_env=self.trust_env,
        )

        # Don't misbehave
        for name in ("kr8s", "kr8s.asyncio", "httpx", "urllib3", "websockets"):
            logging.getLogger(name).setLevel(logging.WARNING)

    async def create_one(self, name: str):
        body = Deployment(
            metadata=ObjectMeta(
                name=name,
                namespace=self.namespace,
                labels=self.build_bench_labels(name),
            ),
            spec=DeploymentSpec(
                replicas=0,
                selector=LabelSelector(matchLabels={"app": name}),
                template=self._large_pod_template(name),
            ),
        )
        dep: Deployment = await self.api_client.create(body)
        # Ensure labels round-trip correctly
        self.check_bench_labels(name, dep.metadata.labels)
        return dep

    async def get_one(self, name: str):
        dep: Deployment = await self.api_client.get(
            Deployment,
            name=name,
            namespace=self.namespace,
        )
        self.check_bench_labels(name, dep.metadata.labels)
        return dep

    async def delete_one(self, name: str):
        await self.api_client.delete(Deployment, name=name, namespace=self.namespace)

    async def watch_all(self) -> AsyncIterable[Any]:
        async for op, dep in self.api_client.watch(Deployment, namespace=self.namespace):
            yield dep
