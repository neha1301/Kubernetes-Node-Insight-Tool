#!/usr/bin/env python3

import argparse
from datetime import datetime, timezone
from typing import Dict, List, Optional
from dataclasses import dataclass, field
from collections import defaultdict

from kubernetes import client, config

try:
    from tabulate import tabulate
except ImportError:
    tabulate = None

# -------------------- Models --------------------

@dataclass
class NodeInsight:
    name: str
    age_hours: float
    ready: str
    karpenter: bool
    instance_type: str
    capacity_type: str
    az: str
    cordoned: bool
    cpu_used: Optional[float] = None
    mem_used: Optional[float] = None
    disk_pressure: bool = False
    evicted_pods: int = 0
    failing_system_pods: List[str] = field(default_factory=list)
    notes: List[str] = field(default_factory=list)

# -------------------- Inspector --------------------

class ClusterInspector:

    def __init__(self, kubeconfig: Optional[str] = None):
        if kubeconfig:
            config.load_kube_config(config_file=kubeconfig)
        else:
            try:
                config.load_incluster_config()
            except:
                config.load_kube_config()

        self.v1 = client.CoreV1Api()
        self.metrics_api = client.CustomObjectsApi()

        self.metrics_available = True
        try:
            self.metrics_api.list_cluster_custom_object(
                "metrics.k8s.io", "v1beta1", "nodes"
            )
        except:
            self.metrics_available = False

    # -------------------- Helpers --------------------

    def node_age(self, node):
        ts = node.metadata.creation_timestamp
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        return (datetime.now(timezone.utc) - ts).total_seconds() / 3600

    def is_karpenter(self, node):
        return any(k.startswith("karpenter.sh") for k in (node.metadata.labels or {}))

    def parse_cpu(self, v):
        if v.endswith("n"):
            return float(v[:-1]) / 1e9
        if v.endswith("u"):
            return float(v[:-1]) / 1e6
        if v.endswith("m"):
            return float(v[:-1]) / 1e3
        return float(v)

    def parse_mem(self, v):
        units = {"Ki": 2**10, "Mi": 2**20, "Gi": 2**30}
        for u, m in units.items():
            if v.endswith(u):
                return float(v[:-len(u)]) * m
        return float(v)

    def bytes_to_gi(self, v):
        return v / (1024 ** 3)

    # -------------------- Collection --------------------

    def collect(self):
        nodes = self.v1.list_node().items
        pods = self.v1.list_pod_for_all_namespaces().items

        pods_by_node = defaultdict(list)
        for pod in pods:
            if pod.spec.node_name:
                pods_by_node[pod.spec.node_name].append(pod)

        metrics = {}
        if self.metrics_available:
            try:
                data = self.metrics_api.list_cluster_custom_object(
                    "metrics.k8s.io", "v1beta1", "nodes"
                )
                for i in data["items"]:
                    metrics[i["metadata"]["name"]] = i
            except:
                pass

        return nodes, pods_by_node, metrics

    # -------------------- Analysis --------------------

    def analyze(self):
        nodes, pods_by_node, metrics = self.collect()
        insights: Dict[str, NodeInsight] = {}

        for node in nodes:
            labels = node.metadata.labels or {}
            name = node.metadata.name
            age = self.node_age(node)

            insight = NodeInsight(
                name=name,
                age_hours=age,
                ready=next(
                    (c.status for c in node.status.conditions if c.type == "Ready"),
                    "Unknown",
                ),
                karpenter=self.is_karpenter(node),
                instance_type=labels.get("node.kubernetes.io/instance-type", "unknown"),
                capacity_type=labels.get("karpenter.sh/capacity-type", "unknown"),
                az=labels.get("topology.kubernetes.io/zone", "unknown"),
                cordoned=bool(node.spec.unschedulable),
            )

            if any(
                c.type == "DiskPressure" and c.status == "True"
                for c in (node.status.conditions or [])
            ):
                insight.disk_pressure = True
                insight.notes.append("DiskPressure reported by kubelet")

            if name in metrics:
                m = metrics[name]["usage"]
                alloc = node.status.allocatable
                insight.cpu_used = (
                    self.parse_cpu(m["cpu"]) / self.parse_cpu(alloc["cpu"])
                ) * 100
                insight.mem_used = (
                    self.parse_mem(m["memory"]) / self.parse_mem(alloc["memory"])
                ) * 100

            for pod in pods_by_node.get(name, []):
                if pod.status.reason == "Evicted":
                    insight.evicted_pods += 1
                if pod.metadata.namespace == "kube-system":
                    if pod.status.phase != "Running":
                        insight.failing_system_pods.append(pod.metadata.name)

            insights[name] = insight

        return insights, nodes

    # -------------------- Capacity --------------------

    def aggregate_capacity(self, nodes):
        summary = defaultdict(lambda: {"count": 0, "cpu": 0.0, "mem": 0.0})

        for node in nodes:
            labels = node.metadata.labels or {}
            itype = labels.get("node.kubernetes.io/instance-type", "unknown")
            alloc = node.status.allocatable

            cpu = self.parse_cpu(alloc.get("cpu", "0"))
            mem = self.bytes_to_gi(self.parse_mem(alloc.get("memory", "0")))

            summary[itype]["count"] += 1
            summary[itype]["cpu"] += cpu
            summary[itype]["mem"] += mem

        return summary

    # -------------------- Rendering --------------------

    def render(self, insights: Dict[str, NodeInsight], nodes):
        node_list = list(insights.values())

        print("\nCluster Insight Summary")
        print("──────────────────────")
        print(f"Nodes: {len(node_list)}")
        print(f"Karpenter-managed: {sum(n.karpenter for n in node_list)}")
        print(f"NotReady: {sum(n.ready != 'True' for n in node_list)}")
        print(f"DiskPressure: {sum(n.disk_pressure for n in node_list)}")
        print(f"Nodes with Evictions: {sum(n.evicted_pods > 0 for n in node_list)}")

        capacity = self.aggregate_capacity(nodes)

        print("\nNode Capacity Overview")
        print("──────────────────────")
        rows = []
        total_nodes = total_cpu = total_mem = 0.0

        for itype, data in sorted(capacity.items()):
            rows.append([
                itype,
                data["count"],
                f"{data['cpu']:.1f}",
                f"{data['mem']:.1f}",
            ])
            total_nodes += data["count"]
            total_cpu += data["cpu"]
            total_mem += data["mem"]

        if tabulate:
            print(tabulate(
                rows,
                headers=["Instance Type", "Nodes", "Total CPU (cores)", "Total Memory (Gi)"],
                tablefmt="grid",
            ))
        else:
            for r in rows:
                print(r)

        print("\nCluster Total:")
        print(f"• Nodes: {int(total_nodes)}")
        print(f"• CPU: {total_cpu:.1f} cores")
        print(f"• Memory: {total_mem:.1f} Gi")

        for n in sorted(
            node_list, key=lambda x: (-x.disk_pressure, -x.evicted_pods, -x.age_hours)
        ):
            print("\n" + "─" * 64)
            print(f"Node: {n.name}")
            print(
                f"Age: {n.age_hours/24:.1f}d | Ready: {n.ready} | Cordoned: {n.cordoned}"
            )
            print(
                f"Instance: {n.instance_type} | Capacity: {n.capacity_type} | AZ: {n.az}"
            )
            print(f"Karpenter: {n.karpenter}")

            print("Resources:")
            if n.cpu_used is not None:
                print(f"  CPU Used: {n.cpu_used:.1f}%")
            if n.mem_used is not None:
                print(f"  Memory Used: {n.mem_used:.1f}%")
            print(f"  DiskPressure: {n.disk_pressure}")

            print("Workloads:")
            print(f"  Evicted Pods: {n.evicted_pods}")
            if n.failing_system_pods:
                print(
                    f"  Failing system pods: {', '.join(n.failing_system_pods[:3])}"
                )

            if n.notes:
                print("Notes:")
                for note in n.notes:
                    print(f"  • {note}")

        print("\nInspection complete.\n")

# -------------------- CLI --------------------

def main():
    parser = argparse.ArgumentParser(description="Kubernetes Node Insight Tool")
    parser.add_argument("--kubeconfig", help="Path to kubeconfig")
    args = parser.parse_args()

    inspector = ClusterInspector(args.kubeconfig)
    insights, nodes = inspector.analyze()
    inspector.render(insights, nodes)


if __name__ == "__main__":
    main()
