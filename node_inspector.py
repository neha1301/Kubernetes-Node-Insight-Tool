#!/usr/bin/env python3
"""
Kubernetes Node & Karpenter Insight Tool
Human-first cluster inspection with full capacity overview + OOM analysis + NotReady diagnostics
Enhanced with System OOM detection and comprehensive kubelet issue tracking
"""

import argparse
import re
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from collections import defaultdict

from kubernetes import client, config

try:
    from tabulate import tabulate
except ImportError:
    tabulate = None


# -------------------- Models --------------------

@dataclass
class PodImpact:
    name: str
    namespace: str
    reason: str
    memory_request: Optional[float] = None
    memory_limit: Optional[float] = None
    cpu_request: Optional[float] = None
    cpu_limit: Optional[float] = None
    restart_count: int = 0
    oom_killed: bool = False
    last_state_reason: Optional[str] = None
    

@dataclass
class OOMEvent:
    timestamp: datetime
    pod_name: str
    namespace: str
    container_name: str
    memory_limit: Optional[str] = None
    event_type: str = "pod"  # "pod" or "system"
    victim_process: Optional[str] = None  # For system OOMs


@dataclass
class NotReadyDiagnosis:
    """Comprehensive diagnosis of why a node went NotReady"""
    karpenter_termination: bool = False
    karpenter_reason: Optional[str] = None
    consolidation: bool = False
    expiration: bool = False
    drift: bool = False
    
    node_condition_issues: List[str] = field(default_factory=list)
    kubelet_issues: List[str] = field(default_factory=list)
    
    disk_pressure: bool = False
    memory_pressure: bool = False
    pid_pressure: bool = False
    network_unavailable: bool = False
    
    system_pod_failures: List[str] = field(default_factory=list)
    critical_pod_failures: List[str] = field(default_factory=list)
    
    manual_deletion: bool = False
    manual_cordon: bool = False
    
    recent_oom_kills: int = 0
    high_disk_usage: bool = False
    
    node_deleted: bool = False
    node_not_found: bool = False
    
    cloud_provider: Optional[str] = None
    cloud_issues: List[str] = field(default_factory=list)
    
    termination_events: List[str] = field(default_factory=list)
    recent_actions: List[str] = field(default_factory=list)
    
    readiness_probe_failures: int = 0
    liveness_probe_failures: int = 0
    
    # New kubelet-level issue tracking
    system_oom_count: int = 0
    system_oom_events: List[Dict] = field(default_factory=list)
    kubelet_restarts: int = 0
    config_errors: List[str] = field(default_factory=list)
    cgroup_issues: List[str] = field(default_factory=list)
    image_gc_issues: List[str] = field(default_factory=list)
    eviction_threshold_issues: List[str] = field(default_factory=list)
    runtime_errors: List[str] = field(default_factory=list)
    network_plugin_errors: List[str] = field(default_factory=list)
    volume_errors: List[str] = field(default_factory=list)
    
    root_cause: Optional[str] = None


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
    cpu_allocatable: Optional[float] = None
    mem_allocatable: Optional[float] = None
    disk_pressure: bool = False
    memory_pressure: bool = False
    pid_pressure: bool = False
    network_unavailable: bool = False
    evicted_pods: int = 0
    oom_events: List[OOMEvent] = field(default_factory=list)
    impacting_pods: List[PodImpact] = field(default_factory=list)
    failing_system_pods: List[str] = field(default_factory=list)
    notes: List[str] = field(default_factory=list)
    historical_ready_changes: int = 0
    last_transition_time: Optional[datetime] = None
    diagnosis: Optional[NotReadyDiagnosis] = None


# -------------------- Inspector --------------------

class ClusterInspector:

    def __init__(self, kubeconfig: Optional[str] = None, days_history: int = 7):
        if kubeconfig:
            config.load_kube_config(config_file=kubeconfig)
        else:
            try:
                config.load_incluster_config()
            except:
                config.load_kube_config()

        self.v1 = client.CoreV1Api()
        self.metrics_api = client.CustomObjectsApi()
        self.days_history = days_history

        self.metrics_available = True
        try:
            self.metrics_api.list_cluster_custom_object(
                "metrics.k8s.io", "v1beta1", "nodes"
            )
        except:
            self.metrics_available = False
        
        # Detect cloud provider
        self.cloud_provider = self._detect_cloud_provider()

    def _detect_cloud_provider(self) -> Optional[str]:
        """Detect which cloud provider the cluster is running on"""
        try:
            nodes = self.v1.list_node(limit=1).items
            if nodes:
                labels = nodes[0].metadata.labels or {}
                provider_id = nodes[0].spec.provider_id or ""
                
                if "eks.amazonaws.com" in str(labels) or "aws://" in provider_id:
                    return "EKS"
                elif "azure" in provider_id.lower():
                    return "AKS"
                elif "gce://" in provider_id or "gke" in str(labels):
                    return "GKE"
        except:
            pass
        return None

    # -------------------- Helpers --------------------

    def node_age(self, node):
        ts = node.metadata.creation_timestamp
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        return (datetime.now(timezone.utc) - ts).total_seconds() / 3600

    def is_karpenter(self, node):
        return any(k.startswith("karpenter.sh") for k in (node.metadata.labels or {}))

    def parse_cpu(self, v):
        if not v:
            return 0.0
        v = str(v)
        if v.endswith("n"):
            return float(v[:-1]) / 1e9
        if v.endswith("u"):
            return float(v[:-1]) / 1e6
        if v.endswith("m"):
            return float(v[:-1]) / 1e3
        return float(v)

    def parse_mem(self, v):
        if not v:
            return 0.0
        v = str(v)
        units = {"Ki": 2**10, "Mi": 2**20, "Gi": 2**30, "Ti": 2**40}
        for u, m in units.items():
            if v.endswith(u):
                return float(v[:-len(u)]) * m
        return float(v)

    def bytes_to_gi(self, v):
        return v / (1024 ** 3)

    # -------------------- OOM Detection --------------------

    def detect_oom_kills(self, node_name: str) -> List[OOMEvent]:
        """Detect OOM kills from events in the past N days"""
        oom_events = []
        cutoff_time = datetime.now(timezone.utc) - timedelta(days=self.days_history)
        
        try:
            events = self.v1.list_event_for_all_namespaces(
                field_selector=f"involvedObject.kind=Pod,source=kubelet"
            ).items
            
            for event in events:
                if event.reason == "OOMKilling":
                    event_time = event.last_timestamp or event.first_timestamp
                    if event_time and event_time.replace(tzinfo=timezone.utc) >= cutoff_time:
                        if event.source.host == node_name:
                            oom_events.append(OOMEvent(
                                timestamp=event_time.replace(tzinfo=timezone.utc),
                                pod_name=event.involved_object.name,
                                namespace=event.involved_object.namespace,
                                container_name=event.involved_object.field_path.split('{')[-1].rstrip('}') if event.involved_object.field_path else "unknown",
                                memory_limit=event.message,
                                event_type="pod"
                            ))
        except Exception as e:
            print(f"Warning: Could not fetch OOM events: {e}")
        
        return sorted(oom_events, key=lambda x: x.timestamp, reverse=True)

    # -------------------- Kubelet Issues Detection --------------------

    def detect_kubelet_issues(self, node_name: str) -> Dict[str, List[Dict]]:
        """Detect all kubelet-level issues including system OOMs, config issues, etc."""
        issues = {
            "system_ooms": [],
            "kubelet_restarts": [],
            "config_errors": [],
            "cgroup_issues": [],
            "image_gc_failures": [],
            "eviction_threshold_met": [],
            "runtime_errors": [],
            "network_plugin_errors": [],
            "volume_errors": [],
            "other_warnings": []
        }
        
        cutoff_time = datetime.now(timezone.utc) - timedelta(days=self.days_history)
        
        try:
            # Get ALL events from kubelet source
            events = self.v1.list_event_for_all_namespaces().items
            
            for event in events:
                # Skip if not from kubelet or not from our node
                if not event.source or event.source.component != "kubelet":
                    continue
                    
                if event.source.host != node_name:
                    continue
                
                event_time = event.last_timestamp or event.first_timestamp
                if not event_time:
                    continue
                
                event_time = event_time.replace(tzinfo=timezone.utc)
                if event_time < cutoff_time:
                    continue
                
                reason = event.reason or ""
                message = event.message or ""
                event_data = {
                    "time": event_time,
                    "reason": reason,
                    "message": message,
                    "count": event.count or 1
                }
                
                # Categorize kubelet issues
                
                # System OOM (not pod-specific)
                if reason == "SystemOOM":
                    # Extract victim process from message
                    victim_match = re.search(r'victim process: ([^,]+)', message)
                    victim = victim_match.group(1) if victim_match else "unknown"
                    pid_match = re.search(r'pid: (\d+)', message)
                    pid = pid_match.group(1) if pid_match else "unknown"
                    event_data["victim_process"] = victim
                    event_data["pid"] = pid
                    issues["system_ooms"].append(event_data)
                
                # Kubelet restarts/crashes
                elif "kubelet" in reason.lower() and any(x in message.lower() for x in ["restart", "stopped", "crash", "exit", "failed"]):
                    issues["kubelet_restarts"].append(event_data)
                
                # Configuration errors
                elif any(x in reason for x in ["InvalidConfiguration", "ConfigError", "FailedMount", "InvalidDiskCapacity"]):
                    issues["config_errors"].append(event_data)
                
                # Cgroup issues
                elif "cgroup" in message.lower() or "CgroupError" in reason or "cgroups" in message.lower():
                    issues["cgroup_issues"].append(event_data)
                
                # Image GC failures
                elif "ImageGC" in reason or "image garbage collection" in message.lower() or "ImageGCFailed" in reason:
                    issues["image_gc_failures"].append(event_data)
                
                # Eviction thresholds
                elif "EvictionThreshold" in reason or "eviction threshold" in message.lower() or "Evicting" in reason:
                    issues["eviction_threshold_met"].append(event_data)
                
                # Container runtime errors
                elif any(x in message.lower() for x in ["containerd", "docker", "cri-o", "runtime", "runc"]) and event.type == "Warning":
                    if "error" in message.lower() or "failed" in message.lower():
                        issues["runtime_errors"].append(event_data)
                
                # Network plugin errors
                elif any(x in message.lower() for x in ["cni", "network plugin", "calico", "flannel", "weave", "cilium"]):
                    if event.type == "Warning" or "error" in message.lower():
                        issues["network_plugin_errors"].append(event_data)
                
                # Volume/storage errors
                elif any(x in reason for x in ["VolumeMount", "VolumeFailed", "FailedAttach", "FailedMount", "VolumeError"]):
                    issues["volume_errors"].append(event_data)
                
                # Catch-all for other warnings
                elif event.type == "Warning" and reason not in ["OOMKilling", "Killing"]:
                    issues["other_warnings"].append(event_data)
            
        except Exception as e:
            print(f"Warning: Could not fetch kubelet events: {e}")
        
        # Filter out empty categories and sort by time
        result = {}
        for k, v in issues.items():
            if v:
                result[k] = sorted(v, key=lambda x: x["time"], reverse=True)
        
        return result

    # -------------------- NotReady Diagnosis --------------------

    def diagnose_notready(self, node, node_name: str, pods_on_node: List) -> NotReadyDiagnosis:
        """Comprehensive diagnosis of NotReady state"""
        diagnosis = NotReadyDiagnosis()
        diagnosis.cloud_provider = self.cloud_provider
        
        cutoff_time = datetime.now(timezone.utc) - timedelta(days=self.days_history)
        
        # 1. Check Karpenter termination reasons
        if self.is_karpenter(node):
            labels = node.metadata.labels or {}
            annotations = node.metadata.annotations or {}
            
            # Check for consolidation
            if "karpenter.sh/do-not-consolidate" not in annotations:
                diagnosis.consolidation = True
                diagnosis.karpenter_reason = "Consolidation enabled"
            
            # Check for expiration
            if "karpenter.sh/do-not-evict" in annotations:
                diagnosis.karpenter_reason = "Protected from eviction"
            
            # Check node TTL/expiration
            ttl_expires = annotations.get("karpenter.sh/ttl-expires")
            if ttl_expires:
                diagnosis.expiration = True
                diagnosis.karpenter_reason = f"TTL expiration set: {ttl_expires}"
        
        # 2. Check node conditions for issues
        for condition in (node.status.conditions or []):
            if condition.type == "Ready" and condition.status != "True":
                diagnosis.node_condition_issues.append(
                    f"Ready: {condition.status} - {condition.reason}: {condition.message}"
                )
                if condition.reason:
                    diagnosis.kubelet_issues.append(condition.reason)
            
            if condition.type == "DiskPressure" and condition.status == "True":
                diagnosis.disk_pressure = True
                diagnosis.high_disk_usage = True
                diagnosis.node_condition_issues.append(
                    f"DiskPressure - {condition.reason}: {condition.message}"
                )
            
            if condition.type == "MemoryPressure" and condition.status == "True":
                diagnosis.memory_pressure = True
                diagnosis.node_condition_issues.append(
                    f"MemoryPressure - {condition.reason}: {condition.message}"
                )
            
            if condition.type == "PIDPressure" and condition.status == "True":
                diagnosis.pid_pressure = True
                diagnosis.node_condition_issues.append(
                    f"PIDPressure - {condition.reason}: {condition.message}"
                )
            
            if condition.type == "NetworkUnavailable" and condition.status == "True":
                diagnosis.network_unavailable = True
                diagnosis.node_condition_issues.append(
                    f"NetworkUnavailable - {condition.reason}: {condition.message}"
                )
        
        # 2.5. Detect kubelet-level issues (NEW)
        kubelet_issues = self.detect_kubelet_issues(node_name)
        
        if "system_ooms" in kubelet_issues:
            diagnosis.system_oom_count = len(kubelet_issues["system_ooms"])
            diagnosis.system_oom_events = kubelet_issues["system_ooms"]
            for oom in kubelet_issues["system_ooms"][:3]:
                diagnosis.recent_actions.append(
                    f"SystemOOM: {oom['victim_process']} (PID {oom['pid']}) killed at {oom['time'].strftime('%Y-%m-%d %H:%M:%S')}"
                )
        
        if "kubelet_restarts" in kubelet_issues:
            diagnosis.kubelet_restarts = len(kubelet_issues["kubelet_restarts"])
            for restart in kubelet_issues["kubelet_restarts"][:2]:
                diagnosis.kubelet_issues.append(f"Kubelet restart: {restart['message']}")
        
        if "config_errors" in kubelet_issues:
            for error in kubelet_issues["config_errors"][:3]:
                diagnosis.config_errors.append(f"{error['reason']}: {error['message']}")
        
        if "cgroup_issues" in kubelet_issues:
            for issue in kubelet_issues["cgroup_issues"][:3]:
                diagnosis.cgroup_issues.append(f"{issue['reason']}: {issue['message']}")
        
        if "image_gc_failures" in kubelet_issues:
            for issue in kubelet_issues["image_gc_failures"][:2]:
                diagnosis.image_gc_issues.append(f"{issue['message']}")
        
        if "eviction_threshold_met" in kubelet_issues:
            for issue in kubelet_issues["eviction_threshold_met"][:3]:
                diagnosis.eviction_threshold_issues.append(f"{issue['message']}")
        
        if "runtime_errors" in kubelet_issues:
            for error in kubelet_issues["runtime_errors"][:3]:
                diagnosis.runtime_errors.append(f"{error['message']}")
        
        if "network_plugin_errors" in kubelet_issues:
            for error in kubelet_issues["network_plugin_errors"][:3]:
                diagnosis.network_plugin_errors.append(f"{error['message']}")
        
        if "volume_errors" in kubelet_issues:
            for error in kubelet_issues["volume_errors"][:3]:
                diagnosis.volume_errors.append(f"{error['reason']}: {error['message']}")
        
        # 3. Check for manual cordon
        if node.spec.unschedulable:
            diagnosis.manual_cordon = True
            diagnosis.recent_actions.append("Node was manually cordoned")
        
        # 4. Check events related to this node
        try:
            events = self.v1.list_event_for_all_namespaces(
                field_selector=f"involvedObject.name={node_name},involvedObject.kind=Node"
            ).items
            
            for event in events:
                event_time = event.last_timestamp or event.first_timestamp
                if not event_time:
                    continue
                
                event_time = event_time.replace(tzinfo=timezone.utc)
                if event_time < cutoff_time:
                    continue
                
                reason = event.reason
                message = event.message
                
                # Karpenter termination events
                if "Terminating" in reason or "Terminating" in message:
                    diagnosis.karpenter_termination = True
                    diagnosis.termination_events.append(f"{reason}: {message}")
                
                if "Consolidat" in message:
                    diagnosis.consolidation = True
                    diagnosis.karpenter_reason = "Consolidation"
                
                if "Drift" in message or "Drift" in reason:
                    diagnosis.drift = True
                    diagnosis.karpenter_reason = "Drift detected"
                
                if "Expir" in message or "TTL" in message:
                    diagnosis.expiration = True
                    diagnosis.karpenter_reason = "Node expiration/TTL"
                
                # Manual deletion
                if "Delet" in reason and event.source.component in ["kubectl", "operator"]:
                    diagnosis.manual_deletion = True
                    diagnosis.recent_actions.append(f"Manual deletion by {event.source.component}")
                
                # Kubelet issues
                if event.source.component and "kubelet" in event.source.component.lower():
                    if "NotReady" in message or "stopped" in message.lower():
                        diagnosis.kubelet_issues.append(f"{reason}: {message}")
                
                # Node lifecycle
                if reason in ["NodeNotReady", "NodeReady"]:
                    diagnosis.recent_actions.append(f"{reason} at {event_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        except Exception as e:
            print(f"Warning: Could not fetch node events: {e}")
        
        # 5. Check system pods (critical for node health)
        critical_namespaces = ["kube-system", "kube-node-lease"]
        critical_pod_patterns = ["kube-proxy", "aws-node", "azure-", "calico", "cilium", "coredns", "node-"]
        
        for pod in pods_on_node:
            if pod.metadata.namespace in critical_namespaces:
                if pod.status.phase != "Running":
                    pod_name = pod.metadata.name
                    diagnosis.system_pod_failures.append(f"{pod_name} ({pod.status.phase})")
                    
                    # Check if it's a critical component
                    if any(pattern in pod_name.lower() for pattern in critical_pod_patterns):
                        diagnosis.critical_pod_failures.append(pod_name)
                        
                        # Cloud-specific checks
                        if self.cloud_provider == "EKS" and "aws-node" in pod_name:
                            diagnosis.cloud_issues.append("AWS VPC CNI pod failure detected")
                        elif self.cloud_provider == "EKS" and "kube-proxy" in pod_name:
                            diagnosis.cloud_issues.append("kube-proxy failure detected")
                        elif self.cloud_provider == "AKS" and "azure" in pod_name:
                            diagnosis.cloud_issues.append("Azure CNI pod failure detected")
                        elif self.cloud_provider == "GKE" and ("calico" in pod_name or "cilium" in pod_name):
                            diagnosis.cloud_issues.append("GKE network pod failure detected")
        
        # 6. Check for probe failures
        try:
            events = self.v1.list_event_for_all_namespaces(
                field_selector=f"involvedObject.kind=Pod"
            ).items
            
            for event in events:
                event_time = event.last_timestamp or event.first_timestamp
                if not event_time:
                    continue
                
                event_time = event_time.replace(tzinfo=timezone.utc)
                if event_time < cutoff_time:
                    continue
                
                if event.source.host == node_name:
                    if "Readiness probe failed" in event.message:
                        diagnosis.readiness_probe_failures += 1
                    if "Liveness probe failed" in event.message:
                        diagnosis.liveness_probe_failures += 1
        
        except Exception as e:
            pass
        
        # 7. Check for recent OOM kills
        oom_events = self.detect_oom_kills(node_name)
        diagnosis.recent_oom_kills = len(oom_events)
        
        # 8. Determine root cause
        diagnosis.root_cause = self._determine_root_cause(diagnosis)
        
        return diagnosis

    def _determine_root_cause(self, diagnosis: NotReadyDiagnosis) -> str:
        """Determine the most likely root cause based on diagnosis"""
        
        # Priority 1: Karpenter actions
        if diagnosis.karpenter_termination:
            if diagnosis.consolidation:
                return " Karpenter Consolidation - Node terminated to optimize cluster resources"
            elif diagnosis.expiration:
                return " Karpenter TTL Expiration - Node reached maximum lifetime"
            elif diagnosis.drift:
                return " Karpenter Drift - Node configuration drifted from desired state"
            else:
                return " Karpenter Termination - Automated node lifecycle management"
        
        # Priority 2: Critical system component failures
        if diagnosis.critical_pod_failures:
            pods = ", ".join(diagnosis.critical_pod_failures[:3])
            return f" Critical System Pod Failure - {pods} not running on node"
        
        # Priority 3: Resource pressure
        if diagnosis.disk_pressure:
            return " Disk Pressure - Node ran out of disk space"
        if diagnosis.memory_pressure:
            return " Memory Pressure - Node experiencing memory exhaustion"
        if diagnosis.pid_pressure:
            return " PID Pressure - Too many processes running on node"
        
        # Priority 4: System-level OOM (NEW)
        if diagnosis.system_oom_count >= 2:
            victims = ", ".join([e["victim_process"] for e in diagnosis.system_oom_events[:3]])
            return f" System OOM - Node kernel OOM killer activated ({diagnosis.system_oom_count}x), victims: {victims}"
        
        # Priority 5: OOM kills
        if diagnosis.recent_oom_kills >= 3:
            return f" Multiple OOM Kills - {diagnosis.recent_oom_kills} pods killed due to memory limits"
        
        # Priority 6: Kubelet restarts (NEW)
        if diagnosis.kubelet_restarts >= 2:
            return f" Kubelet Instability - Kubelet restarted {diagnosis.kubelet_restarts} times"
        
        # Priority 7: Kubelet issues
        if diagnosis.kubelet_issues:
            return f" Kubelet Failure - {diagnosis.kubelet_issues[0]}"
        
        # Priority 8: Network issues
        if diagnosis.network_unavailable:
            return " Network Unavailable - Node lost network connectivity"
        
        # Priority 9: Container runtime errors (NEW)
        if diagnosis.runtime_errors:
            return f" Container Runtime Error - {diagnosis.runtime_errors[0][:80]}"
        
        # Priority 10: Configuration errors (NEW)
        if diagnosis.config_errors:
            return f"⚙️ Configuration Error - {diagnosis.config_errors[0][:80]}"
        
        # Priority 11: Cloud provider issues
        if diagnosis.cloud_issues:
            return f" Cloud Provider Issue - {diagnosis.cloud_issues[0]}"
        
        # Priority 12: Manual actions
        if diagnosis.manual_deletion:
            return " Manual Deletion - Node was manually deleted by operator"
        if diagnosis.manual_cordon:
            return " Manual Cordon - Node was manually cordoned (unschedulable)"
        
        # Priority 13: Probe failures
        if diagnosis.liveness_probe_failures > 5:
            return f" Liveness Probe Failures - {diagnosis.liveness_probe_failures} failures detected"
        if diagnosis.readiness_probe_failures > 5:
            return f" Readiness Probe Failures - {diagnosis.readiness_probe_failures} failures detected"
        
        # Default
        if diagnosis.node_condition_issues:
            return f" Node Condition Issue - {diagnosis.node_condition_issues[0]}"
        
        return "❓ Unknown - Unable to determine root cause from available data"

    def analyze_pod_impact(self, pod) -> PodImpact:
        """Analyze a pod's resource usage and impact"""
        total_mem_req = 0.0
        total_mem_lim = 0.0
        total_cpu_req = 0.0
        total_cpu_lim = 0.0
        
        for container in (pod.spec.containers or []):
            if container.resources:
                if container.resources.requests:
                    total_mem_req += self.parse_mem(container.resources.requests.get("memory", "0"))
                    total_cpu_req += self.parse_cpu(container.resources.requests.get("cpu", "0"))
                if container.resources.limits:
                    total_mem_lim += self.parse_mem(container.resources.limits.get("memory", "0"))
                    total_cpu_lim += self.parse_cpu(container.resources.limits.get("cpu", "0"))
        
        # Check container statuses for OOM kills
        oom_killed = False
        last_state_reason = None
        restart_count = 0
        
        if pod.status.container_statuses:
            for cs in pod.status.container_statuses:
                restart_count += cs.restart_count
                if cs.last_state and cs.last_state.terminated:
                    last_state_reason = cs.last_state.terminated.reason
                    if last_state_reason == "OOMKilled":
                        oom_killed = True
        
        reason = pod.status.reason or "Unknown"
        if pod.status.phase == "Failed":
            reason = f"Failed: {pod.status.reason or 'Unknown'}"
        elif pod.status.phase == "Pending":
            reason = "Pending"
        
        return PodImpact(
            name=pod.metadata.name,
            namespace=pod.metadata.namespace,
            reason=reason,
            memory_request=self.bytes_to_gi(total_mem_req) if total_mem_req > 0 else None,
            memory_limit=self.bytes_to_gi(total_mem_lim) if total_mem_lim > 0 else None,
            cpu_request=total_cpu_req if total_cpu_req > 0 else None,
            cpu_limit=total_cpu_lim if total_cpu_lim > 0 else None,
            restart_count=restart_count,
            oom_killed=oom_killed,
            last_state_reason=last_state_reason
        )

    def analyze_node_history(self, node) -> Tuple[int, Optional[datetime]]:
        """Analyze node's ready state changes over the past N days"""
        ready_changes = 0
        last_transition = None
        
        for condition in (node.status.conditions or []):
            if condition.type == "Ready":
                last_transition = condition.last_transition_time
                if last_transition:
                    last_transition = last_transition.replace(tzinfo=timezone.utc)
                
                if last_transition:
                    age = (datetime.now(timezone.utc) - last_transition).days
                    if age <= self.days_history:
                        ready_changes += 1
        
        return ready_changes, last_transition

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
            
            ready_changes, last_transition = self.analyze_node_history(node)

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
                historical_ready_changes=ready_changes,
                last_transition_time=last_transition
            )

            # Check for pressure conditions
            for condition in (node.status.conditions or []):
                if condition.type == "DiskPressure" and condition.status == "True":
                    insight.disk_pressure = True
                    insight.notes.append("DiskPressure reported by kubelet")
                if condition.type == "MemoryPressure" and condition.status == "True":
                    insight.memory_pressure = True
                    insight.notes.append("MemoryPressure reported by kubelet")
                if condition.type == "PIDPressure" and condition.status == "True":
                    insight.pid_pressure = True
                    insight.notes.append("PIDPressure reported by kubelet")
                if condition.type == "NetworkUnavailable" and condition.status == "True":
                    insight.network_unavailable = True
                    insight.notes.append("NetworkUnavailable")

            # Get allocatable resources
            alloc = node.status.allocatable
            insight.cpu_allocatable = self.parse_cpu(alloc.get("cpu", "0"))
            insight.mem_allocatable = self.bytes_to_gi(self.parse_mem(alloc.get("memory", "0")))

            # Get current metrics
            if name in metrics:
                m = metrics[name]["usage"]
                insight.cpu_used = (
                    self.parse_cpu(m["cpu"]) / insight.cpu_allocatable
                ) * 100 if insight.cpu_allocatable > 0 else 0
                insight.mem_used = (
                    self.parse_mem(m["memory"]) / self.parse_mem(alloc["memory"])
                ) * 100

            # Detect OOM kills
            insight.oom_events = self.detect_oom_kills(name)
            if insight.oom_events:
                insight.notes.append(f"  {len(insight.oom_events)} OOM kill(s) in past {self.days_history} days")

            # Analyze pods on this node
            for pod in pods_by_node.get(name, []):
                if pod.status.reason == "Evicted":
                    insight.evicted_pods += 1
                    impact = self.analyze_pod_impact(pod)
                    insight.impacting_pods.append(impact)
                
                # Check for OOM killed pods
                if pod.status.container_statuses:
                    for cs in pod.status.container_statuses:
                        if cs.last_state and cs.last_state.terminated:
                            if cs.last_state.terminated.reason == "OOMKilled":
                                impact = self.analyze_pod_impact(pod)
                                if impact not in insight.impacting_pods:
                                    insight.impacting_pods.append(impact)
                
                # System pod failures
                if pod.metadata.namespace == "kube-system":
                    if pod.status.phase != "Running":
                        insight.failing_system_pods.append(pod.metadata.name)

            # Diagnose NotReady state if applicable
            if insight.ready != "True":
                insight.diagnosis = self.diagnose_notready(node, name, pods_by_node.get(name, []))
                insight.notes.append(f"🔍 ROOT CAUSE: {insight.diagnosis.root_cause}")

            # Analyze if changes are sudden
            if ready_changes > 0 and last_transition:
                days_since = (datetime.now(timezone.utc) - last_transition).days
                if days_since <= 1:
                    insight.notes.append(f" Recent state change detected ({days_since}d ago)")
                elif days_since <= 3:
                    insight.notes.append(f" State changed {days_since}d ago")

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

        print("\n" + "=" * 70)
        print("CLUSTER INSIGHT SUMMARY")
        print("=" * 70)
        print(f"Nodes: {len(node_list)}")
        if self.cloud_provider:
            print(f"Cloud Provider: {self.cloud_provider}")
        print(f"Karpenter-managed: {sum(n.karpenter for n in node_list)}")
        print(f"NotReady: {sum(n.ready != 'True' for n in node_list)}")
        print(f"DiskPressure: {sum(n.disk_pressure for n in node_list)}")
        print(f"MemoryPressure: {sum(n.memory_pressure for n in node_list)}")
        print(f"PIDPressure: {sum(n.pid_pressure for n in node_list)}")
        print(f"Nodes with Evictions: {sum(n.evicted_pods > 0 for n in node_list)}")
        print(f"Nodes with OOM Kills: {sum(len(n.oom_events) > 0 for n in node_list)}")
        print(f"Total OOM Events (past {self.days_history}d): {sum(len(n.oom_events) for n in node_list)}")

        capacity = self.aggregate_capacity(nodes)

        print("\n" + "=" * 70)
        print("NODE CAPACITY OVERVIEW")
        print("=" * 70)
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
                print(f"{r[0]:<20} {r[1]:<10} {r[2]:<20} {r[3]:<20}")

        print(f"\nCluster Total: {int(total_nodes)} nodes | {total_cpu:.1f} cores | {total_mem:.1f} Gi")

        # Sort nodes by severity - NotReady nodes first
        for n in sorted(
            node_list, 
            key=lambda x: (
                -(x.ready != 'True'),  # NotReady nodes first
                -len(x.oom_events),
                -x.disk_pressure, 
                -x.memory_pressure,
                -x.evicted_pods, 
                -x.age_hours
            )
        ):
            print("\n" + "=" * 70)
            print(f"NODE: {n.name}")
            print("=" * 70)
            print(f"Age: {n.age_hours/24:.1f}d | Ready: {n.ready} | Cordoned: {n.cordoned}")
            print(f"Instance: {n.instance_type} | Capacity: {n.capacity_type} | AZ: {n.az}")
            print(f"Karpenter: {n.karpenter}")

            # Show NotReady diagnosis prominently
            if n.ready != "True" and n.diagnosis:
                print("\n" + "-" * 35)
                print("  NOTREADY DIAGNOSIS")
                print("-" * 35)
                print(f"\n ROOT CAUSE:\n   {n.diagnosis.root_cause}\n")
                
                if n.diagnosis.karpenter_termination:
                    print("📋 KARPENTER TERMINATION:")
                    if n.diagnosis.karpenter_reason:
                        print(f"   Reason: {n.diagnosis.karpenter_reason}")
                    print(f"   Consolidation: {n.diagnosis.consolidation}")
                    print(f"   Expiration: {n.diagnosis.expiration}")
                    print(f"   Drift: {n.diagnosis.drift}")
                    if n.diagnosis.termination_events:
                        print("   Events:")
                        for event in n.diagnosis.termination_events[:3]:
                            print(f"      • {event}")
                
                if n.diagnosis.node_condition_issues:
                    print("\n  NODE CONDITIONS:")
                    for issue in n.diagnosis.node_condition_issues:
                        print(f"   • {issue}")
                
                if n.diagnosis.kubelet_issues:
                    print("\n🔧 KUBELET ISSUES:")
                    for issue in n.diagnosis.kubelet_issues[:5]:
                        print(f"   • {issue}")
                
                # NEW: System OOM Events
                if n.diagnosis.system_oom_count > 0:
                    print(f"\n SYSTEM OOM EVENTS ({n.diagnosis.system_oom_count}):")
                    for i, oom in enumerate(n.diagnosis.system_oom_events[:5], 1):
                        days_ago = (datetime.now(timezone.utc) - oom['time']).days
                        hours_ago = (datetime.now(timezone.utc) - oom['time']).seconds // 3600
                        time_str = f"{days_ago}d ago" if days_ago > 0 else f"{hours_ago}h ago"
                        print(f"   {i}. Victim: {oom['victim_process']} (PID: {oom['pid']})")
                        print(f"      Time: {time_str} ({oom['time'].strftime('%Y-%m-%d %H:%M:%S')})")
                        print(f"      Count: {oom['count']}")
                        if 'message' in oom:
                            print(f"      Message: {oom['message'][:100]}")
                
                # NEW: Kubelet Restarts
                if n.diagnosis.kubelet_restarts > 0:
                    print(f"\n KUBELET RESTARTS: {n.diagnosis.kubelet_restarts}")
                
                # NEW: Configuration Errors
                if n.diagnosis.config_errors:
                    print("\n  CONFIGURATION ERRORS:")
                    for error in n.diagnosis.config_errors:
                        print(f"   • {error}")
                
                # NEW: Cgroup Issues
                if n.diagnosis.cgroup_issues:
                    print("\n CGROUP ISSUES:")
                    for issue in n.diagnosis.cgroup_issues:
                        print(f"   • {issue}")
                
                # NEW: Container Runtime Errors
                if n.diagnosis.runtime_errors:
                    print("\n CONTAINER RUNTIME ERRORS:")
                    for error in n.diagnosis.runtime_errors:
                        print(f"   • {error[:150]}")
                
                # NEW: Network Plugin Errors
                if n.diagnosis.network_plugin_errors:
                    print("\n NETWORK PLUGIN ERRORS:")
                    for error in n.diagnosis.network_plugin_errors:
                        print(f"   • {error[:150]}")
                
                # NEW: Volume Errors
                if n.diagnosis.volume_errors:
                    print("\n VOLUME/STORAGE ERRORS:")
                    for error in n.diagnosis.volume_errors:
                        print(f"   • {error[:150]}")
                
                # NEW: Image GC Issues
                if n.diagnosis.image_gc_issues:
                    print("\n IMAGE GARBAGE COLLECTION ISSUES:")
                    for issue in n.diagnosis.image_gc_issues:
                        print(f"   • {issue[:150]}")
                
                # NEW: Eviction Threshold Issues
                if n.diagnosis.eviction_threshold_issues:
                    print("\n  EVICTION THRESHOLD ISSUES:")
                    for issue in n.diagnosis.eviction_threshold_issues:
                        print(f"   • {issue[:150]}")
                
                if n.diagnosis.critical_pod_failures:
                    print("\n CRITICAL POD FAILURES:")
                    for pod in n.diagnosis.critical_pod_failures:
                        print(f"   • {pod}")
                
                if n.diagnosis.system_pod_failures:
                    print(f"\n SYSTEM POD FAILURES ({len(n.diagnosis.system_pod_failures)}):")
                    for pod in n.diagnosis.system_pod_failures[:5]:
                        print(f"   • {pod}")
                    if len(n.diagnosis.system_pod_failures) > 5:
                        print(f"   ... and {len(n.diagnosis.system_pod_failures) - 5} more")
                
                if n.diagnosis.cloud_issues:
                    print(f"\n  CLOUD PROVIDER ISSUES ({n.diagnosis.cloud_provider}):")
                    for issue in n.diagnosis.cloud_issues:
                        print(f"   • {issue}")
                
                print("\n PRESSURE CONDITIONS:")
                print(f"   DiskPressure: {n.diagnosis.disk_pressure}")
                print(f"   MemoryPressure: {n.diagnosis.memory_pressure}")
                print(f"   PIDPressure: {n.diagnosis.pid_pressure}")
                print(f"   NetworkUnavailable: {n.diagnosis.network_unavailable}")
                
                if n.diagnosis.recent_oom_kills > 0:
                    print(f"\n POD OOM ACTIVITY:")
                    print(f"   Recent Pod OOM Kills: {n.diagnosis.recent_oom_kills}")
                
                if n.diagnosis.readiness_probe_failures > 0 or n.diagnosis.liveness_probe_failures > 0:
                    print(f"\n  PROBE FAILURES:")
                    print(f"   Readiness: {n.diagnosis.readiness_probe_failures}")
                    print(f"   Liveness: {n.diagnosis.liveness_probe_failures}")
                
                if n.diagnosis.manual_deletion or n.diagnosis.manual_cordon:
                    print("\n MANUAL ACTIONS:")
                    print(f"   Manual Deletion: {n.diagnosis.manual_deletion}")
                    print(f"   Manual Cordon: {n.diagnosis.manual_cordon}")
                
                if n.diagnosis.recent_actions:
                    print("\n RECENT ACTIONS:")
                    for action in n.diagnosis.recent_actions[:5]:
                        print(f"   • {action}")
                
                print("\n" + "-" * 35 + "\n")

            print("\n RESOURCES:")
            print(f"  Allocatable: {n.cpu_allocatable:.2f} cores, {n.mem_allocatable:.2f} Gi")
            if n.cpu_used is not None:
                print(f"  CPU Usage: {n.cpu_used:.1f}%")
            if n.mem_used is not None:
                print(f"  Memory Usage: {n.mem_used:.1f}%")
            print(f"  DiskPressure: {n.disk_pressure}")
            print(f"  MemoryPressure: {n.memory_pressure}")
            if n.pid_pressure:
                print(f"  PIDPressure: {n.pid_pressure}")
            if n.network_unavailable:
                print(f"  NetworkUnavailable: {n.network_unavailable}")

            print(f"\n STABILITY (past {self.days_history}d):")
            if n.last_transition_time:
                days_ago = (datetime.now(timezone.utc) - n.last_transition_time).days
                print(f"  Last Ready State Change: {days_ago}d ago")
            print(f"  Ready State Changes: {n.historical_ready_changes}")
            
            if n.historical_ready_changes == 0:
                print("   Node has been stable")
            elif n.historical_ready_changes <= 2:
                print("   Some instability detected")
            else:
                print("   Significant instability detected")

            if n.oom_events:
                print(f"\n POD OOM KILLS ({len(n.oom_events)} events):")
                for i, oom in enumerate(n.oom_events[:5], 1):
                    days_ago = (datetime.now(timezone.utc) - oom.timestamp).days
                    print(f"  {i}. Pod: {oom.namespace}/{oom.pod_name}")
                    print(f"     Container: {oom.container_name}")
                    print(f"     Time: {days_ago}d ago ({oom.timestamp.strftime('%Y-%m-%d %H:%M:%S')})")
                if len(n.oom_events) > 5:
                    print(f"  ... and {len(n.oom_events) - 5} more")

            print(f"\n🔧 WORKLOADS:")
            print(f"  Evicted Pods: {n.evicted_pods}")
            if n.failing_system_pods:
                print(f"  Failing System Pods: {', '.join(n.failing_system_pods[:3])}")
                if len(n.failing_system_pods) > 3:
                    print(f"     ... and {len(n.failing_system_pods) - 3} more")

            if n.impacting_pods:
                print(f"\n  IMPACTING PODS ({len(n.impacting_pods)}):")
                for i, pod in enumerate(n.impacting_pods[:10], 1):
                    print(f"  {i}. {pod.namespace}/{pod.name}")
                    print(f"     Status: {pod.reason}")
                    if pod.oom_killed:
                        print(f"      OOMKilled (Last State: {pod.last_state_reason})")
                    if pod.restart_count > 0:
                        print(f"     Restarts: {pod.restart_count}")
                    if pod.memory_request or pod.memory_limit:
                        mem_req = f"{pod.memory_request:.2f}Gi" if pod.memory_request else "N/A"
                        mem_lim = f"{pod.memory_limit:.2f}Gi" if pod.memory_limit else "N/A"
                        print(f"     Memory: Request={mem_req}, Limit={mem_lim}")
                    if pod.cpu_request or pod.cpu_limit:
                        cpu_req = f"{pod.cpu_request:.2f}" if pod.cpu_request else "N/A"
                        cpu_lim = f"{pod.cpu_limit:.2f}" if pod.cpu_limit else "N/A"
                        print(f"     CPU: Request={cpu_req}, Limit={cpu_lim}")
                if len(n.impacting_pods) > 10:
                    print(f"  ... and {len(n.impacting_pods) - 10} more")

            if n.notes:
                print("\n NOTES:")
                for note in n.notes:
                    print(f"  • {note}")

        print("\n" + "=" * 70)
        print("INSPECTION COMPLETE")
        print("=" * 70 + "\n")


# -------------------- CLI --------------------

def main():
    parser = argparse.ArgumentParser(
        description="Kubernetes Node Insight Tool with OOM Analysis and NotReady Diagnostics"
    )
    parser.add_argument("--kubeconfig", help="Path to kubeconfig")
    parser.add_argument(
        "--days", 
        type=int, 
        default=7, 
        help="Number of days to look back for historical analysis (default: 7)"
    )
    args = parser.parse_args()

    inspector = ClusterInspector(args.kubeconfig, days_history=args.days)
    insights, nodes = inspector.analyze()
    inspector.render(insights, nodes)


if __name__ == "__main__":
    main()

