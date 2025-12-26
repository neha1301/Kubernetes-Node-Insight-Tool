# Kubernetes Node Insight Tool 🧠⚙️  
### Understand your Kubernetes cluster in one run — no dashboards, no alerts, no guesswork.

A **human-first Kubernetes debugging and exploration tool** that **correlates node health, capacity, and workload behavior** into a single, readable output.

If you’ve ever asked *“why are pods evicted?”*, *“is this a node issue or an app issue?”*, or *“what does this cluster actually look like?”* — this tool is for you.

---

## 🚩 The Problem This Solves

Debugging Kubernetes nodes today usually means:

```bash
kubectl get nodes
kubectl describe node <node>
kubectl top nodes
kubectl get pods -A --field-selector spec.nodeName=<node>
kubectl get events -A
