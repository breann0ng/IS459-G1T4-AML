"""
shape_rules.py
Rule-based pattern namer: a baseline for stage 2 (no model, no training).

A copy of classify() from analysis/typology_attribution.ipynb, frozen after tuning on HI-Small
and checked out of sample on HI-Medium (clean-scheme accuracy 78% / 76%). Do not tune it on
the validation alerts: the point is to show what the trained namer adds over plain shape rules.

Used by group_stage2.py --baselines: every alert group is named from the payments inside it.
"""

import networkx as nx

# rule label -> stage 2 class (before the bipartite + stack -> layering merge)
TO_CLASS = {"FAN-IN": "fan_in", "FAN-OUT": "fan_out", "GATHER-SCATTER": "gather_scatter",
            "SCATTER-GATHER": "scatter_gather", "CYCLE": "cycle", "RANDOM": "random",
            "BIPARTITE": "bipartite", "STACK": "stack"}     # SINGLE / PAIR / OTHER -> unclassified


def classify(g):
    """g: MultiDiGraph of accounts, one edge per payment. Returns the rule label."""
    if g.number_of_edges() == 1:
        return "SINGLE"
    D = nx.DiGraph(g)                                   # distinct counterparties, repeats ignored
    ind, outd = dict(D.in_degree()), dict(D.out_degree())
    src = [v for v in D if ind[v] == 0]
    snk = [v for v in D if outd[v] == 0]
    mid = [v for v in D if ind[v] > 0 and outd[v] > 0]
    hubs = [v for v in D if ind[v] >= 2 and outd[v] >= 2]

    if len(hubs) == 1 and all(hubs[0] in e for e in D.edges()):
        return "GATHER-SCATTER"                         # before CYCLE: hub paying a sender back is still G-S
    if not nx.is_directed_acyclic_graph(D):
        return "CYCLE"                                  # before PAIR: A->B->A is a cycle
    if D.number_of_nodes() == 2:
        return "PAIR"
    if nx.number_weakly_connected_components(D) > 1:    # unconnected pieces (clean-scheme test only)
        return "STACK" if nx.dag_longest_path_length(D) >= 2 else "BIPARTITE"
    if len(src) == 1 and not mid:
        return "FAN-OUT"
    if len(snk) == 1 and not mid:
        return "FAN-IN"
    if (len(src) == 1 and len(snk) == 1 and len(mid) >= 2 and
            all(D.has_edge(src[0], v) and D.has_edge(v, snk[0]) for v in mid)):
        return "SCATTER-GATHER"
    if max(ind.values()) <= 1 and max(outd.values()) <= 1:
        return "RANDOM"                                 # a simple chain
    if not mid and len(src) >= 2 and len(snk) >= 2:
        return "BIPARTITE"
    if mid and nx.dag_longest_path_length(D) >= 2:
        return "STACK"
    return "OTHER"


def name_group(src, dst, merge):
    """Stage 2 class for one group, from its payments (sender and receiver keys)."""
    if len(src) == 0:
        return "unclassified"                           # no payment inside the group: no shape
    c = TO_CLASS.get(classify(nx.MultiDiGraph(list(zip(src, dst)))), "unclassified")
    return merge.get(c, c)
