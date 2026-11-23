#!/usr/bin/env python3
"""Exhaustive explicit-state model checker for the v1 two-cluster sync replication failover protocol.

It explores every interleaving of the actions below (breadth-first, every reachable state) within the
bounds and checks the safety invariants in every state. Equivalent to running TLC on a bounded model.

One namespace (one workflow history, enough to exercise divergence), two clusters 0 (A) and 1 (B).

Modeled, as agreed in docs/architecture/sync-replication-design.md ("v1 failover"):
  - Writes on the cluster that believes it is active append (wid, failover version) to its history and
    enqueue a replication task carrying the resulting history (state-based replication).
  - The other cluster processes tasks in queue order and merges them NDC-style: a prefix extension is
    adopted (also when it is active: uncommitted writes of an old active are kept), divergent branches
    resolve to the higher failover version, duplicates are no-ops.
  - Acks are task-queue watermarks (like Temporal's), so a task merged as a no-op or ignored still counts.
    Every ack carries the fencing notice: the sender's failover version if it believes it is active.
  - Commit (client acknowledgement) of a write needs an ack from every other member of the writer's
    applied in-sync set, where the peer's fencing notice is not newer than the failover version the write
    was made under. A writer that was demoted keeps trying to commit (worst case).
  - Planned failover: handover blocks writes, waits until the target acked every task, then switches.
  - Forced failover: only to a member of the control plane's in-sync set.
  - Control plane: drop a non-active member from the in-sync set (record first, push later), push the
    record to clusters at any time, two-step rejoin (active waits first, then member becomes eligible
    once caught up).
  - Namespace metadata (active cluster, failover version) propagates asynchronously.
  - Ack messages may be lost or delayed (bounded FIFO channel). A crashed or partitioned cluster is a
    cluster that takes no steps; a partitioned old active may keep taking steps.

  - Term rule (found by this checker, Raft's "commit only current-term entries"): the active commits only
    writes made under its current failover version; a read exposes the history only once its last write
    is such a committed write. A new active therefore writes a version-bump transition before exposing
    a workflow whose last write is older.
  - Reads: a read on the active exposes its whole history (exposed writes are treated as acknowledged).

Invariants:
  Durable:     every acknowledged (or exposed) write is in the history of every in-sync cluster.
  OnActive:    every acknowledged write is in the history of the current active cluster.
  Consistent:  acknowledged writes appear in the same relative order in every history.

Mutations (each deliberately breaks one rule; the checker must find a violation):
  no-fencing, no-term-rule, one-step-rejoin, rejoin-without-catch-up, active-shrinks-first,
  force-to-any, handover-without-drain.

Usage: model.py [--max-writes N] [--max-failovers N] [--mutation NAME]
"""
import argparse
import sys
from collections import deque

A, B = 0, 1
OTHER = (B, A)
ACK_CHANNEL_BOUND = 2


def other(c):
    return OTHER[c]


def wids(log):
    return [e[0] for e in log]


def merge(local, snap):
    """NDC-style merge of an incoming history snapshot into the local history."""
    if snap[: len(local)] == local:  # local is a prefix of snap (or equal): adopt the extension
        return snap
    if local[: len(snap)] == snap:  # duplicate / older prefix: no-op
        return local
    # Diverged: the branch whose last write has the higher failover version wins.
    lv, sv = local[-1][1], snap[-1][1]
    if sv > lv:
        return snap
    return local


class State:
    __slots__ = ("logs", "tq", "pos", "ackwm", "fence", "ackch", "view", "handover", "isrv",
                 "cpisr", "cpjoin", "gfv", "gactive", "acked", "nwrites", "nfail")

    def key(self):
        return (self.logs, self.tq, self.pos, self.ackwm, self.fence, self.ackch, self.view,
                self.handover, self.isrv, self.cpisr, self.cpjoin, self.gfv, self.gactive,
                self.acked, self.nwrites, self.nfail)

    @staticmethod
    def from_key(k):
        s = State()
        (s.logs, s.tq, s.pos, s.ackwm, s.fence, s.ackch, s.view, s.handover, s.isrv, s.cpisr,
         s.cpjoin, s.gfv, s.gactive, s.acked, s.nwrites, s.nfail) = k
        return s


def setidx(t, i, v):
    lst = list(t)
    lst[i] = v
    return tuple(lst)


def initial():
    s = State()
    s.logs = ((), ())
    s.tq = ((), ())
    s.pos = (0, 0)          # pos[d]: next index of the other cluster's task queue that d processes
    s.ackwm = (0, 0)        # ackwm[c]: tasks of c the other cluster has acked (watermark)
    s.fence = (0, 0)        # fence[c]: highest failover version the other cluster fenced c with
    s.ackch = ((), ())      # ackch[d]: acks in flight from d to the other cluster: (watermark, fence)
    s.view = ((1, A), (1, A))  # (failover version, active cluster) as each cluster sees it
    s.handover = (False, False)
    s.isrv = (frozenset({A, B}), frozenset({A, B}))  # in-sync set each cluster applied
    s.cpisr = frozenset({A, B})  # control plane record: promotable clusters
    s.cpjoin = frozenset()       # control plane record: rejoining (waited on, not promotable)
    s.gfv, s.gactive = 1, A      # the latest failover issued
    s.acked = frozenset()        # writes acknowledged to clients
    s.nwrites = 0
    s.nfail = 0
    return s


def fenced(s, c):
    return s.fence[c] > s.view[c][0]


def successors(s, mut, max_w, max_f):
    out = []

    def emit(name, **chg):
        n = State.from_key(s.key())
        for k, v in chg.items():
            setattr(n, k, v)
        out.append((name, n))

    for c in (A, B):
        d = other(c)
        fv, active = s.view[c]

        # Write on a cluster that believes it is active.
        fenced_c = fenced(s, c) and mut != "no-fencing"
        if active == c and not s.handover[c] and not fenced_c and s.nwrites < max_w:
            w = s.nwrites + 1
            log = s.logs[c] + ((w, fv),)
            emit(f"Write({c},w{w})", logs=setidx(s.logs, c, log),
                 tq=setidx(s.tq, c, s.tq[c] + ((w, fv, log),)), nwrites=w)

        # Commit (acknowledge to client) a write c originated.
        for i, (w, wfv, _snap) in enumerate(s.tq[c]):
            if w in s.acked or w not in wids(s.logs[c]):
                continue
            need = s.isrv[c] - {c}
            # An ack counts for w only if the peer was not active at a newer failover version than w's
            # (such a peer may have ignored w). Compared against the write's version, not c's view.
            not_fenced = mut == "no-fencing" or s.fence[c] <= wfv
            # Term rule (Raft's "commit only current-term entries"): only the active, and only for a
            # write made under its current failover version. Older writes become durable through a
            # later current-version write.
            current_term = mut == "no-term-rule" or (active == c and wfv == fv)
            ok = current_term and all(s.ackwm[c] >= i + 1 and not_fenced for _ in need)
            if ok:
                emit(f"Commit({c},w{w})", acked=s.acked | {w})

        # Read on the active: it exposes the whole history once the last write is committed and was made
        # under the active's current failover version (otherwise it must first write a version bump).
        log = s.logs[c]
        if active == c and log and log[-1][0] in s.acked and not set(wids(log)) <= s.acked:
            if mut == "no-term-rule" or log[-1][1] == fv:
                emit(f"Read({c})", acked=s.acked | set(wids(log)))

        # Process the next replication task from the other cluster.
        if s.pos[c] < len(s.tq[d]):
            _w, _fv, snap = s.tq[d][s.pos[c]]
            emit(f"Process({c}<-{d})", logs=setidx(s.logs, c, merge(s.logs[c], snap)),
                 pos=setidx(s.pos, c, s.pos[c] + 1))

        # Send an ack (watermark + fencing notice) to the other cluster.
        if len(s.ackch[c]) < ACK_CHANNEL_BOUND:
            f = fv if active == c else 0
            msg = (s.pos[c], f)
            if not s.ackch[c] or s.ackch[c][-1] != msg:
                emit(f"SendAck({c}->{d})", ackch=setidx(s.ackch, c, s.ackch[c] + (msg,)))

        # Receive or lose the oldest ack in flight from d to c.
        if s.ackch[d]:
            wm, f = s.ackch[d][0]
            emit(f"RecvAck({c}<-{d})", ackwm=setidx(s.ackwm, c, max(s.ackwm[c], wm)),
                 fence=setidx(s.fence, c, max(s.fence[c], f)), ackch=setidx(s.ackch, d, s.ackch[d][1:]))
            emit(f"LoseAck({d}->{c})", ackch=setidx(s.ackch, d, s.ackch[d][1:]))

        # Namespace metadata reaches c.
        if s.view[c] != (s.gfv, s.gactive):
            emit(f"NsSync({c})", view=setidx(s.view, c, (s.gfv, s.gactive)))

        # Planned failover (handover workflow) from c to d.
        if active == c and s.gactive == c and not s.handover[c]:
            emit(f"StartHandover({c})", handover=setidx(s.handover, c, True))
        if s.handover[c]:
            emit(f"AbortHandover({c})", handover=setidx(s.handover, c, False))
            drained = s.ackwm[c] >= len(s.tq[c]) or mut == "handover-without-drain"
            if active == c and d in s.cpisr and drained and s.nfail < max_f:
                g = s.gfv + 1
                emit(f"CompleteHandover({c}->{d})", gfv=g, gactive=d, view=setidx(s.view, c, (g, d)),
                     handover=setidx(s.handover, c, False), nfail=s.nfail + 1)

        # Forced failover to c (UpdateNamespace issued on c), only to an in-sync cluster.
        if s.gactive != c and (c in s.cpisr or mut == "force-to-any") and s.nfail < max_f:
            g = s.gfv + 1
            emit(f"ForcedFailover(->{c})", gfv=g, gactive=c, view=setidx(s.view, c, (g, c)),
                 nfail=s.nfail + 1)

        # Control plane drops c from the in-sync set (record first; pushed later).
        if c in s.cpisr and c != s.gactive:
            emit(f"CpShrink({c})", cpisr=s.cpisr - {c})
        # Broken variant: the active stops waiting for c before the control plane records it.
        if mut == "active-shrinks-first" and c != s.gactive and c in s.isrv[s.gactive]:
            a = s.gactive
            emit(f"ActiveShrinksLocally({a} drops {c})", isrv=setidx(s.isrv, a, s.isrv[a] - {c}))

        # Control plane pushes its record to c.
        rec = s.cpisr | s.cpjoin
        if s.isrv[c] != rec:
            emit(f"PushIsr({c})", isrv=setidx(s.isrv, c, rec))

        # Two-step rejoin of c.
        if c not in s.cpisr and c not in s.cpjoin:
            if mut == "one-step-rejoin":
                emit(f"CpRejoin({c})", cpisr=s.cpisr | {c})
            else:
                emit(f"CpRejoinStart({c})", cpjoin=s.cpjoin | {c})
        if c in s.cpjoin:
            a = s.gactive
            waited = c in s.isrv[a]
            caught_up = s.ackwm[a] >= len(s.tq[a]) or mut == "rejoin-without-catch-up"
            if a != c and waited and caught_up:
                emit(f"CpRejoinFinish({c})", cpisr=s.cpisr | {c}, cpjoin=s.cpjoin - {c})
    return out


def check(s):
    acked = s.acked
    for w in acked:
        for c in s.cpisr:
            if w not in wids(s.logs[c]):
                return f"Durable: acknowledged w{w} is missing on in-sync cluster {c}"
        if w not in wids(s.logs[s.gactive]):
            return f"OnActive: acknowledged w{w} is missing on the active cluster {s.gactive}"
    orders = []
    for c in (A, B):
        orders.append([w for w in wids(s.logs[c]) if w in acked])
    if any(x != y[: len(x)] and y != x[: len(y)] for x in orders for y in orders):
        common = set(orders[0]) & set(orders[1])
        o0 = [w for w in orders[0] if w in common]
        o1 = [w for w in orders[1] if w in common]
        if o0 != o1:
            return f"Consistent: acknowledged writes ordered differently: {o0} vs {o1}"
    return None


def describe(s):
    return (f"  A log={s.logs[0]} view={s.view[0]} isr={sorted(s.isrv[0])}\n"
            f"  B log={s.logs[1]} view={s.view[1]} isr={sorted(s.isrv[1])}\n"
            f"  control plane: in-sync={sorted(s.cpisr)} joining={sorted(s.cpjoin)} "
            f"active={s.gactive} fv={s.gfv}; acknowledged={sorted(s.acked)}")


def run(max_w, max_f, mut):
    init = initial()
    k0 = init.key()
    parent = {k0: None}
    queue = deque([k0])
    transitions = 0
    while queue:
        k = queue.popleft()
        s = State.from_key(k)
        bad = check(s)
        if bad:
            trace = []
            while k is not None:
                p = parent[k]
                trace.append((p[1] if p else "Init", State.from_key(k)))
                k = p[0] if p else None
            return False, len(parent), transitions, bad, list(reversed(trace))
        for name, n in successors(s, mut, max_w, max_f):
            transitions += 1
            nk = n.key()
            if nk not in parent:
                parent[nk] = (k, name)
                queue.append(nk)
    return True, len(parent), transitions, None, None


def run_fingerprint(max_w, max_f, mut):
    """Depth-first search keeping only 64-bit state fingerprints (like TLC). No counterexample trace:
    rerun without --fingerprint at a smaller bound to get one."""
    stack = [initial().key()]
    seen = {hash(stack[0])}
    transitions = 0
    while stack:
        k = stack.pop()
        s = State.from_key(k)
        bad = check(s)
        if bad:
            return False, len(seen), transitions, bad, None
        for _name, n in successors(s, mut, max_w, max_f):
            transitions += 1
            nk = n.key()
            h = hash(nk)
            if h not in seen:
                seen.add(h)
                stack.append(nk)
                if len(seen) % 5_000_000 == 0:
                    print(f"  ... {len(seen)} states, stack {len(stack)}", flush=True)
    return True, len(seen), transitions, None, None


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--max-writes", type=int, default=3)
    p.add_argument("--max-failovers", type=int, default=2)
    p.add_argument("--mutation", default="")
    p.add_argument("--trace", action="store_true", help="print the full counterexample with states")
    p.add_argument("--fingerprint", action="store_true", help="store only state hashes (large bounds)")
    a = p.parse_args()
    runner = run_fingerprint if a.fingerprint else run
    ok, states, transitions, bad, trace = runner(a.max_writes, a.max_failovers, a.mutation)
    label = a.mutation or "protocol"
    if ok:
        print(f"[{label}] OK: {states} distinct states, {transitions} transitions, all invariants hold "
              f"(max writes {a.max_writes}, max failovers {a.max_failovers})")
        return 0
    print(f"[{label}] VIOLATION after exploring {states} states: {bad}")
    if trace is None:
        return 1
    print(f"  counterexample ({len(trace) - 1} steps): " + " -> ".join(t[0] for t in trace[1:]))
    if a.trace:
        for name, st in trace:
            print(name)
            print(describe(st))
    return 1


if __name__ == "__main__":
    sys.exit(main())
