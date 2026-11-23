--------------------------- MODULE SyncReplication ---------------------------
(***************************************************************************)
(* The v1 two-cluster sync replication failover protocol                   *)
(* (docs/architecture/sync-replication-design.md, "v1 failover").          *)
(*                                                                         *)
(* Mirrors model.py action for action; for the same bounds TLC and         *)
(* model.py report the same number of distinct states.                     *)
(*                                                                         *)
(* One namespace (one workflow history), clusters 0 (A) and 1 (B).         *)
(*  - The cluster that believes it is active appends <<write, failover     *)
(*    version>> and enqueues a replication task carrying the resulting     *)
(*    history (state-based replication).                                   *)
(*  - The other cluster processes tasks in order and merges NDC-style:     *)
(*    extensions are adopted ("keep" model), divergent branches resolve to *)
(*    the higher last failover version, duplicates are no-ops.             *)
(*  - Acks are task-queue watermarks carrying a fencing notice (the        *)
(*    sender's failover version if it believes it is active), over a       *)
(*    bounded FIFO channel that may lose messages.                         *)
(*  - Commit needs acks from the other members of the writer's applied     *)
(*    in-sync set, from a peer whose notice is not newer than the write's  *)
(*    failover version, and only for writes made under the committer's     *)
(*    current failover version (term rule). Reads on the active expose the *)
(*    history once its last write is such a committed write.               *)
(*  - Planned handover, forced failover to an in-sync cluster, control     *)
(*    plane shrink, record push and two-step rejoin, async namespace       *)
(*    propagation. A demoted cluster may keep acting (worst case).         *)
(*                                                                         *)
(* Mutation selects a deliberately broken variant ("none" = the protocol). *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS MaxWrites, MaxFailovers, Mutation

Clusters == {0, 1}
Other(c) == 1 - c
Max(a, b) == IF a > b THEN a ELSE b
AckChannelBound == 2

VARIABLES
    logs,      \* logs[c]: history at c, a sequence of <<write id, failover version>>
    tq,        \* tq[c]: replication tasks c originated: [w, fv, snap]
    pos,       \* pos[c]: number of the other cluster's tasks c has processed
    ackwm,     \* ackwm[c]: c's tasks the other cluster has acked (watermark)
    fence,     \* fence[c]: highest fencing version the other cluster sent c
    ackch,     \* ackch[c]: acks in flight from c to the other cluster: <<watermark, fence>>
    view,      \* view[c]: [fv, active] as c sees the namespace
    handover,  \* handover[c]: c is running a planned handover
    isrv,      \* isrv[c]: in-sync set c applied (who c's commits wait for)
    cpisr,     \* control plane record: promotable clusters
    cpjoin,    \* control plane record: rejoining (waited on, not promotable)
    gfv,       \* latest failover version issued
    gactive,   \* latest active cluster issued
    acked,     \* writes acknowledged to clients or exposed by reads
    nwrites,
    nfail

vars == <<logs, tq, pos, ackwm, fence, ackch, view, handover, isrv, cpisr, cpjoin,
          gfv, gactive, acked, nwrites, nfail>>

Wids(l) == {l[i][1] : i \in 1..Len(l)}
IsPrefix(p, l) == Len(p) <= Len(l) /\ SubSeq(l, 1, Len(p)) = p
Last(l) == l[Len(l)]

\* NDC-style merge of an incoming history snapshot into the local history.
Merge(local, snap) ==
    IF IsPrefix(local, snap) THEN snap
    ELSE IF IsPrefix(snap, local) THEN local
    ELSE IF Last(snap)[2] > Last(local)[2] THEN snap
    ELSE local

Init ==
    /\ logs = [c \in Clusters |-> <<>>]
    /\ tq = [c \in Clusters |-> <<>>]
    /\ pos = [c \in Clusters |-> 0]
    /\ ackwm = [c \in Clusters |-> 0]
    /\ fence = [c \in Clusters |-> 0]
    /\ ackch = [c \in Clusters |-> <<>>]
    /\ view = [c \in Clusters |-> [fv |-> 1, active |-> 0]]
    /\ handover = [c \in Clusters |-> FALSE]
    /\ isrv = [c \in Clusters |-> Clusters]
    /\ cpisr = Clusters
    /\ cpjoin = {}
    /\ gfv = 1
    /\ gactive = 0
    /\ acked = {}
    /\ nwrites = 0
    /\ nfail = 0

Write(c) ==
    /\ view[c].active = c
    /\ ~handover[c]
    /\ (fence[c] <= view[c].fv \/ Mutation = "no-fencing")
    /\ nwrites < MaxWrites
    /\ LET w == nwrites + 1
           l == Append(logs[c], <<w, view[c].fv>>)
       IN /\ logs' = [logs EXCEPT ![c] = l]
          /\ tq' = [tq EXCEPT ![c] = Append(@, [w |-> w, fv |-> view[c].fv, snap |-> l])]
          /\ nwrites' = w
    /\ UNCHANGED <<pos, ackwm, fence, ackch, view, handover, isrv, cpisr, cpjoin, gfv, gactive,
                   acked, nfail>>

Commit(c, i) ==
    LET t == tq[c][i] IN
    /\ t.w \notin acked
    /\ t.w \in Wids(logs[c])
    \* Term rule: only the active, only writes made under its current failover version.
    /\ (Mutation = "no-term-rule" \/ (view[c].active = c /\ t.fv = view[c].fv))
    \* Every other member of c's in-sync set acked it, with a notice not newer than the write.
    /\ \A d \in isrv[c] \ {c} :
          /\ ackwm[c] >= i
          /\ (Mutation = "no-fencing" \/ fence[c] <= t.fv)
    /\ acked' = acked \cup {t.w}
    /\ UNCHANGED <<logs, tq, pos, ackwm, fence, ackch, view, handover, isrv, cpisr, cpjoin,
                   gfv, gactive, nwrites, nfail>>

Read(c) ==
    /\ view[c].active = c
    /\ Len(logs[c]) > 0
    /\ Last(logs[c])[1] \in acked
    /\ ~(Wids(logs[c]) \subseteq acked)
    /\ (Mutation = "no-term-rule" \/ Last(logs[c])[2] = view[c].fv)
    /\ acked' = acked \cup Wids(logs[c])
    /\ UNCHANGED <<logs, tq, pos, ackwm, fence, ackch, view, handover, isrv, cpisr, cpjoin,
                   gfv, gactive, nwrites, nfail>>

Process(c) ==
    /\ pos[c] < Len(tq[Other(c)])
    /\ logs' = [logs EXCEPT ![c] = Merge(@, tq[Other(c)][pos[c] + 1].snap)]
    /\ pos' = [pos EXCEPT ![c] = @ + 1]
    /\ UNCHANGED <<tq, ackwm, fence, ackch, view, handover, isrv, cpisr, cpjoin, gfv, gactive,
                   acked, nwrites, nfail>>

SendAck(c) ==
    LET msg == <<pos[c], IF view[c].active = c THEN view[c].fv ELSE 0>> IN
    /\ Len(ackch[c]) < AckChannelBound
    /\ IF ackch[c] = <<>> THEN TRUE ELSE Last(ackch[c]) # msg
    /\ ackch' = [ackch EXCEPT ![c] = Append(@, msg)]
    /\ UNCHANGED <<logs, tq, pos, ackwm, fence, view, handover, isrv, cpisr, cpjoin, gfv, gactive,
                   acked, nwrites, nfail>>

RecvAck(c) ==
    LET d == Other(c) IN
    /\ ackch[d] # <<>>
    /\ ackwm' = [ackwm EXCEPT ![c] = Max(@, Head(ackch[d])[1])]
    /\ fence' = [fence EXCEPT ![c] = Max(@, Head(ackch[d])[2])]
    /\ ackch' = [ackch EXCEPT ![d] = Tail(@)]
    /\ UNCHANGED <<logs, tq, pos, view, handover, isrv, cpisr, cpjoin, gfv, gactive, acked,
                   nwrites, nfail>>

LoseAck(c) ==
    /\ ackch[c] # <<>>
    /\ ackch' = [ackch EXCEPT ![c] = Tail(@)]
    /\ UNCHANGED <<logs, tq, pos, ackwm, fence, view, handover, isrv, cpisr, cpjoin, gfv, gactive,
                   acked, nwrites, nfail>>

NsSync(c) ==
    /\ view[c] # [fv |-> gfv, active |-> gactive]
    /\ view' = [view EXCEPT ![c] = [fv |-> gfv, active |-> gactive]]
    /\ UNCHANGED <<logs, tq, pos, ackwm, fence, ackch, handover, isrv, cpisr, cpjoin, gfv, gactive,
                   acked, nwrites, nfail>>

StartHandover(c) ==
    /\ view[c].active = c /\ gactive = c /\ ~handover[c]
    /\ handover' = [handover EXCEPT ![c] = TRUE]
    /\ UNCHANGED <<logs, tq, pos, ackwm, fence, ackch, view, isrv, cpisr, cpjoin, gfv, gactive,
                   acked, nwrites, nfail>>

AbortHandover(c) ==
    /\ handover[c]
    /\ handover' = [handover EXCEPT ![c] = FALSE]
    /\ UNCHANGED <<logs, tq, pos, ackwm, fence, ackch, view, isrv, cpisr, cpjoin, gfv, gactive,
                   acked, nwrites, nfail>>

CompleteHandover(c) ==
    /\ handover[c]
    /\ view[c].active = c
    /\ Other(c) \in cpisr
    /\ (ackwm[c] >= Len(tq[c]) \/ Mutation = "handover-without-drain")
    /\ nfail < MaxFailovers
    /\ gfv' = gfv + 1
    /\ gactive' = Other(c)
    /\ view' = [view EXCEPT ![c] = [fv |-> gfv + 1, active |-> Other(c)]]
    /\ handover' = [handover EXCEPT ![c] = FALSE]
    /\ nfail' = nfail + 1
    /\ UNCHANGED <<logs, tq, pos, ackwm, fence, ackch, isrv, cpisr, cpjoin, acked, nwrites>>

ForcedFailover(c) ==
    /\ gactive # c
    /\ (c \in cpisr \/ Mutation = "force-to-any")
    /\ nfail < MaxFailovers
    /\ gfv' = gfv + 1
    /\ gactive' = c
    /\ view' = [view EXCEPT ![c] = [fv |-> gfv + 1, active |-> c]]
    /\ nfail' = nfail + 1
    /\ UNCHANGED <<logs, tq, pos, ackwm, fence, ackch, handover, isrv, cpisr, cpjoin, acked, nwrites>>

\* Control plane drops c: record first, pushed to clusters later.
CpShrink(c) ==
    /\ c \in cpisr /\ c # gactive
    /\ cpisr' = cpisr \ {c}
    /\ UNCHANGED <<logs, tq, pos, ackwm, fence, ackch, view, handover, isrv, cpjoin, gfv, gactive,
                   acked, nwrites, nfail>>

\* Broken variant: the active stops waiting for c before the control plane records it.
ActiveShrinksLocally(c) ==
    /\ Mutation = "active-shrinks-first"
    /\ c # gactive /\ c \in isrv[gactive]
    /\ isrv' = [isrv EXCEPT ![gactive] = @ \ {c}]
    /\ UNCHANGED <<logs, tq, pos, ackwm, fence, ackch, view, handover, cpisr, cpjoin, gfv, gactive,
                   acked, nwrites, nfail>>

PushIsr(c) ==
    /\ isrv[c] # cpisr \cup cpjoin
    /\ isrv' = [isrv EXCEPT ![c] = cpisr \cup cpjoin]
    /\ UNCHANGED <<logs, tq, pos, ackwm, fence, ackch, view, handover, cpisr, cpjoin, gfv, gactive,
                   acked, nwrites, nfail>>

CpRejoinStart(c) ==
    /\ c \notin cpisr /\ c \notin cpjoin
    /\ IF Mutation = "one-step-rejoin"
          THEN cpisr' = cpisr \cup {c} /\ UNCHANGED cpjoin
          ELSE cpjoin' = cpjoin \cup {c} /\ UNCHANGED cpisr
    /\ UNCHANGED <<logs, tq, pos, ackwm, fence, ackch, view, handover, isrv, gfv, gactive, acked,
                   nwrites, nfail>>

\* The member becomes promotable once the active waits for it and it has acked every active task.
CpRejoinFinish(c) ==
    /\ c \in cpjoin
    /\ gactive # c
    /\ c \in isrv[gactive]
    /\ (ackwm[gactive] >= Len(tq[gactive]) \/ Mutation = "rejoin-without-catch-up")
    /\ cpisr' = cpisr \cup {c}
    /\ cpjoin' = cpjoin \ {c}
    /\ UNCHANGED <<logs, tq, pos, ackwm, fence, ackch, view, handover, isrv, gfv, gactive, acked,
                   nwrites, nfail>>

Next ==
    \E c \in Clusters :
        \/ Write(c)
        \/ \E i \in 1..Len(tq[c]) : Commit(c, i)
        \/ Read(c)
        \/ Process(c)
        \/ SendAck(c)
        \/ RecvAck(c)
        \/ LoseAck(c)
        \/ NsSync(c)
        \/ StartHandover(c)
        \/ AbortHandover(c)
        \/ CompleteHandover(c)
        \/ ForcedFailover(c)
        \/ CpShrink(c)
        \/ ActiveShrinksLocally(c)
        \/ PushIsr(c)
        \/ CpRejoinStart(c)
        \/ CpRejoinFinish(c)

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
\* Every acknowledged (or exposed) write is in the history of every promotable cluster.
Durable == \A w \in acked : \A c \in cpisr : w \in Wids(logs[c])

\* ... and in the history of the current active cluster.
OnActive == \A w \in acked : w \in Wids(logs[gactive])

\* Acknowledged writes appear in the same relative order in both histories.
CommonAcked == {w \in acked : w \in Wids(logs[0]) /\ w \in Wids(logs[1])}
Consistent ==
    SelectSeq(logs[0], LAMBDA e : e[1] \in CommonAcked)
        = SelectSeq(logs[1], LAMBDA e : e[1] \in CommonAcked)
=============================================================================
