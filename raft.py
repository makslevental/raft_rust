# tla -> raft
import pickle
from pprint import pprint
from random import randrange

import itertools
## python hacks to emulate some TLA stuff
import time


def _(*items):
    old_pickle = pickle.dumps(items)

    def check(*new_items):
        return old_pickle == pickle.dumps(new_items)

    return check


def SubSeq(ls, j, k):
    return ls[j:k + 1]


class Dict(dict):
    def __getattr__(self, item):
        return self[item]

    def __hash__(self):
        return hash(str(self))

    def __getstate__(self):
        return dict(self)


### Raft TLA

## Constants

NSERVERS = 3

# The set of server IDs
Server = {i for i in range(3)}

# The set of requests that can go into the log
Value = {"A", "B", "C", "D"}

# Server states
Follower = "Follower"
Candidate = "Candidate"
Leader = "Leader"

# Message types
RequestVoteRequest = "RequestVoteRequest"
RequestVoteResponse = "RequestVoteResponse"
AppendEntriesRequest = "AppendEntriesRequest"
AppendEntriesResponse = "AppendEntriesResponse"

## Global variables

# A bag of records representing requests and responses sent from one server to another
messages = ...
# A history variable used in the proof. This would not be present in an
# implementation.
# Keeps track of successful elections, including the initial logs of the
# leader and voters' logs. Set of functions containing various things about
# successful elections (see BecomeLeader).
elections = ...
# THIS IS MY OWN HACK
timeouts = ...
# A history variable used in the proof. This would not be present in an
# implementation.
# Keeps track of every log ever in the system (set of logs).
allLogs = ...

## The following variables are all per server (i.e. functions with domain Server)

# The server's term number
currentTerm = ...
# The server's state (Follower, Candidate, or Leader)
state = ...
# The candidate the server voted for in its current term, or None if it hasn't voted for any.
votedFor = ...
serverVars = currentTerm, state, votedFor

# A Sequence of log entries. The index into this sequence is the index of the log entry.
log = ...
# The index of the latest entry in the log the state machine may apply
commitIndex = ...
logVars = log, commitIndex

## The following variables are used only on candidates:

# The set of servers from which the candidate has received a RequestVote response in its currentTerm
votesResponded = ...
# The set of servers from which the candidate has received a vote in its currentTerm
votesGranted = ...
# A history variable used in the proof. This would not be present in an implementation.
# Function from each server that voted for this candidate in its currentTerm to that voter's log.
voterLog = ...
candidateVars = votesResponded, votesGranted, voterLog

## The following variables are used only on leaders:

# The next entry to send to each follower
nextIndex = ...
# The latest entry that each follower has acknowledged is the same as the
# leader's. This is used to calculate commitIndex on the leader.
matchIndex = ...
leaderVars = nextIndex, matchIndex, elections

# All variables; used for stuttering (asserting state hasn't changed)
vars = messages, allLogs, currentTerm, state, votedFor, votesResponded, votesGranted, voterLog, nextIndex, matchIndex, elections, log, commitIndex

## Helpers

# The set of all quorums. This just calculates simple majorities, but the only
# important property is that every quorum overlaps with every other
Quorum = set(
    frozenset(q) for n in range(len(Server) // 2 + 1, len(Server) + 1) for q in itertools.combinations(Server, n))

# The term of the last entry in a log, or 0 if the log is empty
LastTerm = lambda xlog: 0 if len(xlog) == 0 else xlog[len(xlog) - 1].term


# Helper for Send and Reply. Given a message m and bag of messages, return a
# new bag of messages with one more m in it.
def WithMessage(m, msgs):
    if m in msgs:
        msgs[m] += 1
    else:
        msgs[m] = 1
    return msgs


# Helper for Discard and Reply. Given a message m and bag of messages, return
# a new bag of messages with one less m in it.
def WithoutMessage(m, msgs):
    if m in msgs:
        msgs[m] -= 1
    return msgs


# Add a message to the bag of messages
def Send(m):
    print("send message", m)
    global messages
    messages = WithMessage(m, messages)


# Remove a message from the bag of messages. Used when a server is done
# processing a message
def Discard(m):
    global messages
    messages = WithoutMessage(m, messages)


# Combination of Send and Discard
def Reply(response, request):
    global messages
    messages = WithoutMessage(request, WithMessage(response, messages))


## Define initial values for all variables

def Init():
    global elections, timeouts, allLogs, voterLog, currentTerm, state, votedFor, votesResponded, votesGranted, nextIndex, matchIndex, log, commitIndex, messages

    elections = set()
    timeouts = {i: Dict(timeout=randrange(150, 300) / 1000, previous_time=time.time()) for i in Server}
    allLogs = set()
    voterLog = {i: {} for i in Server}

    currentTerm = {i: 0 for i in Server}
    state = {i: Follower for i in Server}
    votedFor = {i: None for i in Server}

    votesResponded = {i: set() for i in Server}
    votesGranted = {i: set() for i in Server}

    # The values nextIndex[i][i] and matchIndex[i][i] are never read, since the
    # leader does not send itself messages. It's still easier to include these
    # in the functions.
    nextIndex = {i: {j: 0} for i in Server for j in Server}
    matchIndex = {i: {j: -1} for i in Server for j in Server}
    log = {i: () for i in Server}
    commitIndex = {i: 0 for i in Server}

    messages = {}


## Define state transitions

# Server i restarts from stable storage.
# It loses everything but its currentTerm, votedFor, and log.
def Restart(i):
    global state, votesResponded, votesGranted, voterLog, nextIndex, matchIndex, commitIndex

    UNCHANGED = _(messages, currentTerm, votedFor, log, elections)

    state[i] = Follower
    votesResponded[i] = set()
    votesGranted[i] = set()
    voterLog[i] = {}
    nextIndex[i] = {j: 0 for j in Server}
    matchIndex[i] = {j: -1 for j in Server}
    commitIndex[i] = 0

    assert UNCHANGED(messages, currentTerm, votedFor, log, elections)


# Server i times out and starts a new election.
def Timeout(i):
    global state, currentTerm, votedFor, votesResponded, votesGranted, voterLog

    UNCHANGED = _(messages, nextIndex, matchIndex, elections, log, commitIndex)

    if state[i] in {Follower, Candidate} and time.time() - timeouts[i].previous_time > timeouts[i].timeout:
        timeouts[i].previous_time = time.time()
        state[i] = Candidate
        currentTerm[i] = currentTerm[i] + 1
        votedFor[i] = None
        votesResponded[i] = set()
        votesGranted[i] = set()
        voterLog[i] = {}

    assert UNCHANGED(messages, nextIndex, matchIndex, elections, log, commitIndex)


# Candidate i sends j a RequestVote request.
def RequestVote(i, j):
    UNCHANGED = _(currentTerm, state, votedFor, votesResponded, votesGranted, voterLog, nextIndex, matchIndex,
                  elections, log, commitIndex)

    if state[i] == Candidate and j not in votesResponded[i]:
        Send(Dict(
            mtype=RequestVoteRequest,
            mterm=currentTerm[i],
            mlastLogTerm=LastTerm(log[i]),
            mlastLogIndex=len(log[i]) - 1,
            msource=i,
            mdest=j
        ))

    assert UNCHANGED(currentTerm, state, votedFor, votesResponded, votesGranted, voterLog, nextIndex, matchIndex,
                     elections, log, commitIndex)


# Leader i sends j an AppendEntries request containing up to 1 entry.
# While implementations may want to send more than 1 at a time, this spec uses
# just 1 because it minimizes atomic regions without loss of generality.
def AppendEntries(i, j):
    UNCHANGED = _(currentTerm, state, votedFor, votesResponded, votesGranted, voterLog, nextIndex, matchIndex,
                  elections, log, commitIndex)

    if i != j and state[i] == Leader:
        prevLogIndex = nextIndex[i][j] - 1
        prevLogTerm = log[i][prevLogIndex].term if prevLogIndex > 0 else 0
        # Send up to 1 entry, constrained by the end of the log.
        lastEntry = min(len(log[i]) - 1, nextIndex[i][j])
        entries = SubSeq(log[i], nextIndex[i][j], lastEntry)

        Send(Dict(
            mtype=AppendEntriesRequest,
            mterm=currentTerm[i],
            mprevLogIndex=prevLogIndex,
            mprevLogTerm=prevLogTerm,
            mentries=entries,
            # mlog is used as a history variable for the proof.
            # It would not exist in a real implementation.
            mlog=log[i],
            mcommitIndex=min(commitIndex[i], lastEntry),
            msource=i,
            mdest=j
        ))

    assert UNCHANGED(currentTerm, state, votedFor, votesResponded, votesGranted, voterLog, nextIndex, matchIndex,
                     elections, log, commitIndex)


# Candidate i transitions to leader.
def BecomeLeader(i):
    global state, nextIndex, matchIndex, elections

    UNCHANGED = _(messages, currentTerm, votedFor, votesResponded, votesGranted, voterLog, log, commitIndex)

    if state[i] == Candidate and votesGranted[i] in Quorum:
        state[i] = Leader
        nextIndex[i] = {j: len(log[i]) for j in Server}
        matchIndex[i] = {j: -1 for j in Server}
        elections = elections | {Dict(
            eterm=currentTerm[i],
            eleader=i,
            elog=log[i],
            evotes=votesGranted[i],
            evoterlog=voterLog[i],
        )}

    assert UNCHANGED(messages, currentTerm, votedFor, votesResponded, votesGranted, voterLog, log, commitIndex)


# Leader i receives a client request to add v to the log.
def ClientRequest(i, v):
    UNCHANGED = _(messages, currentTerm, state, votedFor, votesResponded, votesGranted, voterLog, nextIndex, matchIndex,
                  elections, commitIndex)

    if state[i] == Leader:
        entry = Dict(term=currentTerm[i], value=v)
        log[i] = log[i] + (entry,)

    assert UNCHANGED(messages, currentTerm, state, votedFor, votesResponded, votesGranted, voterLog, nextIndex,
                     matchIndex,
                     elections, commitIndex)


# Leader i advances its commitIndex.
# This is done as a separate step from handling AppendEntries responses,
# in part to minimize atomic regions, and in part so that leaders of
# single-server clusters are able to mark entries committed.
def AdvanceCommitIndex(i):
    UNCHANGED = _(messages, currentTerm, state, votedFor, votesResponded, votesGranted, voterLog, nextIndex, matchIndex,
                  elections, log)

    if state[i] == Leader:
        # The set of servers that agree up through index.
        Agree = lambda index: {i} | {k for k in Server if matchIndex[i][k] >= index}
        # The maximum indexes for which a quorum agrees
        agreeIndexes = {index for index in range(-1, len(log[i])) if Agree(index) in Quorum}
        # New value for commitIndex'[i]
        if agreeIndexes != {} and agreeIndexes != {-1} and log[i][max(agreeIndexes)].term == currentTerm[i]:
            newCommitIndex = max(agreeIndexes)
        else:
            newCommitIndex = commitIndex[i]
        commitIndex[i] = newCommitIndex

    assert UNCHANGED(messages, currentTerm, state, votedFor, votesResponded, votesGranted, voterLog, nextIndex,
                     matchIndex,
                     elections, log)


## Message handlers
# i = recipient, j = sender, m = message

# Server i receives a RequestVote request from server j with
# m.mterm <= currentTerm[i].
def HandleRequestVoteRequest(i, j, m):
    global votedFor

    UNCHANGED_votedFor = _(votedFor)
    UNCHANGED = _(state, currentTerm, votesResponded, votesGranted, voterLog, nextIndex, matchIndex, elections, log,
                  commitIndex)

    logOk = (m.mlastLogTerm > LastTerm(log[i])) or (
            m.mlastLogTerm == LastTerm(log[i]) and m.mlastLogIndex >= len(log[i]) - 1)
    grant = m.mterm == currentTerm[i] and logOk and votedFor[i] in {None, j}
    if m.mterm <= currentTerm[i]:
        if grant:
            votedFor[i] = j
        else:
            assert UNCHANGED_votedFor(votedFor)
    Reply(Dict(
        mtype=RequestVoteResponse,
        mterm=currentTerm[i],
        mvoteGranted=grant,
        # mlog is used just for the `elections' history variable for
        # the proof. It would not exist in a real implementation.
        mlog=log[i],
        msource=i,
        mdest=j
    ), m)

    UNCHANGED(state, currentTerm, votesResponded, votesGranted, voterLog, nextIndex, matchIndex, elections, log,
              commitIndex)


# Server i receives a RequestVote response from server j with
# m.mterm = currentTerm[i].
def HandleRequestVoteResponse(i, j, m):
    UNCHANGED_votes = _(votesGranted, voterLog)
    UNCHANGED = _(currentTerm, state, votedFor, nextIndex, matchIndex, elections, log, commitIndex)

    if m.mterm == currentTerm[i]:
        votesResponded[i] = votesResponded[i] | {j}

        if m.mvoteGranted:
            votesGranted[i] = votesGranted[i] | {j}
            voterLog[i][j] = m.mlog
        else:
            assert UNCHANGED_votes(votesGranted, voterLog)

    Discard(m)

    assert UNCHANGED(currentTerm, state, votedFor, nextIndex, matchIndex, elections, log, commitIndex)


# Server i receives an AppendEntries request from server j with
# m.mterm <= currentTerm[i]. This just handles m.entries of length 0 or 1, but
# implementations could safely accept more by treating them the same as
# multiple independent requests of 1 entry.
def HandleAppendEntriesRequest(i, j, m):
    global state, commitIndex, log

    UNCHANGED_serverVars = _(currentTerm, state, votedFor)
    UNCHANGED_logVars = _(log, commitIndex)
    UNCHANGED_follower = _(currentTerm, votedFor, messages)
    UNCHANGED_log = _(log)
    UNCHANGED_conflict = _(commitIndex, messages)
    UNCHANGED = _(votesResponded, votesGranted, voterLog, nextIndex, matchIndex, elections)

    logOk = (m.mprevLogIndex == -1) or (
            m.mprevLogIndex > 0 and
            m.mprevLogIndex < len(log[i]) and
            m.mprevLogTerm == log[i][m.mprevLogIndex].term)

    if m.mterm <= currentTerm[i]:
        # reject request
        if m.mterm < currentTerm[i] or (m.mterm == currentTerm[i] and state[i] == Follower and not logOk):
            Reply(Dict(
                mtype=AppendEntriesResponse,
                mterm=currentTerm[i],
                msuccess=False,
                mmatchIndex=0,
                msource=i,
                mdest=j
            ), m)

            assert UNCHANGED_serverVars(currentTerm, state, votedFor) and UNCHANGED_logVars(log, commitIndex)

        # return to follower state
        if m.mterm == currentTerm[i] and state[i] == Candidate:
            state[i] = Follower

            assert UNCHANGED_follower(currentTerm, votedFor, messages) and UNCHANGED_logVars(log, commitIndex)

        # accept request
        if m.mterm == currentTerm[i] and state[i] == Follower and logOk:
            index = m.mprevLogIndex + 1

            # already done with request
            if m.mentries != [] and len(log[i]) - 1 >= index and log[i][index].term == m.mentries[0].term:
                # This could make our commitIndex decrease (for
                # example if we process an old, duplicated request),
                # but that doesn't really affect anything.
                commitIndex[i] = m.mcommitIndex
                Reply(Dict(
                    mtype=AppendEntriesResponse,
                    mterm=currentTerm[i],
                    msuccess=True,
                    mmatchIndex=m.mprevLogIndex + len(m.mentries),
                    msource=i,
                    mdest=j
                ), m)

                assert UNCHANGED_serverVars(currentTerm, state, votedFor) and UNCHANGED_log(log)

            # conflict: remove 1 entry
            if m.mentries != [] and len(log[i]) - 1 >= index and log[i][index].term != m.mentries[0].term:
                new = (log[i][index2] for index2 in range(len(log[i]) - 1))
                log[i] = new

                assert UNCHANGED_serverVars(currentTerm, state, votedFor) and UNCHANGED_conflict(commitIndex, messages)

            # no conflict: append entry
            if len(m.mentries) and len(log[i]) - 1 == m.mprevLogIndex:
                log[i] = log[i] + (m.mentries[0],)

                assert UNCHANGED_serverVars(currentTerm, state, votedFor) and UNCHANGED_conflict(commitIndex, messages)

        assert UNCHANGED(votesResponded, votesGranted, voterLog, nextIndex, matchIndex, elections)


# Server i receives an AppendEntries response from server j with
# m.mterm = currentTerm[i].
def HandleAppendEntriesResponse(i, j, m):
    global nextIndex, matchIndex

    UNCHANGED_matchIndex = _(matchIndex)
    UNCHANGED = _(currentTerm, state, votedFor, votesResponded, votesGranted, voterLog, logVars, log, commitIndex,
                  elections)

    if m.mterm == currentTerm[i]:
        if m.msuccess:
            nextIndex[i][j] = m.mmatchIndex + 1
            matchIndex[i][j] = m.mmatchIndex
        else:
            nextIndex[i][j] = max(nextIndex[i][j] - 1, 1)
            assert UNCHANGED_matchIndex(matchIndex)

    Discard(m)

    assert UNCHANGED(currentTerm, state, votedFor, votesResponded, votesGranted, voterLog, logVars, log, commitIndex,
                     elections)


# Any RPC with a newer term causes the recipient to advance its term first.
def UpdateTerm(i, j, m):
    global currentTerm, state, votedFor

    UNCHANGED = _(messages, votesResponded, votesGranted, voterLog, nextIndex, matchIndex, elections, log, commitIndex)

    if m.mterm > currentTerm[i]:
        currentTerm[i] = m.mterm
        state[i] = Follower
        votedFor[i] = None

    # messages is unchanged so m can be processed further.
    assert UNCHANGED(messages, votesResponded, votesGranted, voterLog, nextIndex, matchIndex, elections, log,
                     commitIndex)


# Responses with stale terms are ignored.
def DropStaleResponse(i, j, m):
    UNCHANGED = _(currentTerm, state, votedFor, votesResponded, votesGranted, voterLog, nextIndex, matchIndex,
                  elections, log, commitIndex)

    if m.mterm < currentTerm[i]:
        Discard(m)

    assert UNCHANGED(currentTerm, state, votedFor, votesResponded, votesGranted, voterLog, nextIndex, matchIndex,
                     elections, log, commitIndex)


# Receive a message.
def Receive(m):
    i = m.mdest
    j = m.msource

    # Any RPC with a newer term causes the recipient to advance
    # its term first. Responses with stale terms are ignored.
    UpdateTerm(i, j, m)

    if m.mtype == RequestVoteRequest:
        HandleRequestVoteRequest(i, j, m)
    elif m.mtype == RequestVoteResponse:
        DropStaleResponse(i, j, m)
        HandleRequestVoteResponse(i, j, m)
    elif m.mtype == AppendEntriesRequest:
        HandleAppendEntriesRequest(i, j, m)
    elif m.mtype == AppendEntriesResponse:
        DropStaleResponse(i, j, m)
        HandleAppendEntriesResponse(i, j, m)


## End of message handlers.

## Network state transitions

def DuplicateMessage(m):
    UNCHANGED = _(currentTerm, state, votedFor, votesResponded, votesGranted, voterLog, nextIndex, matchIndex,
                  elections, log, commitIndex)

    Send(m)

    assert UNCHANGED(currentTerm, state, votedFor, votesResponded, votesGranted, voterLog, nextIndex, matchIndex,
                     elections, log, commitIndex)


# The network drops a message
def DropMessage(m):
    UNCHANGED = _(currentTerm, state, votedFor, votesResponded, votesGranted, voterLog, nextIndex, matchIndex,
                  elections, log, commitIndex)

    Discard(m)

    assert UNCHANGED(currentTerm, state, votedFor, votesResponded, votesGranted, voterLog, nextIndex, matchIndex,
                     elections, log, commitIndex)


def Next():
    global allLogs

    for round in range(100):
        print("round ", round)
        if round == 0:
            for i in Server:
                Restart(i)

        for i in Server:
            Timeout(i)

        for i in Server:
            for j in Server:
                if i == j: continue

                RequestVote(i, j)

        for i in Server:
            BecomeLeader(i)

        for i in Server:
            for v in Value:
                ClientRequest(i, v)

        for i in Server:
            AdvanceCommitIndex(i)

        for i in Server:
            for j in Server:
                if i == j: continue

                AppendEntries(i, j)

        for m in list(messages.keys()):
            if messages[m] > 0:
                Receive(m)

        for m in list(messages.keys()):
            if messages[m] > 0:
                DuplicateMessage(m)

        for m in list(messages.keys()):
            if messages[m] > 0:
                DropMessage(m)

        # allLogs = allLogs | {log[i] for i in Server}

        time.sleep(0.001)

    pprint(log)


if __name__ == '__main__':
    Init()
    Next()
