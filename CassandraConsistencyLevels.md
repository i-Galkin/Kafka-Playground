Write Consistency Levels
| Level        | Description                               | Replicas Required | Use Case                                        |
| ------------ | ----------------------------------------- | ----------------- | ----------------------------------------------- |
| ANY          | At least one node (can be hinted handoff) | 1 (even hint)     | Maximum availability, lowest consistency        |
| * ONE        | At least one replica node                 | 1                 | High throughput writes, eventual consistency OK |
| TWO          | At least two replica nodes                | 2                 | Slightly more consistency than ONE              |
| THREE        | At least three replica nodes              | 3                 | More consistency, less used                     |
| * QUORUM     | Majority of replicas across all DCs       | (RF/2) + 1        | Strong consistency, balanced availability       |
| LOCAL_QUORUM | Majority of replicas in local DC only     | (local_RF/2) + 1  | Most common for production                      |
| EACH_QUORUM  | Majority in each DC separately            | Quorum per DC     | Multi-DC strong consistency                     |
| * ALL        | All replicas must acknowledge             | RF                | Strongest consistency, lowest availability      |
| LOCAL_ONE    | One replica in local DC                   | 1                 | Fast local writes                               |

Read Consistency Levels
| Level        | Description                              | Replicas Queried | Use Case                       |
| ------------ | ---------------------------------------- | ---------------- | ------------------------------ |
| * ONE        | Return from first responding replica     | 1                | Fastest reads, may be stale    |
| TWO          | Return from two replicas (most recent)   | 2                | Better consistency than ONE    |
| THREE        | Return from three replicas (most recent) | 3                | Even better consistency        |
| * QUORUM     | Return from majority across all DCs      | (RF/2) + 1       | Strong consistency reads       |
| LOCAL_QUORUM | Return from majority in local DC         | (local_RF/2) + 1 | Most common for production     |
| EACH_QUORUM  | Return from each DC’s majority           | Quorum per DC    | Multi-DC strong reads          |
| * ALL        | Return from all replicas                 | RF               | Strongest consistency, slowest |
| LOCAL_ONE    | Return from one replica in local DC      | 1                | Fast local reads               |
| SERIAL       | Lightweight transaction read             | -                | Read after LWT (Paxos)         |
| LOCAL_SERIAL | Local DC lightweight transaction read    | -                | Local LWT read                 |

