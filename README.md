### References:
- [Raft paper](https://raft.github.io/raft.pdf)
    Figure 2 is all you need. If you are already familiar with the rest of the paper, that is ;)
- [Student's guide to Raft](https://thesquareplanet.com/blog/students-guide-to-raft/)
    You think you have understood the paper? Think again.
- [Eli bendersky's Raft test harness](https://github.com/eliben/raft/blob/master/part3/raft_test.go)
    My previous attempts at implementing Raft failed due to lack of "proper" tests. So, I decided to use Eli Bendersky's test harness to test my implementation. But the unfortunate consequence of using someone else's test code is that you need to have the same public interface that the tests can interact with. This can limit your choices for your implementation's design.

### Running tests
The OS ports used by tests are hardcoded and are not randomised. So, tests need to be run sequentially.
```
$ cargo t -- --test-threads=1

running 20 tests
test raft_basic ... ok
test raft_commit_multiple_commands ... ok
test raft_commit_one_command ... ok
test raft_commit_with_disconnection_and_recover ... ok
test raft_crash_after_submit ... ok
test raft_crash_follower ... ok
test raft_crash_then_restart_all ... ok
test raft_crash_then_restart_follower ... ok
test raft_crash_then_restart_leader ... ok
test raft_disconnect_all_and_then_restore ... ok
test raft_disconnect_leader_briefly ... ok
test raft_election_disconnect_loop ... ok
test raft_election_leader_and_another_disconnect ... ok
test raft_election_leader_disconnect ... ok
test raft_election_leader_disconnect_then_reconnect ... ok
test raft_election_leader_disconnect_then_reconnect5 ... ok
test raft_no_commit_with_no_quorum ... ok
test raft_replace_multiple_log_entries ... ok
test raft_submit_non_leader_fails ... ok
test raft_test_commits_with_leader_disconnects ... ok

test result: ok. 20 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 107.76s
```