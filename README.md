# Raft implemented in Rust

My shoddy implementation of the Raft protocol in Rust.

# References

In order of increasing detail:

* [The Secret Lives of Data](http://thesecretlivesofdata.com/raft/) - Gentler visualization
* [Terse Raft visualization](https://raft.github.io/) - Kind of terse interactive visualization
* [In Search of an Understandable Consensus Algorithm](https://raft.github.io/raft.pdf) - Start here @ Figure 2
* [Diego Ongaro's PhD dissertation, Consensus: Bridging Theory and Practice](https://github.com/ongardie/dissertation/blob/master/online.pdf?raw=true) - Straight from the horse's mouth
  * The TLA+ spec is written down in B.2 (pdf page 219)
* [The actual TLA+ spec](https://github.com/ongardie/raft.tla/blob/master/raft.tla)

Of questionable value:

* [My implementation in Rust](https://github.com/makslevental/raft_rust) - Probably jump straight to [node.rs](https://github.com/makslevental/raft_rust/blob/master/src/raft/node.rs)
* [My translation of the TLA+ spec into Python](https://github.com/makslevental/raft_rust/blob/master/raft.py) - doesn't really run but can be read more easily (if you don't know TLA+)

