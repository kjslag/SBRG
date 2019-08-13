# SBRG

An implementation of the Spectrum Bifurcation Renormalization Group (SBRG) ([arXiv:1508.03635](https://arxiv.org/abs/1508.03635)) method in Haskell. The SBRG.hs code was used in the following works: [arXiv:1604.04283](https://arxiv.org/abs/1604.04283) and [arXiv:1611.04058](https://arxiv.org/abs/1611.04058). SBRG2.hs is a new version of the code which uses significantly less memory, supports fermionic Hamiltonians, and allows for turning off spectrum bifurcation for improved performance. Without spectrum bifurcation, the method is similar to [RSRG-X](https://arxiv.org/abs/1307.3253), except new Hamiltonian terms are allowed to be generated during the RG.

Runtime and memory usage are linear (with log corrections) in the system size for an MBL system, and at most quadratic for a marginal MBL system.

Another implementation: https://github.com/EverettYou/SBRG

## Compiling

Compiling SBRG requires GHC and some Haskell libraries. The simplest way to get set up is to install the [Haskell Platform](https://www.haskell.org/platform/). Then install the dependancies using

    $ cabal v2-install --lib  clock hashable ieee754 NumInstances random safe  strict parallel  bitwise

SBRG can then be built from the makefile:

    $ make

This creates three executables:
- SBRG (old version)
- BSBRG2: simulates bosonic models
- FSBRG2: simulates fermionic models
