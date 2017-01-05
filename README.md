bmex — BitMEX DTC Gateway
-------------------------------------------------------------------------------
%%VERSION%%

bmex is a gateway between the DTC protocol [1] and the BitMEX exchange [2].

bmex is distributed under the ISC license.

Homepage: https://github.com/vbmithr/bmex

## Installation

The instructions are for a debian-like distribution (tested on Debian,
probably working on Ubuntu).

* Install OCaml: apt-get install ocaml-nox
* Install OPAM version >= 1.2.2:
   * debian >= testing: apt-get install opam
   * other: http://opam.ocaml.org/doc/Install.html#Binarydistribution
* Switch to OCaml 4.03.0
   * opam switch 4.03.0
   * eval `opam config env`
   * opam repo add janestreet git://github.com/janestreet/opam-repository
* Run install script
   * ./scripts/install_bmex.sh
* Build the project
   * make

[1] http://dtcprotocol.org
[2] http://www.bitmex.com
