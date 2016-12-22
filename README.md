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
* Install OPAM (git version):
   * git clone git://github.com/ocaml/opam
   * ./configure && make lib-ext && make && sudo make install
* Run install script (TODO)
   * ./install_bmex.sh (TODO)

[1] http://dtcprotocol.org
[2] http://www.bitmex.com
