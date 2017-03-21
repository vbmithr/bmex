bmex — BitMEX DTC Gateway
-------------------------------------------------------------------------------
%%VERSION%%

bmex is a gateway between the DTC protocol [1] and the BitMEX exchange [2].

bmex is distributed under the ISC license.

Homepage: https://github.com/vbmithr/bmex

## Installation

The instructions are for a debian-like distribution (tested on Debian,
probably working on Ubuntu).

* Install OPAM >= 1.2.2
   * debian stable: See http://opam.ocaml.org/doc/Install.html
   * debian >= testing: apt-get install opam
* Switch to OCaml 4.04.0
   * opam switch 4.04.0
   * eval `opam config env`
   * opam repo add janestreet git://github.com/janestreet/opam-repository
* Run install script
   * ./scripts/install_bmex.sh
* Build the project
   * make
* Run the daemon
   * ./_build/default/src/bmex.exe --help
   * i.e. ./_build/default/src/bmex.exe  -port 5567 -daemon -tls -testnet -loglevel 3

Sierra Chart expect bmex to run on port 5567 with TLS enabled.

## Upgrade

```
$ opam pin remove `opam pin -s`
$ opam update
$ opam upgrade
```

Then continue from *Run install script* above.


[1] http://dtcprotocol.org
[2] http://www.bitmex.com
