#!/bin/bash

set -x

opam pin add -n -y --dev-repo uri
opam pin add -n -y --dev-repo ipaddr
opam pin add -n -y --dev-repo conduit

opam pin add -n -y nocrypto git://github.com/vbmithr/ocaml-nocrypto#sexp-dev
opam pin add -n -y cohttp git://github.com/vbmithr/ocaml-cohttp#sexp-dev
opam pin add -n -y dtc git://github.com/vbmithr/ocaml-dtc
opam pin add -n -y bs-devkit git://github.com/vbmithr/bs_devkit#no-bin-prot
opam pin add -n -y bs-api git://github.com/vbmithr/bs_api
opam pin add -n -y bmex git://github.com/vbmithr/bmex

opam list --installed depext || opam install -y depext
opam depext -y bmex

opam install --deps-only bmex
