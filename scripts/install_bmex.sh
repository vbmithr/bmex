#!/bin/bash

set -x

opam pin add -n -y --dev-repo wamp
opam pin add -n -y scid git://github.com/vbmithr/ocaml-scid
opam pin add -n -y dtc git://github.com/vbmithr/ocaml-dtc
opam pin add -n -y bs-devkit git://github.com/vbmithr/bs_devkit
opam pin add -n -y bs-api git://github.com/vbmithr/bs_api
opam pin add -n -y bmex git://github.com/vbmithr/bmex

opam list --installed depext || opam install -y depext
opam depext -y bmex

opam install --deps-only bmex
