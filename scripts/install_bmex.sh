#!/bin/bash

set -x

opam pin add -n -y uri git://github.com/vbmithr/ocaml-uri#sexp-dev
opam pin add -n -y nocrypto git://github.com/vbmithr/ocaml-nocrypto#sexp-dev
opan pin add -n -y ipaddr git://github.com/vbmithr/ocaml-ipaddr#sexp-dev
opam pin add -n -y conduit git://github.com/vbmithr/ocaml-conduit#async-with-connection
opam pin add -n -y cohttp git://github.com/vbmithr/ocaml-cohttp#sexp-dev
opam pin add -n -y dtc git://github.com/vbmithr/ocaml-dtc
opam pin add -n -y bs-devkit git://github.com/vbmithr/bs_devkit
opam pin add -n -y bs-api git://github.com/vbmithr/bs_api
opam pin add -n -y bmex git://github.com/vbmithr/bmex

opam list --installed depext || opam install -y depext
opam depext -y bmex

opam install --deps-only bmex
