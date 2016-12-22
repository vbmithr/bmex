#!/usr/bin/env ocaml
#use "topfind"
#require "topkg"
open Topkg

let () =
  Pkg.describe "bmex" @@ fun c ->
  Ok [ (* Pkg.mllib "src/bmex.mllib"; *)
    (* Pkg.test "test/test"; *)
    Pkg.bin ~auto:true "src/bmex" ;
     ]
