all:
	ocaml pkg/pkg.ml build

.PHONY: clean

clean:
	ocaml pkg/pkg.ml clean

