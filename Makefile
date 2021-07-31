GOPATH:=$(shell pwd)
GO:=go
GOFLAGS:=-v -p 1

default: bin/benchmark_db

all: clean default

version/version.go: .version
	sh -c 'mkdir -p version && ./bin/version.sh .version >$@'

bin/benchmark_db: version/version.go
	@echo "========== Compiling artifacts for: $@ =========="
	sh -c '$(GO) build $(GOFLAGS) -o $@ github.com/hartsp2000/benchmark_db'

clean:
	@echo "========== Cleaning artifacts =========="
	@echo "Deleting generated binary files ..."; for binary in bin/benchmark_db ; do if [ -f "$${binary}" ]; then rm -f $${binary} && echo $${binary}; fi; done
	@echo "Deleting generated version files ..."; for version_dir in /version ; do if [ -d "version" ]; then rm -Rf version && echo version; fi; done
	echo "Deleting backup files: "
	find . -name \*~ -exec rm -f {}  \;
