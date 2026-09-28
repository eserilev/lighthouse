# The leanSpec `latest` release is rebuilt on every push to leanSpec `main`.
# When the checksum no longer matches, bump both values below to the new release.
LEAN_SPEC_COMMIT := 0b7d33ecbc9ee2435759c92de4da4d08d7faf1c8
LEAN_SPEC_FIXTURES_SHA256 := 21d9de7056b4e658031dc09e50c0e9dc1b0206089253258ce69ecff01154d4bd

BASE_URL := https://github.com/leanEthereum/leanSpec/releases/download/latest
TARBALL := fixtures-prod-scheme.tar.gz
OUTPUT_DIR := ./leanspec-fixtures

.PHONY: all clean

all: clean $(OUTPUT_DIR)

clean:
	rm -rf $(TARBALL) $(OUTPUT_DIR)

$(OUTPUT_DIR):
	curl --progress-bar --location --remote-name --show-error --retry 3 --retry-all-errors --fail \
		$(BASE_URL)/$(TARBALL)
	echo "$(LEAN_SPEC_FIXTURES_SHA256)  $(TARBALL)" | sha256sum --check || \
		(echo "leanSpec fixtures changed upstream, expected leanSpec@$(LEAN_SPEC_COMMIT)"; rm -f $(TARBALL); exit 1)
	mkdir -p $(OUTPUT_DIR)
	tar -xzf $(TARBALL) -C $(OUTPUT_DIR) fixtures/consensus
	rm -f $(TARBALL)
