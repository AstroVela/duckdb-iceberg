.PHONY: fixture lakekeeper polaris nessie

PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=iceberg
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# We need this for testing
CORE_EXTENSIONS='httpfs;parquet;tpch'

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

# Run Vane-only targets in a recursive Make invocation so their variables never
# enter DuckDB's upstream extension build.
VANE_EXTENSION_MAKEFILE := $(PROJ_DIR)vane-extension-ci-tools/makefiles/vane_extension.Makefile
VANE_EXTENSION_TARGETS := vane_verify_ci_tools vane_validate vane_prepare vane_identity \
	vane_native vane_ci vane_wheel_dependencies vane_wheel
.PHONY: $(VANE_EXTENSION_TARGETS)

$(VANE_EXTENSION_TARGETS):
	@test -f "$(VANE_EXTENSION_MAKEFILE)" || { \
		printf 'initialize vane-extension-ci-tools before running %s\n' "$@" >&2; \
		exit 2; \
	}
	+$(MAKE) --no-print-directory -f "$(VANE_EXTENSION_MAKEFILE)" "$@" \
		VANE_EXTENSION_ROOT="$(abspath $(PROJ_DIR))"

include make/util.mk
include make/catalogs/fixture.mk
include make/catalogs/lakekeeper.mk
include make/catalogs/nessie.mk
include make/catalogs/polaris.mk

install_requirements:
	python3 -m pip install -r scripts/requirements.txt

# Custom makefile targets
data: data_clean fixture_start
	python3 -m scripts.data_generators.generate_data spark-rest local

data_large: data data_clean
	python3 -m scripts.data_generators.generate_data spark-rest local

data_clean:
	rm -rf data/generated
