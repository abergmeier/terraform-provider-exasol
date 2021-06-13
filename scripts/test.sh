#!/usr/bin/env bash
set -o errexit -o nounset -o pipefail

BASENAME=$(basename "$0")
CANONICAL_SCRIPT=$(readlink -e "$0")
SCRIPT_DIR=$(dirname "${CANONICAL_SCRIPT}")
ROOT_DIR=$(dirname "${SCRIPT_DIR}")

export EXAHOST=${EXAHOST:-127.0.0.1}
export TF_CLI_CONFIG_FILE=/tmp/dev.tfrc

cat <<EOF > "$TF_CLI_CONFIG_FILE"
provider_installation {

  # Use /home/developer/tmp/terraform-null as an overridden package directory
  # for the hashicorp/null provider. This disables the version and checksum
  # verifications for this provider and forces Terraform to look for the
  # null provider plugin in the given directory.
  dev_overrides {
    "abergmeier/exasol" = "${ROOT_DIR}/cmd/terraform-provider-exasol"
  }

  # For all other providers, install them directly from their origin provider
  # registries as normal. If you omit this, Terraform will _only_ use
  # the dev_overrides block, and so no other providers will be available.
  direct {}
}
EOF

(
    cd "${ROOT_DIR}"
    #TF_LOG=trace
    TF_ACC=true go test ./... -v
)

(
    cd "${ROOT_DIR}/cmd/terraform-provider-exasol"
    go build
)

(
    cd "${ROOT_DIR}/deployments"
    terraform init
    terraform apply
    terraform destroy
)
