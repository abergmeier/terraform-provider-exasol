#!/usr/bin/env bash
set -o errexit -o nounset -o pipefail

BASENAME=$(basename "$0")
CANONICAL_SCRIPT=$(readlink -e "$0")
SCRIPT_DIR=$(dirname "${CANONICAL_SCRIPT}")
ROOT_DIR=$(dirname "${SCRIPT_DIR}")

(
    cd "${ROOT_DIR}"
    go test ./...
)

(
    cd "${ROOT_DIR}/cmd/terraform-provider-exasol"
    go build
    mv terraform-provider-exasol "${ROOT_DIR}/deployment/terraform-provider-exasol"
)

(
    cd "${ROOT_DIR}/deployment"
    terraform init
    terraform apply
    terraform destroy
)
