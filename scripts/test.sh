#!/usr/bin/env bash
set -o errexit -o nounset -o pipefail

BASENAME=$(basename "$0")
CANONICAL_SCRIPT=$(readlink -e "$0")
SCRIPT_DIR=$(dirname "${CANONICAL_SCRIPT}")
ROOT_DIR=$(dirname "${SCRIPT_DIR}")

export EXAHOST=${EXAHOST:-127.0.0.1}
(
    cd "${ROOT_DIR}"
    #TF_LOG=trace
    TF_ACC=true go test ./... -v
)

(
    cd "${ROOT_DIR}/cmd/terraform-provider-exasol"
    go build
    mkdir -p "${ROOT_DIR}/deployments/.terraform/plugins/registry.terraform.io/abergmeier/exasol/0.0.6/linux_amd64"
    mv terraform-provider-exasol "${ROOT_DIR}/deployments/.terraform/plugins/registry.terraform.io/abergmeier/exasol/0.0.6/linux_amd64/terraform-provider-exasol_v0.0.6"
)

(
    cd "${ROOT_DIR}/deployments"
    terraform init
    terraform apply
    terraform destroy
)
