# 0002 — Azure credit VM instead of Oracle Always Free

- Status: accepted
- Date: 2026-10-27

## Context

Phase 1 compute was an Oracle Always Free VM. Oracle signup requires a payment card. The operator has an Azure subscription with a $100 credit and will not add a card to Oracle.

The project constraint is still local-first for the lake: no paid GCS, Azure Blob, or AWS S3. Oracle was only the virtual machine, not the lake or the warehouse.

## Decision

- Phase 1 compute is **one Ubuntu VM on Azure**, created by Terraform in `zoomcamp/terraform/` (`hashicorp/azurerm`).
- The VM is paid from the **$100 Azure credit**. The credit expires and is a balance. Deallocate the VM when idle. `terraform destroy` when the demo is finished.
- Default size is `Standard_B2s` (2 vCPU, 4 GiB). `Standard_B2ms` is the larger fallback if Airflow runs out of memory.
- SSH port 22 is open to the operator IP only. Airflow and MinIO are reached through an SSH tunnel.
- The **lake stays MinIO** on that VM. Do not add Azure Blob, Azure SQL, or other Azure data services.
- The **warehouse target stays MotherDuck**. This decision does not build that connection.
- Sign-in is `az login`. No Azure client secret in git.

## Consequences

- `terraform apply` spends the credit. A VM left running can use the $100 in a few weeks.
- Applied 2026-10-28 in `austriaeast` zone 1 as `Standard_B2s_v2`. The student region policy rejects `uksouth`, and `Standard_B2s` had no capacity in the allowed regions. Code defaults stay `uksouth` / `Standard_B2s`; `terraform.tfvars` overrides them.
- Peer reviewers see Terraform plus an Azure VM, not an Oracle Always Free instance. The README must say the credit is finite.
- ARM shapes are out of scope so the existing x86 Docker images keep working.
