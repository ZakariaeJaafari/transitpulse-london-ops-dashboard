# TransitPulse — Azure credit VM (Terraform)

Provisions one Ubuntu VM for the graded `zoomcamp/` stack. It draws a **$100 Azure credit**. That credit is a balance with an expiry, not a forever-free machine.

**SSH (22) from your IP only.** Airflow (`8080`) and MinIO (`9000`/`9001`) are not on the public security group. Tunnel them after login.

The lake stays **MinIO on the VM**. This stack does not create Azure Blob Storage, Azure SQL, or any other data service. The warehouse target is still **MotherDuck**.

Secrets: `terraform.tfvars` is gitignored at the repo root. Sign in with the Azure CLI. Do not put a client secret in the repo.

## Prerequisites

1. An Azure subscription with the $100 credit.
2. [Azure CLI](https://learn.microsoft.com/cli/azure/install-azure-cli) and [Terraform](https://developer.hashicorp.com/terraform/install).
3. An SSH key pair. Only the public line goes in `terraform.tfvars`.

## Configure

```bash
az login
az account show --query "{name:name, id:id}" -o table
```

Use that subscription id below.

```bash
cd zoomcamp/terraform
cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars — subscription_id, location, ssh_allowed_cidr, ssh_public_key
```

## Validate (no cloud changes)

```bash
terraform init
terraform validate
```

## Plan / apply (spends the credit)

```bash
terraform plan
terraform apply
```

`plan` shows what would be created. `apply` starts billing the credit for the VM, the 30 GiB SSD, and the public IP.

Current apply: `austriaeast` zone 1, size `Standard_B2s_v2` (set in `terraform.tfvars`). The security group is attached to the public subnet. Docker is not on the VM yet.

After apply, note `public_ip` and use `ssh_command` from the outputs. Tunnel Airflow and MinIO:

```bash
ssh -i ~/.ssh/your_key -L 8080:localhost:8080 -L 9001:localhost:9001 ubuntu@<public_ip>
```

## Stop the spend

Deallocate when you are not using the VM. Compute charges stop. The disk and public IP can still draw a small amount.

```bash
az vm deallocate --resource-group transitpulse-rg --name transitpulse-zoomcamp
```

Start again with `az vm start --resource-group transitpulse-rg --name transitpulse-zoomcamp`.

`terraform destroy` from this folder removes the group and stops the remaining charges.

See [docs/cloud-and-free-demo.md](../../docs/cloud-and-free-demo.md).
