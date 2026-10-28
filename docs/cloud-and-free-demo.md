# TransitPulse — Cloud VM + Free Public Demo

Visual A→Z flows: **[diagrams.md](./diagrams.md)**

How to run the course stack **on an Azure VM** created with Terraform from a **$100 credit**, and how to put a lightweight demo on the internet with **GitHub + Streamlit Cloud + MotherDuck** (no always-on Spark/Flink required).

Decision: [0002-azure-credit-vm.md](./decisions/0002-azure-credit-vm.md).

---

## What we ship for cloud

- **Laptop only** is not the cloud story.
- **MotherDuck alone** may or may not convince a peer reviewer that the *project* is in the cloud.
- **This project:** one **Azure VM** created with **Terraform**, running the **`zoomcamp/`** stack (or at least Airflow + dashboard), documented in the README. The $100 credit pays for that VM. FastAPI is extras and is not required on the VM.
- The **lake stays MinIO** on the VM. Do not add Azure Blob Storage, Azure SQL, or other Azure data services.

---

# Part A — Azure VM ($100 credit)

## Why Azure

- The account already has a **$100 credit**. Oracle Always Free asked for a payment card, so that path is dropped.
- Docs: https://learn.microsoft.com/azure/cost-management-billing/manage/spending-limit
- Signup / portal: https://portal.azure.com
- What Terraform creates:
  - One resource group
  - One VNet, subnet, and network security group
  - One public IP
  - One Ubuntu 24.04 VM, default size `Standard_B2s` (2 vCPU, 4 GiB)

**As-built (2026-10-28):** applied in `austriaeast` zone 1 as `Standard_B2s_v2` (2 vCPU, 8 GiB). The student subscription blocks `uksouth`, and `Standard_B2s` had no capacity in the allowed regions. `vm_size` and `location` in gitignored `terraform.tfvars` override the code defaults. The NSG is associated with subnet `transitpulse-public`. Docker is not installed on the VM yet.

**Caveats (honest):**

- The credit is a **balance with an expiry**, not a machine that stays free forever. A VM left running can use the $100 in a few weeks.
- **Deallocate** the VM when you are not using it. `terraform destroy` when the demo is done. The OS disk and public IP can still draw a little credit while the VM is deallocated.
- `Standard_B2s` is tight for Airflow. If the container is killed, set `vm_size` to `Standard_B2ms` (8 GiB) and apply again. That size spends the credit faster.
- Stay on **x86**. ARM sizes would force a rebuild of the Docker images.
- Full Airflow + Spark + Flink + Redpanda + MinIO on **one** small VM is too much — see “What to run on the VM” below.

## Step-by-step (CLI login, then Terraform)

### 1. Confirm the credit

1. Sign in at https://portal.azure.com.
2. Open **Cost Management + Billing** and note the remaining credit and the expiry date.
3. Note the subscription id (`az account show`).

### 2. Networking (created by Terraform)

Terraform builds the VNet. The security group allows:

- `22` SSH from **your IP** only

It does **not** open `8080`, `8501`, `9000`, or `9001`. Use an SSH tunnel for Airflow and MinIO. Streamlit can be tunneled the same way (`8501`) when you want to look at it. Do not add those ports to the security group for a public demo.

### 3. Launch the VM

From `zoomcamp/terraform/` after `terraform.tfvars` is filled:

```bash
az login
terraform init
terraform plan
terraform apply
```

Image: Ubuntu 24.04 LTS (`Canonical:ubuntu-24_04-lts:server`). SSH user: `ubuntu`.

```bash
ssh -i ~/.ssh/your_key ubuntu@<PUBLIC_IP>
```

### 4. Bootstrap the VM

```bash
sudo apt update && sudo apt install -y docker.io docker-compose-v2 git
sudo usermod -aG docker $USER   # log out/in after
git clone https://github.com/<you>/transitpulse.git
cd transitpulse/zoomcamp
cp .env.example .env            # fill secrets on the VM only
docker compose up -d
```

### 5. What to run on the VM (pragmatic)

| Mode | Services on VM | Notes |
|------|----------------|-------|
| **Full** | Entire **`zoomcamp/`** Compose | Only if it fits RAM. Spark and Flink will not fit the default size. |
| **Lean (recommended)** | Airflow + **dlt** + MinIO + Streamlit; keep Flink/Spark as local screenshots | Still “in the cloud” + Terraform |
| **Demo-only** | Streamlit talking to MotherDuck | Weak alone for “pipeline in cloud”; pair with Part B + document batch/stream ran in CI/local. FastAPI (`extras/`) is optional and not graded. |

Point the warehouse at **MotherDuck** from the VM (outbound HTTPS) when that step is built. The lake on the VM is MinIO, not Azure Blob.

### 6. Stop the credit drain

```bash
az vm deallocate --resource-group transitpulse-rg --name transitpulse-zoomcamp
```

Start later with `az vm start` on the same names. `terraform destroy` removes the group.

---

## Terraform (IaC)

Put under `zoomcamp/terraform/` in the repo. Goal: a peer can see **code that creates the VM**.

### Minimal resources to declare

- AzureRM provider + subscription id
- Resource group, VNet, subnet, network security group
- Public IP + Ubuntu VM
- Outputs: `public_ip`, `ssh_command`

### High-level flow

1. Install Terraform: https://developer.hashicorp.com/terraform/install
2. `az login`. No API key file and no client secret in the repo.
3. `zoomcamp/terraform/terraform.tfvars` (gitignored) — subscription id, region, your IP, SSH public key.
4. ```bash
   cd zoomcamp/terraform
   terraform init
   terraform plan
   terraform apply
   ```
5. README section: “Infrastructure” with a screenshot of `terraform apply` + the Azure VM + how to SSH. Redact the subscription id.

### Azure + Terraform docs

- AzureRM provider: https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs
- Linux VM: https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/linux_virtual_machine

### Size tip

If `Standard_B2s` is unavailable in the region, change `location` or set `vm_size` to another burstable x86 size. Do not switch to a D-series or E-series size. Those spend the credit much faster.

---

## README blurb you can paste (for graders)

> This project’s cloud compute is an **Azure VM** provisioned with **Terraform** (VNet, security rule, Ubuntu). The VM is paid from a **$100 Azure credit** and is deallocated when idle. The data warehouse is **MotherDuck** (free). The object storage lake is **MinIO on the VM** (S3-compatible), not Azure Blob. See `zoomcamp/terraform/` and `docs/cloud-and-free-demo.md`.

---

# Part B — Free internet demo without hosting Spark/Flink (GitHub + Streamlit + MotherDuck)

Use this so peers/recruiters open a **live URL** even if the heavy cluster is local or on a VM that isn’t public.

## Architecture (demo path)

```
GitHub repo
  ├─ GitHub Actions CI          (tests / dbt parse / compose config)
  ├─ dbt → MotherDuck (free)    (gold marts; can be built from laptop/VM/CI)
  └─ Streamlit Community Cloud  (dashboard 2 tiles → reads MotherDuck)
       └─ optional: FastAPI on Render/Railway/Fly free tier
```

Heavy pieces (**Airflow, dlt pipelines, Redpanda, PyFlink, PySpark, MinIO**) stay on the laptop or the Azure VM; README shows screenshots + how to run Compose. Graders still see cloud compute + a cloud warehouse + a live dashboard + IaC if you did Part A.

## B1 — MotherDuck (free warehouse)

1. Sign up: https://motherduck.com
2. Create token; put in GitHub Actions secrets + Streamlit secrets — never commit.
3. From laptop/VM: run **dlt** pipelines (ingest) then `dbt build` into MotherDuck.
4. Optionally seed a small `marts.*` sample so the demo works if producers are down.

Bruin/MotherDuck docs (reference): https://getbruin.com/docs/bruin/platforms/motherduck

## B2 — Streamlit Community Cloud (free dashboard URL)

Overview: https://streamlit.io/cloud
Docs: https://docs.streamlit.io/deploy/streamlit-community-cloud

1. Put dashboard app in repo, e.g. `zoomcamp/dashboard/app.py` with **2 tiles**:
   - categorical: disruption/delay by Tube line
   - temporal: reliability over `event_date`
2. `zoomcamp/dashboard/requirements.txt` (streamlit, pandas, duckdb or motherduck connector, etc.).
3. Push to **public** GitHub repo.
4. https://share.streamlit.io → **Sign in with GitHub** → pick repo / branch / `zoomcamp/dashboard/app.py` → **Deploy**.
5. App secrets (Streamlit Cloud → Settings → Secrets), example:
   ```toml
   MOTHERDUCK_TOKEN = "..."
   MOTHERDUCK_DATABASE = "transitpulse"
   ```
6. Every `git push` redeploys. Put the public URL in the Zoomcamp README.

## B3 — Optional free FastAPI URL (extras, not graded)

If you want `extras/api/` `/health` + `/metrics/*` online without the Azure VM:

| Host | Notes |
|------|--------|
| https://render.com | Free web service; spins down when idle |
| https://railway.app | Limited free credits |
| https://fly.io | Free allowance; card sometimes required |

Point the service at MotherDuck; same env secrets pattern as Streamlit.

GitHub alone does **not** host FastAPI persistently (Actions ≠ server; Pages ≠ API).

## B4 — GitHub Actions (CI, not hosting)

`.github/workflows/ci.yml` on each PR:

- `uv sync` + `pytest`
- Airflow DAG parse
- `dbt deps` + `dbt parse` in `zoomcamp/dbt/`
- `docker compose -f zoomcamp/docker-compose.yml config`

Optional separate workflow: `dbt build` into MotherDuck on `main` using secrets (keeps demo data fresh).

---

# Recommended combo for a live link

| Layer | Choice | Cost |
|-------|--------|------|
| IaC + compute | Terraform in `zoomcamp/terraform/` → one Azure VM (lean **zoomcamp** Compose) | $100 credit, then stop |
| Warehouse | MotherDuck free | $0 |
| Public dashboard | Streamlit Community Cloud | $0 |
| CI | GitHub Actions | $0 |
| Stream/batch heavy jobs | On the VM and/or laptop; screenshots in README | credit only while the VM runs |

**Zoomcamp story:** Terraform + Azure VM; dashboard URL for peers; full **`zoomcamp/`** pipeline reproducible via Compose. FastAPI is extras.

---

# Checklist before peer review

- [ ] `zoomcamp/terraform/` in repo + `terraform plan` screenshot or apply output (redact the subscription id)
- [ ] Azure VM visible / described in README, including how the credit is stopped
- [ ] Streamlit (or VM) dashboard URL with **2 tiles**
- [ ] MotherDuck mentioned as DWH; MinIO as lake (not Azure Blob)
- [ ] No secrets in git
- [ ] Clean clone instructions still work locally

---

# What not to expect

- **GitHub Pages** hosting Airflow/Flink.
- **GitHub Actions** as a 24/7 data platform.
- Leaving the VM running for weeks. Deallocate it.
- Fitting every container on `Standard_B2s`.

---

*Companion to `README.md` and `PROJECT.md`. Update links if the Azure portal or Streamlit UI changes.*
