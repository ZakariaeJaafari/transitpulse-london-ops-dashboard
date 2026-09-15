# TransitPulse — Cloud 4/4 + Free Public Demo

Visual A→Z flows: **[diagrams.md](./diagrams.md)**

How to earn Zoomcamp **Cloud = 4 points** (cloud + IaC) at $0, **and** how to put a lightweight demo on the internet with **GitHub + Streamlit Cloud + MotherDuck** (no always-on Spark/Flink required).

---

## What Zoomcamp wants for Cloud points

| Score | Meaning |
|------:|---------|
| 0 | Runs only on your laptop |
| 2 | Developed in the cloud |
| **4** | Developed in the cloud **and** IaC (Terraform / Pulumi / CloudFormation / OCI Resource Manager) |

**MotherDuck alone** may or may not convince a peer reviewer that the *project* is “in the cloud.” Safer for **4/4**: a **free cloud VM** created with **Terraform**, running the **`zoomcamp/`** stack (or at least Airflow + dashboard), documented in the README. FastAPI is extras and is not required for Cloud 4.

---

# Part A — Free VM for Cloud 4/4 (recommended: Oracle Cloud Always Free)

## Why Oracle

- **Always Free** compute for the life of the account (home region).
- Docs: https://docs.oracle.com/en-us/iaas/Content/FreeTier/freetier_topic-Always_Free_Resources.htm
- Signup: https://www.oracle.com/cloud/free/
- Includes (typical Always Free):
  - Up to **2×** `VM.Standard.E2.1.Micro` (AMD), **or**
  - **Ampere A1 Flex** Arm: up to **2 OCPUs + 12 GB RAM** total (one beefier box or two smaller)
  - Block storage + VCN networking allowances

**Caveats (honest):**
- Credit card often required for signup; Always Free should stay $0 if you don’t upgrade / leave paid resources running.
- **“Out of host capacity”** is common for A1 in busy regions — retry other ADs, other shapes, or wait.
- Ampère is **ARM**: use multi-arch Docker images or build for `linux/arm64`. Micro AMD is x86 if you want fewer image surprises (weaker RAM).
- Full Airflow + Spark + Flink + Redpanda + MinIO on **one** free VM is tight — see “What to run on the VM” below.

## Step-by-step (console first, then Terraform)

### 1. Create the free account
1. Go to https://www.oracle.com/cloud/free/ → **Start for free**.
2. Complete verification; note your **home region** (Always Free compute must be there).
3. In the Console, confirm you see **Always Free-eligible** labels.

### 2. Networking (VCN)
1. Create a VCN with Internet Gateway (or use the wizard “Create VCN with Internet Connectivity”).
2. Security list / NSG ingress (for demo only; lock down later):
   - `22` SSH from **your IP** only
   - `8080` Airflow (optional)
   - `8501` Streamlit
   - `9000/9001` MinIO (prefer SSH tunnel instead of public)
   - `8000` FastAPI only if you also deploy **`extras/api/`** (not graded)
3. Prefer **SSH tunnels** for Airflow/MinIO over opening everything to `0.0.0.0/0`.

### 3. Launch Always Free VM
1. **Compute → Instances → Create**.
2. Shape: Prefer `VM.Standard.A1.Flex` (2 OCPU / 12 GB) if capacity allows; else `VM.Standard.E2.1.Micro`.
3. Image: Ubuntu **Always Free Eligible**.
4. Add your SSH public key.
5. Assign a public IP.
6. SSH: `ssh -i ~/.ssh/your_key ubuntu@<PUBLIC_IP>`

### 4. Bootstrap the VM
```bash
sudo apt update && sudo apt install -y docker.io docker-compose-v2 git
sudo usermod -aG docker $USER   # log out/in after
git clone https://github.com/<you>/transitpulse.git
cd transitpulse/zoomcamp
cp .env.example .env            # fill secrets on the VM only
docker compose up -d
```

### 5. What to run on the free VM (pragmatic)

| Mode | Services on VM | Enough for Cloud 4? |
|------|----------------|---------------------|
| **Full** | Entire **`zoomcamp/`** Compose | Yes, if it fits RAM |
| **Lean (recommended)** | Airflow + **dlt** + MinIO + Streamlit/Metabase; run Flink/Spark on-demand or keep screenshots from local | Yes — still “in the cloud” + Terraform |
| **Demo-only** | Streamlit talking to MotherDuck | Weak alone for “pipeline in cloud”; pair with Part B + document batch/stream ran in CI/local. FastAPI (`extras/`) is optional and not graded. |

Point warehouse at **MotherDuck free** from the VM (outbound HTTPS). No GCS/Azure needed.

---

## Terraform (required for the “+ IaC” half of 4 points)

Put under `zoomcamp/terraform/` in the repo. Goal: peer can see **code that creates cloud infra**.

### Minimal resources to declare
- OCI provider + compartment/tenancy vars
- VCN + subnet + IGW + route table
- Security list / NSG
- Always Free compute instance + public IP
- Outputs: `public_ip`, `ssh_command`

### High-level flow
1. Install Terraform locally: https://developer.hashicorp.com/terraform/install
2. Create OCI API key in Console → User → API Keys; download PEM; note fingerprint, tenancy OCID, user OCID, region.
3. `zoomcamp/terraform/terraform.tfvars` (gitignored) or env vars — **never commit private keys**.
4. ```bash
   cd zoomcamp/terraform
   terraform init
   terraform plan
   terraform apply
   ```
5. README section: “Infrastructure” with screenshot of `terraform apply` + OCI instance + public URL.

### OCI + Terraform docs
- OCI Terraform provider: https://registry.terraform.io/providers/oracle/oci/latest/docs
- OCI Resource Manager also counts as IaC if you prefer Console-uploaded Terraform.

### Capacity tip
If A1 fails with out-of-capacity, Terraform + a retry script/GitHub Action can help, but don’t spam Oracle’s API aggressively. Document the Micro fallback.

---

## README blurb you can paste (for graders)

> This project is deployed on an **Oracle Cloud Always Free** VM provisioned with **Terraform** (VCN, security rules, compute). The data warehouse is **MotherDuck** (free). Object storage lake is **MinIO on the VM** (S3-compatible), avoiding paid GCS/Azure. See `zoomcamp/terraform/` and `docs/cloud-and-free-demo.md`.

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

Heavy pieces (**Airflow, dlt pipelines, Redpanda, PyFlink, PySpark, MinIO**) stay on laptop or free VM; README shows screenshots + how to run Compose. Graders still see cloud warehouse + live dashboard + IaC if you did Part A.

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

If you want `extras/api/` `/health` + `/metrics/*` online without the Oracle VM:

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

# Recommended combo for max points + live link

| Layer | Choice | Cost |
|-------|--------|------|
| IaC + compute | Terraform in `zoomcamp/terraform/` → Oracle Always Free VM (lean **zoomcamp** Compose) | $0 |
| Warehouse | MotherDuck free | $0 |
| Public dashboard | Streamlit Community Cloud | $0 |
| CI | GitHub Actions | $0 |
| Stream/batch heavy jobs | On VM and/or laptop; screenshots in README | $0 |

**Zoomcamp story:** Cloud + Terraform = 4; dashboard URL for peers; full **`zoomcamp/`** pipeline reproducible via Compose. FastAPI is extras.

---

# Checklist before peer review

- [ ] `zoomcamp/terraform/` in repo + `terraform plan` screenshot or apply output (redact secrets)
- [ ] OCI instance visible / described in README
- [ ] Streamlit (or VM) dashboard URL with **2 tiles**
- [ ] MotherDuck mentioned as DWH; MinIO as lake
- [ ] No secrets in git
- [ ] Clean clone instructions still work locally

---

# What not to expect

- **GitHub Pages** hosting Airflow/Flink.
- **GitHub Actions** as a 24/7 data platform.
- Fitting every container comfortably on the tiny AMD Micro — prefer A1 12 GB or lean Compose.

---

*Companion to `README.md` and `PROJECT.md`. Update links if Oracle/Streamlit UIs change.*
