# Azure VM move (Zoomcamp cloud log)

Certification log for the Phase 1 cloud step: the graded `zoomcamp/` slice ran on a laptop first, then the same Compose stack was started on one Azure VM. This page records **what has actually run**. The how-to for a fresh apply is [cloud-and-free-demo.md](./cloud-and-free-demo.md). Decision: [0002-azure-credit-vm.md](./decisions/0002-azure-credit-vm.md).

**As of:** 2026-10-31.

Data on this project is **Data provided by Transport for London** / Powered by TfL Open Data, plus Open-Meteo. This is not an official TfL product.

## What this proves for the course

- Cloud compute is an **Azure VM** created with **Terraform** in `zoomcamp/terraform/`, paid from a **$100 Azure for Students credit**.
- The lake stays **MinIO** on that VM. No Azure Blob, Azure SQL, or other Azure data service.
- The warehouse is **MotherDuck** database `transitpulse`. dlt loads `bronze`, and dbt `--target motherduck` builds `marts.line_status_snapshot` and `marts.weather_hourly`. The laptop DuckDB file is the dbt `dev` target only.
- Streamlit on the VM reads those MotherDuck marts. Port 8501 stays off the security group; the laptop reaches it through an SSH tunnel.

## Done on the laptop before the VM

These already ran locally and stayed the source of truth for the move:

- MinIO bronze upload and the Tube **dlt** load from that object.
- dbt model `marts.line_status_snapshot` in `zoomcamp/transitPule_dbt/` against `zoomcamp/data/transitpulse.duckdb`.
- Airflow DAG `tube_status` (`load_tube_bronze` then `build_line_status_mart`) green on local Compose.
- Streamlit in `zoomcamp/dashboard/` reading local `marts.*`.
- `pytest` in `zoomcamp/tests/` and `.github/workflows/ci.yml` (also `dbt parse`, `docker compose config`, and Airflow `DagBag` import in the `2.10.4-python3.12` image — no `.env`, no warehouse token, no webserver).

## Done — create the VM

Working directory: `zoomcamp/terraform/`. Real values live only in gitignored `terraform.tfvars`.

1. `az login` to the **Azure for Students** subscription.
2. `terraform init`, `terraform validate`, `terraform plan`, `terraform apply`.
3. The student region policy rejects `uksouth` (`RequestDisallowedByAzure`). Allowed regions include `austriaeast`.
4. `Standard_B2s` had no capacity in the allowed regions (`SkuNotAvailable`). The apply that succeeded uses `location = austriaeast` and `vm_size = Standard_B2s_v2` (2 vCPU, 8 GiB) in `terraform.tfvars`. Code defaults stay `uksouth` / `Standard_B2s`.
5. The VM and its public IP are pinned to **zone 1**. On `Standard_B2s_v2` in `austriaeast`, zone 3 is restricted.
6. `terraform plan` after the firewall attach reported **no changes**.

Resources from that apply:

| Piece | Name |
|-------|------|
| Resource group | `transitpulse-rg` |
| Region | `austriaeast`, zone 1 |
| VM | `transitpulse-zoomcamp`, Ubuntu 24.04, `Standard_B2s_v2` |
| VNet / subnet | `transitpulse-vnet` / `transitpulse-public` (`10.0.1.0/24`) |
| SSH | user `ubuntu`, key auth, port 22 from the operator `/32` only |
| NSG | `transitpulse-ssh`, associated with `transitpulse-public` |
| Public IP | `transitpulse-ip` (read it with `terraform output public_ip`) |

Airflow `:8080` and MinIO `:9000` / `:9001` are **not** on the security group. Reach them with an SSH tunnel from the laptop. Do not publish those ports. The laptop Compose stack keeps `:8080`, `:9000`, and `:9001`, so the tunnel uses other local ports. Open a new Git Bash window and leave it open while you use the UI. `<public_ip>` is `terraform output public_ip`.

Airflow on the VM, in the browser at http://localhost:8081:

```bash
ssh -i ~/.ssh/zack-transitpulse -L 8081:localhost:8080 ubuntu@<public_ip>
```

Sign in with `AIRFLOW_WWW_USER` and `AIRFLOW_WWW_USER_PASSWORD` from `zoomcamp/.env`.

MinIO on the VM, in the browser at http://localhost:19001. The console on `19001` calls the API on `19000`, so both forwards are required:

```bash
ssh -i ~/.ssh/zack-transitpulse -L 19000:localhost:9000 -L 19001:localhost:9001 ubuntu@<public_ip>
```

Sign in with `MINIO_ROOT_USER` and `MINIO_ROOT_PASSWORD` from the same `.env`.

Stop the compute charge when the session ends:

```bash
az vm deallocate --resource-group transitpulse-rg --name transitpulse-zoomcamp
```

Start the same machine later with `az vm start` on those names. The disk and public IP can still draw a small amount while deallocated.

## Done — move Compose onto the VM

1. SSH: `ssh -i ~/.ssh/zack-transitpulse ubuntu@<public_ip>` (passphrase is the one chosen at `ssh-keygen`).
2. On the VM: `sudo apt update && sudo apt install -y docker.io docker-compose-v2 git`, then `sudo usermod -aG docker "$USER"`, then log in again.
3. Versions after that login: **Docker 29.1.3**, **Docker Compose 2.40.3**.
4. On the VM: `git clone -b FirstUpload https://github.com/ZakariaeJaafari/transitpulse-london-ops-dashboard.git`, then `cd transitpulse-london-ops-dashboard/zoomcamp`.
5. The laptop `.env` was copied to that directory with `scp`. It is gitignored. Do not commit it and do not print it.
6. On the VM, from `zoomcamp/`: `docker compose up -d --build`.

Result on 2026-10-28:

- `zoomcamp-minio-1` — healthy
- `zoomcamp-airflow-1` — healthy (image `zoomcamp-airflow`)

That is the lean Compose file: MinIO and Airflow only. Streamlit is not a Compose service.

## Done — Tube DAG on the VM

1. From the VM `zoomcamp/` directory, inside the Airflow container (MinIO there is `http://minio:9000`):

```bash
docker compose exec airflow uv run python lake/upload_bronze.py
```

Uploaded `tube_line.json` (16362 bytes) and `london_weather.json` (7545 bytes) into bucket `transitpulse`.

2. `airflow dags trigger tube_status` stayed `queued` until `airflow dags unpause tube_status`. Airflow creates a new DAG paused.
3. Run `manual__2026-10-28T20:28:11` failed. `load_tube_bronze` could not open `/opt/transitpulse/zoomcamp/data/transitpulse.duckdb` (`Permission denied`). The clone is owned by `ubuntu`. The task runs as uid `50000` (`airflow`). `build_line_status_mart` was `upstream_failed`.
4. On the VM:

```bash
mkdir -p data transitPule_dbt/target transitPule_dbt/logs
sudo chown -R 50000:0 data transitPule_dbt
docker compose exec airflow airflow dags trigger tube_status
```

5. Run `manual__2026-10-28T20:41:27` is **success** (start 20:41:28 UTC, end 20:41:43 UTC). That run is the load and `line_status_snapshot` on the VM's DuckDB file.
6. After that session, `az vm deallocate --resource-group transitpulse-rg --name transitpulse-zoomcamp` completed. Start the same VM again with `az vm start` before the next SSH or tunnel.

## Done — MotherDuck DAGs on the VM

2026-10-30. `az vm start` reached `VM running`. The SSH rule in gitignored `terraform.tfvars` was updated so the current home address could connect. That address is not recorded here.

1. The laptop `.env` was copied again with `scp`. It is gitignored.
2. `transitPule_dbt` was owned by uid `50000`, so `git pull` could not replace tracked files. `sudo chown -R ubuntu:ubuntu .` on the clone, then `git reset --hard origin/FirstUpload`. `HEAD` is `4a3fc50` (`Load bronze to motherduck and adapt pipelines to that localy`).
3. `mkdir -p data transitPule_dbt/target transitPule_dbt/logs`, then `sudo chown -R 50000:0 data transitPule_dbt` so the Airflow user can write the dbt log.
4. `docker compose up -d --build --wait`: `zoomcamp-minio-1` and `zoomcamp-airflow-1` healthy.
5. `airflow dags unpause weather_hourly`, then trigger both DAGs.
6. `tube_status` run `manual__2026-10-30T19:09:45+00:00` is **success** (start 19:09:47 UTC, end 19:10:17 UTC).
7. `weather_hourly` run `manual__2026-10-30T19:09:48+00:00` is **success** (start 19:09:59 UTC, end 19:10:27 UTC).

Those runs load `transitpulse.bronze` and build the two marts with `dbt run --target motherduck`. After the session, `az vm deallocate --resource-group transitpulse-rg --name transitpulse-zoomcamp` returned to the shell prompt with no error.

## Done — Streamlit on the VM

2026-10-31. `az vm start` reached a running VM. SSH as `ubuntu` worked.

1. On the VM, `uv` was installed for the `ubuntu` user. From `zoomcamp/`: `uv sync`, then `uv run streamlit run dashboard/app.py --server.address 127.0.0.1 --server.port 8501 --server.headless true`.
2. The first process failed: `Invalid MotherDuck token.` The VM `.env` still had a Windows carriage return on the token line. `sed -i 's/\r$//' .env`, then `export motherduck_token="$MOTHERDUCK_TOKEN"`.
3. The second process listened on `127.0.0.1:8501` at 20:00:00 UTC. The laptop tunnel was `ssh -L 8502:localhost:8501`. Port 8501 was not added to the security group.
4. At 20:00:38 UTC the page rendered both charts: the Tube tile and the weather tile, from MotherDuck `marts.line_status_snapshot` and `marts.weather_hourly`.
5. Streamlit was stopped, the SSH session closed, and `az vm deallocate --resource-group transitpulse-rg --name transitpulse-zoomcamp` returned to the prompt with no error.

## Not done yet

- None for the Phase 1 VM slice. Deallocate the VM whenever it is not in use.

## Secrets that stay off GitHub

`zoomcamp/.env`, `zoomcamp/terraform/terraform.tfvars`, `terraform.tfstate`, `*.duckdb`, and the SSH private key. Examples with placeholders are `zoomcamp/.env.example` and `zoomcamp/terraform/terraform.tfvars.example`.
