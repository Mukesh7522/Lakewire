# Deploying for free, step by step

## What "free and always on" really requires

Kafka, Spark, MinIO and Airflow need roughly 12 GB of RAM together. GitHub Actions cannot host that (jobs are capped at 6 hours and are not always-on). The only widely available free option with enough memory is **Oracle Cloud Always Free**: an Ampere A1 VM with up to 4 OCPUs, 24 GB RAM and 200 GB of block storage in total.

Things to check yourself, because free-tier terms change and I cannot verify them live:

- Sign-up needs a credit or debit card for identity verification. Staying inside Always Free resources should not be charged. Read the current terms before you start.
- A1 capacity is sometimes "out of capacity". Retry later or another availability domain.
- Oracle can reclaim Always Free instances that stay idle for a week. This workload is continuous, so it should not trigger that.
- Without a card, no free host in this plan can run the full stack. The fallback would be a smaller design (scheduled micro-batches in GitHub Actions) that I have not built.

## 1. GitHub

1. Create a **public** repository named `lakewire` (public repos get free Actions minutes and free Pages).
2. Push this code to `main`.
3. Set `REPO_URL` near the top of the script in `site/index.html` (replace `YOUR_USER`). Until `site/data/*.json` exists the page shows clearly labelled simulated demo data; open `/?demo` to force that mode.
4. Settings, Pages, Source: **GitHub Actions**.
5. Settings, Environments: create `production`.

## 2. The VM

1. Create an instance: shape `VM.Standard.A1.Flex`, 4 OCPU, 24 GB, Ubuntu 24.04 (aarch64), 150 to 190 GB boot volume, your SSH public key.
2. Keep only port 22 open in the VCN security list. Dashboards are reached through an SSH tunnel, never the public internet.
3. SSH in and run:
   ```bash
   curl -fsSL https://raw.githubusercontent.com/YOUR_USER/lakewire/main/scripts/bootstrap_vm.sh -o bootstrap_vm.sh
   bash bootstrap_vm.sh https://github.com/YOUR_USER/lakewire.git
   ```
4. Log out and in once, then edit `/opt/lakewire/.env`: strong passwords, and a real `USER_AGENT` with your contact email.
5. `cd /opt/lakewire && docker compose up -d --build`. The first build takes 10 to 20 minutes.
6. `python3 scripts/healthcheck.py` should print `healthy`.

## 3. Let GitHub deploy for you

On your laptop create a dedicated key and pin the host key:

```bash
ssh-keygen -t ed25519 -f lakewire_deploy -N ""
ssh-copy-id -i lakewire_deploy.pub ubuntu@VM_IP      # or append the .pub to ~/.ssh/authorized_keys on the VM
ssh-keyscan -t ed25519 VM_IP                          # copy this output
```

Add repository secrets (Settings, Secrets and variables, Actions):

| Secret | Value |
|---|---|
| `VM_HOST` | the VM public IP |
| `VM_USER` | `ubuntu` |
| `VM_SSH_KEY` | contents of `lakewire_deploy` (private key) |
| `VM_KNOWN_HOSTS` | the `ssh-keyscan` output |

## 4. First run

1. Push to `main`. `ci` runs, then `cd` deploys.
2. Run `publish-dashboard` once from the Actions tab. Your dashboard appears at `https://YOUR_USER.github.io/lakewire/`.
3. Open Airflow through a tunnel: `ssh -L 8080:localhost:8080 -L 3000:localhost:3000 ubuntu@VM_IP`, then browse to `localhost:8080` (password: `docker compose logs airflow | grep -i password`) and `localhost:3000` (Grafana, user `admin`).
4. DAGs are unpaused by default. `lakewire_pageviews_daily` backfills from `PAGEVIEWS_START_DATE`; set it to a few days back, not months, until you have seen disk growth.

## Free-tier hygiene

- GitHub disables scheduled workflows after 60 days without repository activity. Push a small commit now and then, or re-enable them in the Actions tab.
- If `publish-dashboard` goes red, the data is stale. Start with `docs/RUNBOOK.md`.
