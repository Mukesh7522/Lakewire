# Deploying for free, step by step

## What "free and always on" really requires

The stack was measured running locally (2 wikis, one full day of pageviews): about 5.5 GB of RAM steady state and about 7.5 GB while the daily pageview ingest runs. With all five default wikis expect somewhat more, so **a 12 GB VM is enough and 24 GB is not required**. Re-measure with `docker stats` in the first week. GitHub Actions cannot host this (jobs are capped at 6 hours and are not always-on). The only widely available free option with enough memory is **Oracle Cloud Always Free**: Ampere A1 compute and 200 GB of block storage in total.

Things to check yourself, because free-tier terms change and I cannot verify them live:

- Sign-up needs a credit or debit card for identity verification (no prepaid or virtual cards). Staying inside Always Free resources should not be charged. Read the current terms before you start.
- The Always Free A1 allowance is 1,500 OCPU-hours and 9,000 GB-hours per month, which is 2 OCPUs and 12 GB always on. Oracle reduced it from 4 OCPUs and 24 GB in June 2026, so older guides quoting 24 GB are out of date. Check your own console before choosing a shape.
- A1 capacity is sometimes "out of capacity". Retry later or another availability domain.
- Oracle reclaims instances it judges idle: over 7 days, 95th-percentile CPU, network and (on A1) memory all below 20%. This stack holds well over 20% of 12 GB in memory, so it should not qualify, but check the instance metrics after the first week.

## If you do not want to use Oracle

There is no other free option that fits this stack (checked in November 2026):

| Option | RAM | Verdict |
|---|---|---|
| Google Cloud free tier (`e2-micro`, three US regions) | 1 GB | Too small. The stack needs about 6 to 8 GB. |
| AWS free tier | credits, 6 months | Not free for long. |
| Azure free VMs | 12 months, up to 4 GB | Not free for long, and too small. |
| **Your own machine** (a PC, old laptop, mini PC) | whatever it has | Free apart from electricity. Needs 8 GB or more free and to stay on. |
| **A cheap paid VPS** | 8 to 16 GB | Hetzner Cloud lists 4 vCPU / 8 GB at about 9 to 11 EUR a month and 8 vCPU / 16 GB at about 16 EUR (EU regions). 8 GB is tight against the measured 7.5 GB peak, so prefer 16 GB. |

Your own machine works for everything except the publish step: `publish.yml` pulls the snapshot over SSH, so GitHub must be able to reach the host. At home you would instead run `python -m lakewire.export` on a schedule and push `site/data/*.json` to the repo yourself. That path is not built; ask for it if you go this way.
- Without a card, no free host in this plan can run the full stack. The fallback would be a smaller design (scheduled micro-batches in GitHub Actions) that I have not built.

## 1. GitHub

1. Create a **public** repository named `lakewire` (public repos get free Actions minutes and free Pages).
2. Push this code to `main`.
3. Set `REPO_URL` near the top of the script in `site/index.html` (replace `YOUR_USER`). Until `site/data/*.json` exists the page shows clearly labelled simulated demo data; open `/?demo` to force that mode.
4. Settings, Pages, Source: **GitHub Actions**.
5. Settings, Environments: create `production`.

## 2. The VM

1. Create an instance: shape `VM.Standard.A1.Flex`, 2 OCPU and 12 GB (or more if your account allows), Ubuntu 24.04 (aarch64), 150 to 190 GB boot volume, your SSH public key.
2. Keep only port 22 open in the VCN security list. Dashboards are reached through an SSH tunnel, never the public internet.
3. SSH in and run:
   ```bash
   curl -fsSL https://raw.githubusercontent.com/YOUR_USER/lakewire/main/scripts/bootstrap_vm.sh -o bootstrap_vm.sh
   bash bootstrap_vm.sh https://github.com/YOUR_USER/lakewire.git
   ```
4. Log out and in once, then edit `/opt/lakewire/.env`: a real `USER_AGENT` with your contact email. The bootstrap script already generated strong random S3 and Grafana passwords; compose refuses to start if they are missing.
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
4. The Trending section of the dashboard stays empty until the first daily pageview run finishes (it waits for the dump, then downloads about 1.2 GB). The hourly marts job tolerates the missing table, so edit charts work from the first hour.
5. DAGs are unpaused by default. `lakewire_pageviews_daily` backfills from `PAGEVIEWS_START_DATE`; set it to a few days back, not months, until you have seen disk growth.

## Free-tier hygiene

- GitHub disables scheduled workflows after 60 days without repository activity. Push a small commit now and then, or re-enable them in the Actions tab.
- If `publish-dashboard` goes red, the data is stale. Start with `docs/RUNBOOK.md`.
