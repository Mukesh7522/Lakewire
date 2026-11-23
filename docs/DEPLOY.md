# Deploying for free, step by step

There are two ways to run Lakewire. Kafka, Spark, the S3 store, Airflow and the monitoring all run as containers from the one `docker-compose.yml` in both, so there is no separate Kafka or other service to set up.

| | Path A: your own computer | Path B: a VM |
|---|---|---|
| Cost | free (electricity) | free on Oracle if it works for you, otherwise about 9 to 16 EUR a month |
| Accounts | GitHub only | GitHub plus a cloud account |
| Always on | only while the computer is on and awake | yes |
| Dashboard publishing | `scripts/publish_local.ps1` pushes it | GitHub Actions pulls it over SSH |

## Path A: your own computer (free, no cloud account)

What you need: Windows with Docker Desktop (WSL 2), about 8 GB of RAM available to Docker, and 20 GB of free disk. The stack was measured at about 5.5 GB steady and 7.5 GB while the daily pageview ingest runs, with two wikis.

Be honest with yourself about what "free" costs here: the dashboard is only as live as the computer. While it is off or asleep nothing is ingested. The producer saves its position and resumes from it on restart (verified across a short restart, not across hours), so a short sleep is recovered, but a long outage may leave a gap. A laptop on battery with the lid closed is not a server.

1. **GitHub.** Create a public repository, push this code to `main`, and set `REPO_URL` near the top of the script in `site/index.html`. In Settings, Pages, set Source to **Deploy from a branch**, branch `gh-pages`, folder `/ (root)`. Do not create the `production` environment, secrets or the `DEPLOY_TARGET` variable: those are for Path B, and without them the VM workflows skip themselves instead of failing.
2. **Secrets.** `powershell -NoProfile -File scripts\init_env.ps1` creates `.env` with random passwords and two wikis. Edit `USER_AGENT` to include your contact email.
3. **Limit Docker's memory** so it cannot starve Windows: create `%UserProfile%\.wslconfig` containing `[wsl2]` and `memory=10GB`, then run `wsl --shutdown` and restart Docker Desktop.
4. **Start it.** `docker compose up -d --build` (the first build takes 10 to 20 minutes), then `python scripts\healthcheck.py` should print `healthy`.
5. **First snapshot.** The hourly marts job runs at the top of the hour. To run it now: `docker compose exec airflow airflow dags trigger lakewire_marts_hourly`. The Trending section stays empty until the first daily pageview run finishes (about 1.2 GB download).
6. **Publish.** `powershell -NoProfile -File scripts\publish_local.ps1` exports the snapshot and force-pushes the site to `gh-pages` as one commit, so history never grows. Your dashboard appears at `https://YOUR_USER.github.io/lakewire/`.
7. **Publish every hour.** Run once, replacing the path: `schtasks /Create /SC HOURLY /MO 1 /TN LakewirePublish /TR "powershell -NoProfile -File C:\path\to\Lakewire\scripts\publish_local.ps1"`. In Docker Desktop, enable "Start Docker Desktop when you sign in", and in Windows power settings set sleep to Never while plugged in.

To use less memory, set `WIKIS=enwiki` in `.env` and run `docker compose up -d`.

## Path B: a VM

### What "free and always on" requires

The stack needs about 6 to 8 GB of RAM, so **a 12 GB VM is enough and 24 GB is not required**. Re-measure with `docker stats` in the first week. GitHub Actions cannot host this (jobs are capped at 6 hours and are not always-on).

Free VM options, checked in November 2026:

| Option | RAM | Verdict |
|---|---|---|
| Oracle Cloud Always Free (Arm) | 12 GB | The only free always-on VM that fits. Needs a credit or debit card for sign-up, and Arm capacity is sometimes unavailable. |
| Google Cloud free tier (`e2-micro`, three US regions) | 1 GB | Too small. |
| AWS free tier | credits, 6 months | Not free for long. |
| Azure free VMs | 12 months, up to 4 GB | Not free for long, and too small. |
| Azure for Students | $100 credit, 12 months, no card | Students only, and $100 covers roughly a month of a 16 GB VM. |
| GitHub Codespaces | 120 core-hours a month | About 60 hours on 2 cores. Not always on. |
| A cheap paid VPS | 8 to 16 GB | Hetzner Cloud lists 4 vCPU / 8 GB at about 9 to 11 EUR a month and 8 vCPU / 16 GB at about 16 EUR (EU regions). 8 GB is tight against the measured 7.5 GB peak, so prefer 16 GB. |

Things to check yourself about Oracle, because free-tier terms change:

- The Always Free A1 allowance is 1,500 OCPU-hours and 9,000 GB-hours per month, which is 2 OCPUs and 12 GB always on. Oracle reduced it from 4 OCPUs and 24 GB in June 2026, so older guides quoting 24 GB are out of date.
- A1 capacity is sometimes "out of capacity". Retry later or another availability domain.
- Oracle reclaims instances it judges idle: over 7 days, 95th-percentile CPU, network and (on A1) memory all below 20%. This stack holds well over 20% of 12 GB, so it should not qualify, but check the instance metrics after the first week.

### 1. GitHub

1. Create a **public** repository named `lakewire` (public repos get free Actions minutes and free Pages).
2. Push this code to `main`.
3. Set `REPO_URL` near the top of the script in `site/index.html` (replace `YOUR_USER`). Until `site/data/*.json` exists the page shows clearly labelled simulated demo data; open `/?demo` to force that mode.
4. Settings, Pages, Source: **GitHub Actions**.
5. Settings, Environments: create `production`.
6. Settings, Secrets and variables, Actions, **Variables**: add `DEPLOY_TARGET` with the value `vm`. The deploy and publish workflows only run when it is set.

### 2. The VM

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

### 3. Let GitHub deploy for you

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

### 4. First run

1. Push to `main`. `ci` runs, then `cd` deploys.
2. Run `publish-dashboard` once from the Actions tab. Your dashboard appears at `https://YOUR_USER.github.io/lakewire/`.
3. Open Airflow through a tunnel: `ssh -L 8080:localhost:8080 -L 3000:localhost:3000 ubuntu@VM_IP`, then browse to `localhost:8080` (password: `docker compose logs airflow | grep -i password`) and `localhost:3000` (Grafana, user `admin`).
4. The Trending section of the dashboard stays empty until the first daily pageview run finishes (it waits for the dump, then downloads about 1.2 GB). The hourly marts job tolerates the missing table, so edit charts work from the first hour.
5. DAGs are unpaused by default. `lakewire_pageviews_daily` backfills from `PAGEVIEWS_START_DATE`; set it to a few days back, not months, until you have seen disk growth.

## Free-tier hygiene (both paths)

- GitHub disables scheduled workflows after 60 days without repository activity. Push a small commit now and then, or re-enable them in the Actions tab.
- If `publish-dashboard` goes red, the data is stale. Start with `docs/RUNBOOK.md`.
