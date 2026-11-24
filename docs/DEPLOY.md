# Deploying for free, step by step

There are three ways to run Lakewire. In paths A and B, Kafka, Spark, the S3 store, Airflow and the monitoring all run as containers from the one `docker-compose.yml`, so there is no separate Kafka or other service to set up. Path C needs no server at all.

| | Path A: your own computer | Path B: a VM | Path C: GitHub only |
|---|---|---|---|
| Cost | free (electricity) | free on Oracle if it works for you, otherwise about 9 to 16 EUR a month | free |
| Accounts | GitHub only | GitHub plus a cloud account | GitHub only |
| Always on | only while the computer is on and awake | yes | yes (scheduled, hourly) |
| Runs | the full Kafka and Spark stack | the full Kafka and Spark stack | a lighter DuckDB version |
| Dashboard publishing | `scripts/publish_local.ps1` pushes it | GitHub Actions pulls it over SSH | the workflow publishes it |

## Path C: GitHub only (free, nothing runs on your computer)

A lighter version of the pipeline runs entirely on scheduled GitHub Actions workflows, which are free for public
repositories. No server, no Docker, no cloud account, no database. It is a different system from the Kafka and Spark one,
built for the same dashboard:

| | Full (paths A and B) | Lite (path C) |
|---|---|---|
| Ingestion | live stream into Kafka, always on | a scheduled job replays from a saved position each hour |
| Processing | Spark Structured Streaming into Delta | DuckDB into Parquet |
| Orchestration | Airflow | GitHub Actions schedules |
| Storage | Delta on an S3-compatible store | Parquet files in a git branch |
| Raw history | Bronze 14 days, Silver 120 days | Silver 7 days, no Bronze |
| Freshness | seconds, marts rebuilt hourly | up to about an hour plus GitHub's scheduling delay |
| Monitoring | Prometheus and Grafana | run history, annotations, failure emails |
| Cost | electricity, or a VM | $0 (public repository) |

What the lite version keeps: the data contract and the dead-letter log, deduplication by event id, the dbt models and all
24 of their tests (rebuilt from scratch every hour), the same marts, and the same dashboard, which describes the mode it
is running in. State lives in two branches that every run replaces with a single commit, so the repository never grows:
`data` (the Parquet files and the saved stream position) and `gh-pages` (the published site).

Measured on the live Wikimedia stream (five wikis): the first run replays an hour in about 100 seconds; a normal hourly
run takes about a minute; an hour of edits is about 0.6 MB, so the 7-day window is about 100 MB; a day of pageviews
(top 20,000 pages per hour) is about 5 MB and takes about 8 minutes to download and process. The pageview step was
compared with the Spark job on a real day (top 50,000 per hour): the same 1,200,000 rows, 93,800,476 total views and the
same content fingerprint.

Setup, about 10 minutes:

1. Push this repository to a **public** GitHub repository (`main`). Edit nothing else.
2. Settings, Pages, Source: **Deploy from a branch**, branch `gh-pages`, folder `/ (root)`.
3. Leave the repository variable `DEPLOY_TARGET` unset (or set it to `github`). Setting it to `vm` or `local` switches
   the lite workflows off, because the system runs somewhere else.
4. Actions, `lakewire-lite-hourly`, **Run workflow**. The first run takes about 5 minutes because it replays three hours.
5. Actions, `lakewire-lite-pageviews`, **Run workflow**. It loads yesterday and the day before (about 15 to 20 minutes).
   After that it runs by itself every day.
6. Open the site. The hero should say "collected hourly" and the pipeline explorer should show the GitHub Actions
   diagram. Trending fills in after the pageview run and the next hourly run.

Reading a run: every run ends with a summary line in its annotations, for example `read=108000, filtered=93000,
kept_new=15000, dlq=0, caught_up=True`. A failed run shows its error as an annotation and GitHub emails you.

If the full stack is running on your computer, switch it off once the first lite run has succeeded (otherwise both publish
to `gh-pages`): disable the scheduled task (`Disable-ScheduledTask -TaskName LakewirePublish`) and run
`docker compose down` (without `-v`, so the old data is kept).

Limits to know about:

- GitHub can delay scheduled runs, especially around the top of the hour (this one runs at :23 for that reason), and may
  drop queued runs under very heavy load. A missed run is harmless: the next one resumes from the saved position, as long
  as the gap is inside Wikimedia's replay window (7 to 31 days).
- GitHub disables scheduled workflows after 60 days with no repository activity. Push any commit, or re-enable them in
  the Actions tab, to restart them.
- The repository must stay public for the free Actions minutes and Pages. The `data` branch is public too: it holds public
  Wikipedia edit metadata, including editor usernames, which are public in the stream.
- The scheduling, the token push and the Pages rebuild request are GitHub-specific and can only be proven by a real run.
  Everything else was run end to end against the live stream in a container that imitates a runner.

## Path A: your own computer (free, no cloud account)

What you need: Windows with Docker Desktop (WSL 2), about 8 GB of RAM available to Docker, and 20 GB of free disk. The stack was measured at about 5.5 GB steady and 7.5 GB while the daily pageview ingest runs, with two wikis.

Be honest with yourself about what "free" costs here: the dashboard is only as live as the computer. While it is off or asleep nothing is ingested. The producer saves its position and resumes from it, so a sleep is recovered: a real 58-minute sleep was caught up in about 30 seconds with no gap (`docs/incidents/2026-11-23-laptop-sleep.md`). A much longer outage may still leave a gap, because Wikimedia's stream history is finite. A laptop on battery with the lid closed is not a server.

1. **GitHub.** Create a public repository, push this code to `main`, and set `REPO_URL` near the top of `site/assets/site.js`. In Settings, Pages, set Source to **Deploy from a branch**, branch `gh-pages`, folder `/ (root)`. Do not create the `production` environment, secrets or the `DEPLOY_TARGET` variable: those are for Path B, and without them the VM workflows skip themselves instead of failing.
2. **Secrets.** `powershell -NoProfile -File scripts\init_env.ps1` creates `.env` with random passwords and two wikis. Edit `USER_AGENT` to include your contact email.
3. **Limit Docker's memory** so it cannot starve Windows: create `%UserProfile%\.wslconfig` containing `[wsl2]` and `memory=10GB`, then run `wsl --shutdown` and restart Docker Desktop.
4. **Start it.** `docker compose up -d --build` (the first build takes 10 to 20 minutes), then `python scripts\healthcheck.py` should print `healthy`.
5. **First snapshot.** The hourly marts job runs at the top of the hour. To run it now: `docker compose exec airflow airflow dags trigger lakewire_marts_hourly`. The Trending section stays empty until the first daily pageview run finishes (about 1.2 GB download).
6. **Publish.** `powershell -NoProfile -File scripts\publish_local.ps1` exports the snapshot and force-pushes the site to `gh-pages` as one commit, so history never grows. Your dashboard appears at `https://YOUR_USER.github.io/lakewire/`.
7. **Publish every hour, just after the refresh.** The marts job runs at the top of each UTC hour, so run the publish about 6 minutes later. In PowerShell, replacing the path with where you cloned the repo:
   ```powershell
   $script = "C:\path\to\Lakewire\scripts\publish_local.ps1"
   $utc  = [DateTime]::UtcNow
   $next = [DateTime]::SpecifyKind($utc.Date.AddHours($utc.Hour).AddMinutes(6), 'Utc')
   if ($next -le $utc) { $next = $next.AddHours(1) }
   $action   = New-ScheduledTaskAction -Execute "powershell.exe" -Argument "-NoProfile -WindowStyle Hidden -ExecutionPolicy Bypass -File `"$script`""
   $trigger  = New-ScheduledTaskTrigger -Once -At $next.ToLocalTime() -RepetitionInterval (New-TimeSpan -Hours 1) -RepetitionDuration (New-TimeSpan -Days 3650)
   $settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable -MultipleInstances IgnoreNew -ExecutionTimeLimit (New-TimeSpan -Minutes 30)
   Register-ScheduledTask -TaskName LakewirePublish -Action $action -Trigger $trigger -Settings $settings
   ```
   The battery settings matter: a task made with plain `schtasks` refuses to start on battery, which silently stops publishing whenever a laptop is unplugged. This form also copes with spaces in the path. If the snapshot is stale when the task fires (for example right after the computer wakes from sleep), the script waits up to 8 minutes for the hourly marts run to produce a fresh one. Test it with `Start-ScheduledTask -TaskName LakewirePublish`, then check `Get-ScheduledTaskInfo -TaskName LakewirePublish` shows `LastTaskResult : 0`. The task only runs while you are signed in. In Docker Desktop, enable "Start Docker Desktop when you sign in", and in Windows power settings set sleep to Never while plugged in.

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
3. Set `REPO_URL` near the top of `site/assets/site.js` (replace `YOUR_USER`). Until `site/data/*.json` exists the page shows clearly labelled simulated demo data; open `/?demo` to force that mode.
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
