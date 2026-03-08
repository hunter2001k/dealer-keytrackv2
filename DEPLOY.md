# 🚀 Deploy in 5 Minutes — KeyTrack

## Option A — Railway (Easiest, always-on free tier)

1. Go to **railway.app** → Sign up with GitHub
2. Click **New Project** → **Deploy from GitHub repo**
3. Upload this folder to a new GitHub repo first (drag & drop on github.com)
4. Select your repo → Railway auto-detects Python
5. Click **Variables** tab → add your 8 env vars (see list below)
6. Click **Deploy** — live in ~90 seconds ✅

---

## Option B — Render (Also free)

1. Go to **render.com** → Sign up
2. Click **New → Web Service** → Connect GitHub → select your repo
3. Build Command: `pip install -r requirements.txt`
4. Start Command:  `gunicorn app:app`
5. Add env vars below → **Create Web Service** ✅

---

## Required Environment Variables

| Variable | Where to get it |
|---|---|
| `FLASK_SECRET_KEY` | Any random string e.g. `abc123xyz789` |
| `AIRTABLE_TOKEN` | airtable.com/create/tokens |
| `AIRTABLE_BASE_ID` | Your base URL: airtable.com/**appXXXXX**/... |
| `AIRTABLE_TABLE` | `Vehicles` (exact table name) |
| `OPENAI_API_KEY` | platform.openai.com/api-keys |
| `ACCESS_PASSWORD` | Password for all staff |
| `ADMIN_PASSWORD` | Password for managers only |
| `DEALERSHIP_NAME` | e.g. `Premier Auto Group` |

---

## Add to Phone Home Screen

**iPhone:** Safari → your URL → Share → Add to Home Screen
**Android:** Chrome → your URL → ⋮ menu → Add to Home Screen

---

## Your App URLs After Deploy

| Page | URL |
|---|---|
| Staff Login | `yourdomain.com/login` |
| Fleet Dashboard | `yourdomain.com/dashboard` |
| Scan a Vehicle | `yourdomain.com/scan/CAR-01` |
| Analytics | `yourdomain.com/analytics` |
| QR Generator | `yourdomain.com/qr-generator` |
| Admin Panel | `yourdomain.com/admin` |
| Window Sticker | `yourdomain.com/window-sticker/CAR-01` |
