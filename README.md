# 🔑 Secure Dealer Vehicle & Key Tracker

A mobile-first Flask app that lets dealership staff scan a QR code on a windshield to instantly see vehicle specs, get an AI-generated sales pitch, and ring the physical key tag — all from their phone.

---

## ✨ Features

| Feature | Detail |
|---|---|
| 🔒 Security Gate | Password login + optional URL token for QR codes |
| 📋 Fleet Dashboard | Searchable list of all vehicles from Airtable |
| 🚗 Vehicle Page | Full specs, VIN, status, notes pulled live from Airtable |
| 🤖 AI Sales Pitch | GPT-4o-mini reads Airtable Notes and writes a 3-sentence pitch |
| 🔑 Key Finder | One tap opens AirTag (findmy://) or Pebblebee app via deep link |
| ✏️ Live Status Update | Change Available/Sold/In Service directly from the scan page |

---

## 🗂️ Airtable Setup

### 1. Create a new Base called "Dealer Inventory"

### 2. Create a Table called **Vehicles** with these fields:

| Field Name | Field Type | Notes |
|---|---|---|
| `Internal_ID` | Single line text | Primary key — e.g. `CAR-01`, `CAR-02` |
| `Make` | Single line text | e.g. Toyota |
| `Model` | Single line text | e.g. Camry |
| `Year` | Number | e.g. 2023 |
| `Color` | Single line text | e.g. Midnight Blue |
| `VIN` | Single line text | 17-character VIN |
| `Mileage` | Single line text | e.g. 12,450 mi |
| `Price` | Currency or text | e.g. $28,900 |
| `Trim` | Single line text | e.g. XSE V6 |
| `Engine` | Single line text | e.g. 3.5L V6 |
| `Status` | Single select | Options: Available, Sold, In Service, Demo, Hold, Pending |
| `Key_Tag_ID` | Single line text | ID printed on the tag, e.g. PB-ALPHA-001 |
| `Deep_Link_URL` | URL | For Pebblebee: the app deep link. Leave blank for AirTag. |
| `Notes` | Long text | Raw notes about the car — the AI reads this to generate a pitch |

### 3. Airtable Formula: Quick Status Summary

Add a **Formula** field called `Summary` to see key info at a glance:
```
CONCATENATE(Year, " ", Make, " ", Model, " (", Status, ") — $", Price)
```

### 4. Get your Airtable Token

1. Go to https://airtable.com/create/tokens
2. Click **+ Create new token**
3. Give it **read + write** scopes on your base
4. Copy the token into your `.env` file

---

## 🔗 Deep Link Reference

| Tracker | Deep Link URL |
|---|---|
| **Apple AirTag** | `findmy://items` — opens Find My app |
| **Pebblebee** | Check app settings for your custom deep link |
| **Tile** | `com.thetileapp.tile://` |
| **Samsung SmartTag** | `smartthings://` |
| **Generic / Custom** | Put any URL in the `Deep_Link_URL` Airtable field |

For AirTag: leave `Deep_Link_URL` blank in Airtable. The app automatically uses `findmy://items`.
For all others: put the deep link in the `Deep_Link_URL` field — it overrides the default.

---

## 🚀 Setup & Run

### 1. Install Python dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure environment variables

```bash
cp .env.example .env
# Edit .env with your real values
```

### 3. Run locally

```bash
# Load .env automatically
python -c "from dotenv import load_dotenv; load_dotenv()"
python app.py
```

Or with python-dotenv auto-loading:
```bash
flask --app app run --debug
```

Visit http://localhost:5000 — log in with your `ACCESS_PASSWORD`.

### 4. Test a vehicle scan

With a vehicle record where `Internal_ID = CAR-01`:
```
http://localhost:5000/scan/CAR-01
```

---

## 📱 QR Code Generation

Generate a QR code for each vehicle pointing to:
```
https://yourdomain.com/scan/CAR-01
```

**With URL token (no login popup):**
```
https://yourdomain.com/scan/CAR-01?token=YourURLSecretToken
```

### Recommended QR generators:
- **qr.io** — bulk generation from CSV
- **goqr.me** — quick single codes
- **Python script** using `qrcode` library:

```python
import qrcode
vehicles = ["CAR-01", "CAR-02", "CAR-03"]
base_url = "https://yourdomain.com/scan"
token = "YourURLSecretToken"

for vid in vehicles:
    img = qrcode.make(f"{base_url}/{vid}?token={token}")
    img.save(f"qr_{vid}.png")
```

---

## ☁️ Production Deployment (Render / Railway / Fly.io)

### Render (free tier available)

1. Push code to GitHub
2. New Web Service → connect your repo
3. Build Command: `pip install -r requirements.txt`
4. Start Command: `gunicorn app:app`
5. Add all env vars in the Render dashboard

### Environment Variables to Set in Production

```
FLASK_SECRET_KEY       (long random string)
AIRTABLE_TOKEN
AIRTABLE_BASE_ID
AIRTABLE_TABLE
OPENAI_API_KEY
ACCESS_PASSWORD
URL_SECRET_TOKEN       (optional)
```

---

## 📁 Project Structure

```
dealer-tracker/
├── app.py                  ← Flask backend (all routes + API calls)
├── requirements.txt
├── .env.example            ← Copy to .env and fill in values
└── templates/
    ├── login.html          ← Auth gate
    ├── dashboard.html      ← Fleet overview
    ├── vehicle.html        ← QR scan landing page (main screen)
    ├── 404.html
    └── 403.html
```

---

## 🔐 Security Notes

- All routes are protected by `@login_required` — unauthenticated users always hit the login page
- Sessions are server-signed with `FLASK_SECRET_KEY`
- `URL_SECRET_TOKEN` allows QR codes to auto-authenticate without a login prompt — keep this token secret and rotate it periodically
- For production, add HTTPS (required for `findmy://` deep links on iOS)
