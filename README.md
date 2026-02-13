# Level Health Broker Portal

## Local Run

1. Backend (FastAPI)

```bash
cd backend
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python main.py
```

API runs at `http://localhost:8000`.

2. Frontend (Vite)

```bash
cd frontend
npm install
npm run dev
```

Frontend runs at `http://localhost:5173`.

## Cloud Deploy (Render + Vercel)

This repo includes:
- `render.yaml` for backend deployment on Render
- `frontend/vercel.json` for frontend deployment on Vercel

### 1) Push this repo to GitHub

```bash
git add .
git commit -m "Prepare cloud deployment"
git push
```

### 2) Deploy backend on Render

1. In Render, click **New +** -> **Blueprint**.
2. Select this GitHub repo.
3. Render will detect `render.yaml`.
4. Before applying, set:
   - `FRONTEND_BASE_URL` to your Vercel URL (example: `https://broker-portal.vercel.app`)
   - `ALLOWED_ORIGINS` to your Vercel URL (example: `https://broker-portal.vercel.app`)
   - `ALLOWED_ORIGIN_REGEX` for preview deploys (example: `https://.*\\.vercel\\.app`)
5. Deploy.
6. After deploy, copy the backend URL (example: `https://level-health-backend.onrender.com`).

### 3) Point Vercel rewrites to backend

Edit `frontend/vercel.json` and replace:
- `https://YOUR-RENDER-BACKEND.onrender.com`

with your real Render backend URL.

Commit and push that change.

### 4) Deploy frontend on Vercel

1. In Vercel, click **Add New** -> **Project**.
2. Import this GitHub repo.
3. Set **Root Directory** to `frontend`.
4. Deploy.

### 5) Final env check

In Render service env vars, confirm:
- `FRONTEND_BASE_URL=https://broker-portal.vercel.app`
- `ALLOWED_ORIGINS=https://broker-portal.vercel.app`
- `ALLOWED_ORIGIN_REGEX=https://.*\\.vercel\\.app`
- `DB_PATH=/var/data/app.db`
- `UPLOADS_DIR=/var/data/uploads`
- `SESSION_COOKIE_SECURE=true`
- `SESSION_COOKIE_SAMESITE=lax`
- `HUBSPOT_IMPLEMENTATION_FORM_URL=https://share.hsforms.com/...`
- `HUBSPOT_IMPLEMENTATION_FORM_PORTAL_ID=7106327`
- `HUBSPOT_IMPLEMENTATION_FORM_ID=f215c8d6-451d-4b7b-826f-fdab43b80369`
- `HUBSPOT_IMPLEMENTATION_FORM_REGION=na1`

## Important Production Notes

- This app currently uses SQLite.
- Render disk is configured for persistence at `/var/data`.
- If you later scale to multiple instances, move DB to managed Postgres.
