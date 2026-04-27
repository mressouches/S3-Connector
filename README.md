# S3 Connector

A password-protected Streamlit application to browse and download files from any S3-compatible storage bucket.

---

## Features

- App-level password protection (login screen before anything is accessible)
- Paste S3 credentials directly in the browser — never stored on disk
- Compatible with AWS S3 and any S3-compatible endpoint (OVH, Scaleway, MinIO, Cloudflare R2, etc.)
- Folder tree browser with collapsible directories
- Filter files by name
- One-click file download

---

## Requirements

- Python 3.8+
- pip

---

## Installation

```bash
# Clone or copy the project folder, then:
python -m venv .venv
.venv\Scripts\activate        # Windows
# source .venv/bin/activate   # macOS / Linux

pip install -r requirements.txt
```

---

## Configuration

Copy `.env.example` to `.env` and set a password:

```bash
copy .env.example .env
```

Edit `.env`:

```
APP_PASSWORD=your_secure_password_here
```

> S3 credentials are entered in the UI at runtime and never written to disk.

---

## Running the app

```bash
streamlit run app.py
```

Open [http://localhost:8501](http://localhost:8501) in your browser.

---

## How to use

1. **Login** — enter the app password defined in `.env`
2. **Connect** — fill in the sidebar form:
   - Endpoint URL *(leave blank for AWS S3)*
   - Access Key ID
   - Secret Access Key
   - Bucket name
   - Prefix / folder *(optional)*
3. **Browse** — click folder buttons to expand/collapse directories
4. **Download** — click the download button next to any file

---

## Project structure

```
S3 connector/
├── app.py               # Streamlit entry point
├── s3_client.py         # boto3 wrapper (list objects, download)
├── auth.py              # App password verification
├── requirements.txt     # Python dependencies
├── .env                 # Local secrets (git-ignored)
├── .env.example         # Environment variable template
└── README.md            # This file
```

---

## Security

- `.env` is git-ignored — never commit it
- S3 credentials live in `st.session_state` only, for the duration of the browser session
- The app password is compared with `hmac.compare_digest` to prevent timing attacks
