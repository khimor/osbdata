"""
Send welcome email to new subscribers.
Checks for subscribers with welcome_sent=false, sends email, marks as sent.

Usage:
    python scripts/send_welcome.py

Requires env vars: EMAIL_USERNAME, EMAIL_PASSWORD, SUPABASE_URL, SUPABASE_KEY
"""

import os
import sys
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from datetime import datetime, timezone, timedelta

sys.path.insert(0, str(Path(__file__).parent.parent))

SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
DASHBOARD_URL = "https://osbdata.com"

# Top 5 states with latest data (hardcoded for speed - no CSV parsing needed)
LATEST_STATES = [
    {"code": "NY", "name": "New York", "period": "Feb 2026", "handle": "$2.0B", "ggr": "$177M", "hold": "8.8%"},
    {"code": "IL", "name": "Illinois", "period": "Jan 2026", "handle": "$1.4B", "ggr": "$130M", "hold": "9.1%"},
    {"code": "NJ", "name": "New Jersey", "period": "Feb 2026", "handle": "$846M", "ggr": "$64M", "hold": "7.6%"},
    {"code": "PA", "name": "Pennsylvania", "period": "Feb 2026", "handle": "$592M", "ggr": "$54M", "hold": "9.2%"},
    {"code": "OH", "name": "Ohio", "period": "Jan 2026", "handle": "$929M", "ggr": "$99M", "hold": "10.7%"},
]


def get_new_subscribers():
    """Get subscribers who haven't received a welcome email yet."""
    from dotenv import load_dotenv
    load_dotenv()

    url = os.environ.get('SUPABASE_URL')
    key = os.environ.get('SUPABASE_KEY')
    if not url or not key:
        print("SUPABASE_URL/KEY not set")
        return []

    from supabase import create_client
    client = create_client(url, key)

    # Get subscribers created in the last hour with welcome_sent = false
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
    resp = client.table('subscribers').select('*').eq('welcome_sent', False).gte('created_at', cutoff).execute()

    return resp.data if resp.data else []


def mark_welcomed(email):
    """Mark subscriber as having received welcome email."""
    from dotenv import load_dotenv
    load_dotenv()

    url = os.environ.get('SUPABASE_URL')
    key = os.environ.get('SUPABASE_KEY')
    if not url or not key:
        return

    from supabase import create_client
    client = create_client(url, key)
    client.table('subscribers').update({'welcome_sent': True}).eq('email', email).execute()


def render_welcome_email(name):
    """Render HTML welcome email."""
    greeting = f"Hi {name}," if name else "Hi there,"

    state_rows = ""
    for s in LATEST_STATES:
        state_rows += f"""
        <tr style="border-bottom: 1px solid #1a1a28;">
          <td style="padding: 10px 12px; color: #6488f0; font-weight: 600; font-family: 'JetBrains Mono', monospace;">{s['code']}</td>
          <td style="padding: 10px 12px; color: #8b8b9e; font-size: 13px;">{s['name']}</td>
          <td style="padding: 10px 12px; color: #e4e4ec; text-align: right; font-family: 'JetBrains Mono', monospace;">{s['handle']}</td>
          <td style="padding: 10px 12px; color: #e4e4ec; text-align: right; font-family: 'JetBrains Mono', monospace;">{s['ggr']}</td>
          <td style="padding: 10px 12px; color: #e4e4ec; text-align: right; font-family: 'JetBrains Mono', monospace;">{s['hold']}</td>
        </tr>"""

    return f"""
<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1.0"></head>
<body style="margin: 0; padding: 0; background-color: #08080c; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;">
  <div style="max-width: 640px; margin: 0 auto; padding: 24px 16px;">

    <div style="padding: 24px; background: #0f0f15; border: 1px solid #1a1a28; border-radius: 8px; margin-bottom: 16px;">
      <h1 style="margin: 0 0 4px 0; font-size: 18px; font-weight: 600; color: #e4e4ec;">OSB Tracker</h1>
      <p style="margin: 0; font-size: 13px; color: #55556a;">Welcome to OSB Data</p>
    </div>

    <div style="padding: 20px 24px; background: #0f0f15; border: 1px solid #1a1a28; border-radius: 8px; margin-bottom: 16px;">
      <p style="margin: 0 0 16px 0; font-size: 14px; color: #8b8b9e;">{greeting}</p>
      <p style="margin: 0 0 12px 0; font-size: 14px; color: #e4e4ec; line-height: 1.6;">
        Thanks for joining. You now have access to real-time regulatory data across all US sports betting states - handle, GGR, operator market share, and more.
      </p>
      <p style="margin: 0; font-size: 14px; color: #e4e4ec; line-height: 1.6;">
        Here is a snapshot of the latest data:
      </p>
    </div>

    <div style="background: #0f0f15; border: 1px solid #1a1a28; border-radius: 8px; overflow: hidden; margin-bottom: 16px;">
      <table style="width: 100%; border-collapse: collapse; font-size: 13px;">
        <thead>
          <tr style="border-bottom: 1px solid #2a2a3c;">
            <th style="padding: 10px 12px; text-align: left; font-size: 11px; text-transform: uppercase; letter-spacing: 0.05em; color: #55556a; font-weight: 500;">State</th>
            <th style="padding: 10px 12px; text-align: left; font-size: 11px; text-transform: uppercase; letter-spacing: 0.05em; color: #55556a; font-weight: 500;">Name</th>
            <th style="padding: 10px 12px; text-align: right; font-size: 11px; text-transform: uppercase; letter-spacing: 0.05em; color: #55556a; font-weight: 500;">Handle</th>
            <th style="padding: 10px 12px; text-align: right; font-size: 11px; text-transform: uppercase; letter-spacing: 0.05em; color: #55556a; font-weight: 500;">GGR</th>
            <th style="padding: 10px 12px; text-align: right; font-size: 11px; text-transform: uppercase; letter-spacing: 0.05em; color: #55556a; font-weight: 500;">Hold</th>
          </tr>
        </thead>
        <tbody>{state_rows}</tbody>
      </table>
    </div>

    <div style="padding: 20px 24px; background: #0f0f15; border: 1px solid #1a1a28; border-radius: 8px; margin-bottom: 16px;">
      <p style="margin: 0 0 12px 0; font-size: 14px; color: #e4e4ec; line-height: 1.6;">
        Explore the full dashboard to compare states, track operators, and drill into individual markets. Every data point links back to its original regulatory source for full transparency.
      </p>
      <p style="margin: 0; font-size: 14px; color: #8b8b9e; line-height: 1.6;">
        Data updates automatically as states publish new filings. We will email you when new data drops.
      </p>
    </div>

    <div style="text-align: center; padding: 20px;">
      <a href="{DASHBOARD_URL}/app" style="display: inline-block; padding: 14px 32px; background: #6488f0; color: #ffffff; text-decoration: none; border-radius: 8px; font-size: 15px; font-weight: 600;">Explore Dashboard</a>
    </div>

    <div style="padding: 16px 0; text-align: center; border-top: 1px solid #1a1a28;">
      <p style="margin: 0 0 8px 0; font-size: 13px; color: #8b8b9e;">Questions or feedback? Just reply to this email.</p>
      <p style="margin: 0; font-size: 12px; color: #55556a;">Khimor - OSB Data Team</p>
    </div>

  </div>
</body>
</html>"""


def send_email(to_email, html_body):
    username = os.environ.get("EMAIL_USERNAME")
    password = os.environ.get("EMAIL_PASSWORD")
    if not username or not password:
        print("EMAIL credentials not set")
        return False

    msg = MIMEMultipart("alternative")
    msg["Subject"] = "Welcome to OSB Data"
    msg["From"] = f"OSB Tracker <{username}>"
    msg["To"] = to_email
    msg["Reply-To"] = "khimor@osbdata.com"

    msg.attach(MIMEText("Welcome to OSB Data. Visit osbdata.com/app to explore.", "plain"))
    msg.attach(MIMEText(html_body, "html"))

    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(username, password)
            server.sendmail(username, to_email, msg.as_string())
        return True
    except Exception as e:
        print(f"  Failed to send to {to_email}: {e}")
        return False


def main():
    subscribers = get_new_subscribers()
    if not subscribers:
        print("No new subscribers to welcome")
        return

    print(f"Found {len(subscribers)} new subscriber(s)")

    for sub in subscribers:
        email = sub.get('email')
        name = sub.get('name', '')
        if not email:
            continue

        html = render_welcome_email(name)
        if send_email(email, html):
            mark_welcomed(email)
            print(f"  Welcomed: {email}")
        else:
            print(f"  Failed: {email}")


if __name__ == '__main__':
    main()
