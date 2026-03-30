"""
Check for new contact form submissions and email a notification.
Runs alongside the welcome email workflow.
"""

import os
import sys
import smtplib
from email.mime.text import MIMEText
from pathlib import Path
from datetime import datetime, timezone, timedelta

sys.path.insert(0, str(Path(__file__).parent.parent))

NOTIFY_EMAIL = "khimor@osbdata.com"


def main():
    from dotenv import load_dotenv
    load_dotenv()

    url = os.environ.get('SUPABASE_URL')
    key = os.environ.get('SUPABASE_KEY')
    if not url or not key:
        print("SUPABASE_URL/KEY not set")
        return

    from supabase import create_client
    client = create_client(url, key)

    # Get contacts from the last 35 minutes (runs every 30 min with some overlap)
    cutoff = (datetime.now(timezone.utc) - timedelta(minutes=35)).isoformat()
    resp = client.table('contacts').select('*').gte('created_at', cutoff).order('created_at', desc=True).execute()

    if not resp.data:
        print("No new contacts")
        return

    print(f"Found {len(resp.data)} new contact(s)")

    for contact in resp.data:
        email = contact.get('email', '?')
        name = contact.get('name', '-')
        company = contact.get('company', '-')
        message = contact.get('message', '-')
        source = contact.get('source', 'website')
        created = contact.get('created_at', '')[:19]

        body = f"""New {source.replace('_', ' ')} submission on OSB Data:

Name: {name}
Email: {email}
Company: {company}
Message: {message}

Submitted: {created} UTC
"""

        subject = f"OSB Data - New {source.replace('_', ' ')}: {email}"

        username = os.environ.get("EMAIL_USERNAME")
        password = os.environ.get("EMAIL_PASSWORD")
        if not username or not password:
            print(f"  {email}: no email credentials, skipping notification")
            continue

        msg = MIMEText(body)
        msg["Subject"] = subject
        msg["From"] = f"OSB Tracker <{username}>"
        msg["To"] = NOTIFY_EMAIL

        try:
            with smtplib.SMTP("smtp.gmail.com", 587) as s:
                s.starttls()
                s.login(username, password)
                s.send_message(msg)
            print(f"  Notified about: {email} ({source})")
        except Exception as e:
            print(f"  Failed to notify about {email}: {e}")


if __name__ == '__main__':
    main()
