-- Run in Supabase SQL Editor
CREATE TABLE IF NOT EXISTS contacts (
  id bigserial PRIMARY KEY,
  email text NOT NULL,
  name text,
  company text,
  message text,
  source text DEFAULT 'website',
  created_at timestamptz DEFAULT now()
);

ALTER TABLE contacts ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Service role full access" ON contacts
  FOR ALL USING (true) WITH CHECK (true);

CREATE POLICY "Public insert" ON contacts
  FOR INSERT WITH CHECK (true);
