-- OSB Tracker - Supabase Table Setup
-- Run this in the Supabase SQL Editor (https://supabase.com/dashboard/project/yjrfmlcfvogsfodgmcfw/sql)

-- 1. Main data table
CREATE TABLE IF NOT EXISTS monthly_data (
  id bigserial PRIMARY KEY,
  state_code text NOT NULL,
  period_start date,
  period_end date NOT NULL,
  period_type text NOT NULL DEFAULT 'monthly',
  operator_raw text,
  operator_reported text,
  operator_standard text,
  parent_company text,
  channel text,
  sport_category text,
  handle bigint,
  gross_revenue bigint,
  standard_ggr bigint,
  promo_credits bigint,
  net_revenue bigint,
  payouts bigint,
  tax_paid bigint,
  federal_excise_tax bigint,
  hold_pct double precision,
  days_in_period integer,
  is_partial_period boolean DEFAULT false,
  data_is_revised boolean DEFAULT false,
  source_file text,
  source_sheet text,
  source_row integer,
  source_column text,
  source_page integer,
  source_table_index integer,
  source_url text,
  source_report_url text,
  source_screenshot text,
  source_raw_line text,
  source_context text,
  scrape_timestamp timestamptz,
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now()
);

-- Composite unique constraint for upsert
ALTER TABLE monthly_data ADD CONSTRAINT monthly_data_upsert_key
  UNIQUE (state_code, period_end, period_type, operator_standard, channel, sport_category);

-- Indexes for fast queries
CREATE INDEX IF NOT EXISTS idx_monthly_data_state ON monthly_data (state_code);
CREATE INDEX IF NOT EXISTS idx_monthly_data_period ON monthly_data (period_end);
CREATE INDEX IF NOT EXISTS idx_monthly_data_operator ON monthly_data (operator_standard);
CREATE INDEX IF NOT EXISTS idx_monthly_data_state_period ON monthly_data (state_code, period_end);

-- Auto-update updated_at on modification
CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER monthly_data_updated_at
  BEFORE UPDATE ON monthly_data
  FOR EACH ROW EXECUTE FUNCTION update_updated_at();

-- 2. Subscribers table
CREATE TABLE IF NOT EXISTS subscribers (
  id bigserial PRIMARY KEY,
  email text NOT NULL UNIQUE,
  name text,
  states jsonb DEFAULT '"all"'::jsonb,
  frequency text DEFAULT 'immediate',
  active boolean DEFAULT true,
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now()
);

CREATE TRIGGER subscribers_updated_at
  BEFORE UPDATE ON subscribers
  FOR EACH ROW EXECUTE FUNCTION update_updated_at();

-- Insert initial subscriber
INSERT INTO subscribers (email, name, states, frequency)
VALUES ('nosherzapoo@gmail.com', 'Nosher', '"all"'::jsonb, 'immediate')
ON CONFLICT (email) DO NOTHING;

-- 3. Scrape logs table
CREATE TABLE IF NOT EXISTS scrape_logs (
  id bigserial PRIMARY KEY,
  state_code text NOT NULL,
  tier integer,
  started_at timestamptz DEFAULT now(),
  completed_at timestamptz,
  rows_scraped integer DEFAULT 0,
  new_data boolean DEFAULT false,
  status text DEFAULT 'running',
  error_message text,
  created_at timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_scrape_logs_state ON scrape_logs (state_code, started_at DESC);

-- 4. Row Level Security (RLS)
-- Enable RLS on all tables
ALTER TABLE monthly_data ENABLE ROW LEVEL SECURITY;
ALTER TABLE subscribers ENABLE ROW LEVEL SECURITY;
ALTER TABLE scrape_logs ENABLE ROW LEVEL SECURITY;

-- Public read access for monthly_data (API clients can read)
CREATE POLICY "Public read access" ON monthly_data
  FOR SELECT USING (true);

-- Service role can do everything
CREATE POLICY "Service role full access" ON monthly_data
  FOR ALL USING (true) WITH CHECK (true);

CREATE POLICY "Service role full access" ON subscribers
  FOR ALL USING (true) WITH CHECK (true);

CREATE POLICY "Service role full access" ON scrape_logs
  FOR ALL USING (true) WITH CHECK (true);
