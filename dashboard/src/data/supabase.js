import { createClient } from '@supabase/supabase-js';

const SUPABASE_URL = 'https://yjrfmlcfvogsfodgmcfw.supabase.co';
const SUPABASE_ANON_KEY = 'sb_publishable_wnNbi50k0OtTabl5iXEEkg_-CvXA47g';

export const supabase = createClient(SUPABASE_URL, SUPABASE_ANON_KEY);

/**
 * Fetch all data from Supabase monthly_data table.
 * Returns array of row objects matching the CSV schema.
 */
export async function fetchAllFromSupabase() {
  const allRows = [];
  let offset = 0;
  const batchSize = 1000;

  while (true) {
    const { data, error } = await supabase
      .from('monthly_data')
      .select('*')
      .range(offset, offset + batchSize - 1)
      .order('state_code')
      .order('period_end');

    if (error) {
      console.warn('Supabase fetch error:', error.message);
      return null; // Signal to fall back to CSV
    }

    if (!data || data.length === 0) break;

    allRows.push(...data);
    offset += batchSize;

    if (data.length < batchSize) break; // Last page
  }

  return allRows.length > 0 ? allRows : null;
}
