-- Migration: add CSV-equivalent catalogue fields to products table
-- Run this once in the Supabase SQL editor

ALTER TABLE products
  ADD COLUMN IF NOT EXISTS level_6               TEXT DEFAULT '',
  ADD COLUMN IF NOT EXISTS collection_description       TEXT DEFAULT '',
  ADD COLUMN IF NOT EXISTS collection_editorial_url     TEXT DEFAULT '',
  ADD COLUMN IF NOT EXISTS collection_editorial_type    TEXT DEFAULT 'image',
  ADD COLUMN IF NOT EXISTS is_homepage           BOOLEAN DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS price_visible         BOOLEAN DEFAULT TRUE,
  ADD COLUMN IF NOT EXISTS min_order_qty         INTEGER DEFAULT 1,
  ADD COLUMN IF NOT EXISTS sold_out              BOOLEAN DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS show_product          BOOLEAN DEFAULT TRUE;

-- Public read-only API: allow anonymous reads of approved products
-- (run this only if you want a public /api/catalogue endpoint without auth)
-- ALTER TABLE products ENABLE ROW LEVEL SECURITY;
-- CREATE POLICY "Public read approved" ON products
--   FOR SELECT USING (status = 'approved' AND show_product = TRUE);
