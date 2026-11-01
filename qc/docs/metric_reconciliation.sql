-- Executed independently with SQLite in src/pipeline.py for EACH snapshot.
-- Local `listings` table contains one audited snapshot only.
SELECT COUNT(*) AS listing_count,
       COUNT(DISTINCT host_id) AS host_count,
       AVG(price_cents) / 100.0 AS mean_advertised_price_usd,
       AVG(availability_365) AS mean_availability_365
FROM listings;
SELECT MAX(n) AS largest_host_count
FROM (SELECT host_id, COUNT(*) n FROM listings
      WHERE host_id IS NOT NULL GROUP BY host_id);
-- Largest-host share uses known-host listing count; never all-city/all-quarter
-- portfolios in each neighborhood. Advertised price is not summed as revenue.
