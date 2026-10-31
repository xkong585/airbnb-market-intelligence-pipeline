# Original Tableau workbook: deployment checklist (not executed)

- Use city + snapshot_date + neighborhood as the host rollup grain.
- Confirm source vintage and provider release versus actual scrape dates.
- Match listing count, known host count, largest-host share, advertised price
  denominator, and availability to the corresponding snapshot SQL.
- Preserve negative/suppressed/top-coded income reasons and ACS 90% MOE.
- Do not label availability as observed occupancy or estimated revenue as realized.
- Check all fields after Snowflake schema refresh; root mart views retain the
  legacy definitions until reviewed against the new host snapshot grain.

The published new evidence is the standalone QC slice, not a completed check of
old Tableau numbers. No new Tableau credentials/workbook were available here.
