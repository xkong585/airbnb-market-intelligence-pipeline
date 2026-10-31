# Investigations resolved before publication

1. **Gate stopped on missing minimum nights.** Official June file has 4 null
   rows; September has 11. Resolution: quarantine those rows with reason,
   retain the original failed-run log, no zero fill and no unexplained loss.
   Nonmissing negative/zero/fractional minimum nights still block publication.
2. **Gate stopped on exact scrape-day equality.** June release includes 1,620
   June 22 rows and 5,712 June 14 rows. The source contract requires quarter
   alignment, not same-day collection. Preserve row dates; assert same quarter,
   publish min/max scrape dates. This is coverage timing, not demand growth.
3. **API returned HTML Missing Key.** Replaced the unavailable key-gated API
   with the official B19013 bulk table. Source lock includes only actual data;
   downloader rejects HTML. No fabricated Census records or borrowed API key.
4. **ACS sentinels and top codes.** Seven tract estimates use -666666666; six
   are 250001 (top-coded). The original regex would strip a minus. New code
   keeps raw values/reasons and MOE, and omits censored/special values from
   numeric summaries. Six MOEs are -333333333 and seven are -222222222.
5. **Spatial nonmatches.** One listing in each snapshot is outside the selected
   generalized tract polygons. Kept as unmatched with null area attributes.
   No attempt to infer a real property address; match rates are not forced to 100%.

Fault-injection tests are software tests, not production incidents. No savings,
revenue improvement, cloud deployment, or Tableau reconciliation is claimed.
