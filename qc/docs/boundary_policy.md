# Geographic contract and boundary policy

2023 Census cartographic tract boundaries (1:500,000), selected to California
county 075 (San Francisco), transformed from the declared source CRS to EPSG:4326.
Validate geometry and unique string GEOID before point-in-polygon association.
Use polygon `covers(point)` to include boundary points. Record 0, 1 and >1 matches;
for >1 matches choose the lexicographically smallest GEOID deterministically and
preserve match_count. Invalid geometries fail; never silently repair geometry.

Cartographic boundaries are generalized, not full TIGER/Line precision; coastal
and boundary discrepancies are expected. Inside Airbnb coordinates are anonymized.
Assignments are approximate neighborhood context, not verified property addresses.
No real-address inference. Geographic mismatch is reported, not imputed.

ACS vintage 2023 represents 2019-2023 estimates, in 2023 inflation-adjusted USD.
It is not 2026 quarter income or host income. Retain estimate and 90% margin of
error raw tokens; negative sentinel codes become missing with reason recorded.
Do not take an absolute value or strip the minus sign. Top-coded value 250001
is retained as an annotation, not an exact $250001 estimate. Margins of error
retain their special annotations. No negative sentinels enter aggregates.
