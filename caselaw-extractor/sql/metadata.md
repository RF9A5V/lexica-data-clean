## Metadata

Basically each DB generated from this process should have a metadata table that records the following information:

- jurisdiction (either one of the states, like NY, CA, TX, or if federal, the district or circuit, like 2.C for Second Circuit or SDNY for Southern District of New York, and if Supreme Court, US)
- court_level (likely one of appellate, district, supreme, etc.)
- enable_keywords (boolean, whether to enable keyword search)
- enable_holdings (boolean, whether to enable holdings search)
- reporter (the reporter name, like N.Y. or Cal. or Fed. or U.S.)
- note (any additional notes or comments)
