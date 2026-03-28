# Research Log — US State Sports Betting Revenue Data

## Summary
- States researched: 34/33
- States with sample data downloaded: 24
- States with operator breakdown confirmed: 19
- States with sport breakdown confirmed: 11
- States that need manual review: 4
- Total sample files collected: 39
- Total screenshots captured: 34
- Research completed: 2026-03-20 11:57 PM

---

### AR — Arkansas
- URL: https://www.dfa.arkansas.gov/office/taxes/excise-tax-administration/miscellaneous-tax/casino-gaming-and-sports-wagering/ (DFA casino gaming page working. Annual report PDF URL returned HTML (may have been moved/restructured).)
- Formats: PDF (annual casino gaming statistics reports)
- Sample files downloaded: 0
- Data fields documented: 3
- Operator breakdown: NO
- Sport breakdown: NO
- Scraping difficulty: high
- Notes: Searched for DFA and Racing Commission monthly reports - found only annual Casino Gaming FYE PDF reports. Annual report PDF URL returned HTML (possible redirect/restructure). Third-party sources (RG.org, BetArkansas.com) provide monthly data compiled from DFA sources but the BetArkansas.com page is 

### AZ — Arizona
- URL: https://gaming.az.gov/resources/reports (working)
- Formats: PDF
- Sample files downloaded: 0
- Data fields documented: 7
- Operator breakdown: YES
- Sport breakdown: NO
- Scraping difficulty: hard
- Notes: WebFetch to gaming.az.gov returned 403. WebSearch found report URLs and structure. curl downloads returned Cloudflare challenge HTML. Confirmed via web search: PDF format only, monthly frequency, operator-by-operator with retail/mobile split.

### CO — Colorado
- URL: https://sbg.colorado.gov/sports-betting-monthly-reports (blocked)
- Formats: PDF
- Sample files downloaded: 0
- Data fields documented: 4
- Operator breakdown: NO
- Sport breakdown: YES
- Scraping difficulty: hard
- Notes: WebFetch returned 403 for sbg.colorado.gov. All curl attempts returned HTML. WebSearch confirmed report structure, sport categories, tax rates, and promo deduction sunset schedule. No sample files could be downloaded.

### CT — Connecticut
- URL: https://portal.ct.gov/dcp/gaming-division/gaming/gaming-revenue-and-statistics (working)
- Formats: CSV, JSON, XML, RDF
- Sample files downloaded: 1
- Data fields documented: 15
- Operator breakdown: YES
- Sport breakdown: NO
- Scraping difficulty: easy
- Notes: Fetched portal.ct.gov page - found DCP gaming division links. Found data.ct.gov CSV endpoint via web search. Downloaded 160-line CSV with all historical data. All 3 operators clearly identified with all financial columns.

### DC — District of Columbia
- URL: https://dclottery.com/olg/financials (working)
- Formats: Image/PNG (reports posted as embedded images on web pages), HTML (web-based viewing)
- Sample files downloaded: 0
- Data fields documented: 6
- Operator breakdown: YES
- Sport breakdown: NO
- Scraping difficulty: high
- Notes: Fetched dclottery.com/olg/financials - confirmed 68 reports available, filterable by month/year, all marked unaudited. Found that reports are embedded PNG images, not text/tables. Searched for operator breakdown details - confirmed per-operator data exists but only in image format. Tax rates confirm

### DE — Delaware
- URL: https://www.delottery.com/Sports-Lottery/Monthly-Net-Proceeds (Original URL returned 404 (truncated URL). Correct URLs working.)
- Formats: HTML (web-based tables), PDF (downloadable fiscal year reports)
- Sample files downloaded: 0
- Data fields documented: 9
- Operator breakdown: YES
- Sport breakdown: NO
- Scraping difficulty: medium
- Notes: Original URL returned 404 (truncated). Found correct URLs: Monthly-Net-Proceeds and FY-specific pages. Fetched FY2025 page - confirmed operator breakdown (Delaware Park, Bally's Dover, Harrington, Retailers) with fields: tickets, sales, amount won, vendor fees, net proceeds, state share, purses, com

### IA — Iowa
- URL: https://irgc.iowa.gov/publications-reports/sports-wagering-revenue (working)
- Formats: PDF
- Sample files downloaded: 2
- Data fields documented: 0
- Operator breakdown: YES
- Sport breakdown: NO
- Scraping difficulty: hard
- Notes: Fetched irgc.iowa.gov page - found monthly and FY report links with media IDs. Downloaded Feb 2026 monthly and FY2026 cumulative PDFs. PDF viewer confirmed 2-page structure: page 1 = 19 casinos with 10 data rows each (total/retail/internet x handle/payouts/net receipts + tax), page 2 = 13 online ope

### IL — Illinois
- URL: https://igb.illinois.gov/sports-wagering/sports-reports.html (working)
- Formats: PDF, CSV
- Sample files downloaded: 0
- Data fields documented: 3
- Operator breakdown: YES
- Sport breakdown: NO
- Scraping difficulty: MEDIUM
- Notes: WebFetch successful for page structure. WebSearch provided tax/operator details. Playwright screenshot captured (with domcontentloaded). No sample files downloaded due to JS-rendered download links.

### IN — Indiana
- URL: https://www.in.gov/igc/publications/monthly-revenue/ (working)
- Formats: PDF, XLSX
- Sample files downloaded: 3
- Data fields documented: 10
- Operator breakdown: YES
- Sport breakdown: YES
- Scraping difficulty: easy
- Notes: WebFetch returned page structure. curl successfully downloaded PDF and XLSX. openpyxl inspection revealed 8 sheets with detailed sports wagering data in sheets 7 and 8. All operator brands and sport categories confirmed.

### KS — Kansas
- URL: https://www.kslottery.com/publications/sports-monthly-revenues/ (working)
- Formats: PDF
- Sample files downloaded: 1
- Data fields documented: 5
- Operator breakdown: YES
- Sport breakdown: NO
- Scraping difficulty: easy
- Notes: Fetched kslottery.com page - found PDF links organized by year (2023-2026). Downloaded Feb 2026 PDF (2 pages). Page 1 confirmed: Casino, Provider, Settled Wagers, Revenues, State Share columns with Retail/Online subtotals. Page 2 has same structure for FY cumulative. All operator pairings documented

### KY — Kentucky
- URL: https://khrc.ky.gov/newstatic_info.aspx?static_ID=694 (working)
- Formats: PDF, Tableau Dashboard
- Sample files downloaded: 1
- Data fields documented: 7
- Operator breakdown: YES
- Sport breakdown: NO
- Scraping difficulty: medium
- Notes: Fetched KHRC page - found Tableau dashboard and PDF links. Downloaded 2023 PDF report (5 pages covering Sept-Dec 2023 data). All operator/venue pairings confirmed from PDF. Glossary definitions extracted from report footer.

### LA — Louisiana
- URL: https://lsp.org/about/leadershipsections/bureau-of-investigations/gaming-enforcement-division/gaming-revenue-reports/ (working)
- Formats: PDF, Excel (XLSX)
- Sample files downloaded: 3
- Data fields documented: 14
- Operator breakdown: NO
- Sport breakdown: YES
- Scraping difficulty: easy-medium
- Notes: Original URL was truncated. Found correct URL at lsp.org gaming-revenue-reports page. Found both PDF and Excel formats for mobile and retail sportsbook data. Downloaded Feb 2026 mobile PDF, mobile XLSX, and retail XLSX. PDF confirmed rich data: monthly breakdown, FY/CY cumulative, sport-by-sport spl

### MA — Massachusetts
- URL: https://massgaming.com/regulations/revenue/ (working)
- Formats: PDF
- Sample files downloaded: 2
- Data fields documented: 6
- Operator breakdown: YES
- Sport breakdown: NO
- Scraping difficulty: easy-medium
- Notes: WebFetch returned page structure with operator list and report format. curl downloaded summary PDF and Year One PDF successfully. PDF inspection confirmed infographic + tabular structure. WebSearch confirmed tax rates, categories, and operator list.

### MD — Maryland
- URL: https://www.mdgaming.com/maryland-sports-wagering/revenue-reports/ (working)
- Formats: HTML, Excel
- Sample files downloaded: 0
- Data fields documented: 9
- Operator breakdown: YES
- Sport breakdown: YES
- Scraping difficulty: medium
- Notes: WebFetch returned page structure with summary data. WebSearch confirmed operators, tax rates, sport categories. Tax rate change from 15% to 20% for mobile confirmed effective June 1, 2025.

### ME — Maine
- URL: https://www.maine.gov/dps/gcu/sports-wagering/sports-wagering-revenue (working)
- Formats: PDF
- Sample files downloaded: 1
- Data fields documented: 12
- Operator breakdown: YES
- Sport breakdown: NO
- Scraping difficulty: easy-medium
- Notes: Fetched maine.gov sports wagering revenue page - found 4 operators with individual PDF reports. Downloaded Passamaquoddy Feb 2026 PDF - confirmed single-page layout with monthly columns, clear AGR formula. Searched for tax rates (10%), promo deductions, GGR formula. Very structured data.

### MI — Michigan
- URL: https://www.michigan.gov/mgcb/detroit-casinos/resources/revenues-and-wagering-tax-information (blocked_403_for_fetch)
- Formats: Excel
- Sample files downloaded: 1
- Data fields documented: 4
- Operator breakdown: YES
- Sport breakdown: NO
- Scraping difficulty: MEDIUM
- Notes: WebFetch returned 403. WebSearch provided operator/tax details. Playwright screenshot captured successfully. Authorized operators Excel downloaded. Revenue Excel file download failed (404 - URL path may have changed).

### MO — Missouri
- URL: https://www.mgc.dps.mo.gov/SportsWagering/sw_financials/rb_SWFin_main.html (working)
- Formats: PDF, Excel (XLSX/XLS)
- Sample files downloaded: 4
- Data fields documented: 0
- Operator breakdown: YES
- Sport breakdown: YES
- Scraping difficulty: easy
- Notes: Fetched MGC sports wagering page - found 2 report types (Monthly Financials, Revenue Detail) in PDF+Excel. Downloaded all 4 files for Jan 2026. Confirmed structure from PDF: Summary has Retail/Mobile with operator-level breakdown; Detail has per-operator per-sport breakdown with 10 sport categories.

### MS — Mississippi
- URL: https://www.msgamingcommission.com/reports/monthly_reports (working)
- Formats: PDF, Excel (XLSX)
- Sample files downloaded: 1
- Data fields documented: 8
- Operator breakdown: NO
- Sport breakdown: YES
- Scraping difficulty: low-medium
- Notes: Fetched msgamingcommission.com/reports/monthly_reports - confirmed PDF + Excel formats available. Fetched monthly_details/1166 (June 2024) - got exact download URLs. Direct curl download returned HTML (blocked). Used browser user-agent to download PDF successfully (3 pages). Read PDF - confirmed 3-r

### MT — Montana
- URL: https://sportsbetmontana.com/en/view/news (working)
- Formats: PDF
- Sample files downloaded: 2
- Data fields documented: 10
- Operator breakdown: NO
- Sport breakdown: YES
- Scraping difficulty: low
- Notes: Fetched sportsbetmontana.com/en/view/news - found weekly and monthly PDF links. Downloaded January 2025 monthly and WE 1.18.25 weekly PDFs successfully. Read January 2025 PDF - confirmed sport-level breakdown with Handle/Handle%/Payout/GGR columns. DOJ Gambling Control Division site does NOT have sp

### NC — North Carolina
- URL: https://ncgaming.gov/about/reports (working (reports page may render as image; /reports/ alternate URL has proper links))
- Formats: PDF
- Sample files downloaded: 1
- Data fields documented: 8
- Operator breakdown: NO
- Sport breakdown: NO
- Scraping difficulty: easy-medium
- Notes: Fetched ncgaming.gov/about/reports - page rendered as image (Adobe Express). Used alternate URL ncgaming.gov/reports/ which had proper HTML links. Downloaded Mar 2026 PDF report. PDF has FY2026 data (July 2025 - Feb 2026) on page 1 with 7 columns, and FY2025 definitions on page 2. Clean tabular form

### NH — New Hampshire
- URL: https://www.nhlottery.com/About-Us/Financial-Reports (working)
- Formats: PDF
- Sample files downloaded: 1
- Data fields documented: 3
- Operator breakdown: NO
- Sport breakdown: NO
- Scraping difficulty: easy
- Notes: Fetched nhlottery.com financial reports page - found FY20-FY26 PDFs. Downloaded FY26 PDF - confirmed simple 3-column layout (Mobile/Retail/Combined) with Handle, GGR, State Rev Share. Searched for revenue share details (51% mobile). Very clean data format.

### NJ — New Jersey
- URL: https://www.njoag.gov/about/divisions-and-offices/division-of-gaming-enforcement-home/financial-and-statistical-information/ (blocked_403)
- Formats: PDF
- Sample files downloaded: 1
- Data fields documented: 18
- Operator breakdown: YES
- Sport breakdown: YES
- Scraping difficulty: EASY
- Notes: WebFetch returned 403 for main page. PDF downloaded successfully via curl. Full 8-page PDF analyzed with detailed operator, sport, tax data. WebSearch provided tax rate/GGR details. Playwright screenshot captured.

### NV — Nevada
- URL: https://gaming.nv.gov/about/gaming-revenue/information/ (working)
- Formats: PDF
- Sample files downloaded: 1
- Data fields documented: 16
- Operator breakdown: NO
- Sport breakdown: YES
- Scraping difficulty: medium
- Notes: Fetched gaming.nv.gov/about/gaming-revenue/information - confirmed monthly PDF reports from 2004-present. Downloaded 2025oct-gri.pdf (465KB, 48 pages). Read pages 1-5 - confirmed structure: cover page, introduction, table of contents, then data pages. Statewide page shows sports categories: Race Boo

### NY — New York
- URL: https://gaming.ny.gov/revenue-reports (working)
- Formats: PDF, Excel
- Sample files downloaded: 0
- Data fields documented: 5
- Operator breakdown: YES
- Sport breakdown: NO
- Scraping difficulty: MEDIUM
- Notes: WebFetch successful for page structure. WebSearch provided tax/GGR details. Playwright screenshot captured. Sample file download failed due to 403.

### OH — Ohio
- URL: https://casinocontrol.ohio.gov/about/revenue-reports (working)
- Formats: PDF
- Sample files downloaded: 1
- Data fields documented: 10
- Operator breakdown: YES
- Sport breakdown: NO
- Scraping difficulty: EASY
- Notes: WebFetch returned 404 for main page. PDF downloaded successfully from CDN. Full 10-page PDF analyzed with 8 monthly tabs + summary + revenue notes. WebSearch provided tax/operator details. Playwright screenshot captured. EASY scraping target.

### OR — Oregon
- URL: https://www.oregonlottery.org/about/lottery-news/press-releases/ (working (press releases page). Original URL is Oregon Digital Collections - not primary source for revenue data.)
- Formats: PDF (annual comprehensive financial report), Press releases (HTML)
- Sample files downloaded: 0
- Data fields documented: 4
- Operator breakdown: NO
- Sport breakdown: NO
- Scraping difficulty: high
- Notes: Fetched oregonlottery.org press releases - found Super Bowl recap-style press releases, not structured data. Searched for official monthly downloads - none found. Oregon Lottery does not publish monthly structured data files. Annual ACFR available from Secretary of State. Third-party sites (RG.org, 

### PA — Pennsylvania
- URL: https://gamingcontrolboard.pa.gov/news-and-transparency/revenue (working)
- Formats: PDF, Excel
- Sample files downloaded: 2
- Data fields documented: 6
- Operator breakdown: YES
- Sport breakdown: NO
- Scraping difficulty: EASY
- Notes: WebFetch successful. WebSearch provided tax/revenue details. Both PDF and Excel downloaded successfully. PA PDF fully read and analyzed. Playwright screenshot captured. EASY scraping target.

### RI — Rhode Island
- URL: https://www.rilot.com/en-us/about-us/financials.html (working)
- Formats: PDF
- Sample files downloaded: 1
- Data fields documented: 3
- Operator breakdown: NO
- Sport breakdown: NO
- Scraping difficulty: easy
- Notes: Fetched rilot.com financials page - found FY2019-FY2026 PDFs. Downloaded Jan FY26 PDF - confirmed facility breakdown (Twin River, Tiverton, Online, Combined) with Write/Payout/Book Revenue columns. Searched for revenue sharing structure (51/32/17 split). Very clean single-page PDF format.

### SD — South Dakota
- URL: https://dor.sd.gov/businesses/gaming/ (working)
- Formats: PDF
- Sample files downloaded: 1
- Data fields documented: 10
- Operator breakdown: NO
- Sport breakdown: YES
- Scraping difficulty: low-medium
- Notes: Fetched dor.sd.gov/businesses/gaming - found monthly gaming statistics PDFs. Downloaded jan-2025-gaming-stats.pdf successfully (118KB, valid PDF). Read PDF - confirmed 2-page structure with sports wagering summary on page 1 and detailed sport-by-sport breakdown on page 2. Sports categories include 1

### TN — Tennessee
- URL: https://www.tn.gov/swac/reports.html (working)
- Formats: PDF, CSV
- Sample files downloaded: 2
- Data fields documented: 4
- Operator breakdown: NO
- Sport breakdown: NO
- Scraping difficulty: easy
- Notes: Fetched tn.gov/swac/reports.html - found PDF and CSV links organized by year. Downloaded Feb 2025 PDF and Feb 2026 CSV. PDF confirms minimal data: just gross wagers, adjustments, handle, and privilege tax. CSV has same 4 fields in a non-standard CSV format. URL patterns vary slightly between years (

### VA — Virginia
- URL: https://www.valottery.com/aboutus/casinosandsportsbetting/sportsbettingrevenue (working)
- Formats: PDF
- Sample files downloaded: 1
- Data fields documented: 10
- Operator breakdown: NO
- Sport breakdown: NO
- Scraping difficulty: medium-hard
- Notes: WebFetch to provided URL confirmed it was the wrong page (lottery winners). WebSearch found correct URL and report location. Downloaded November 2025 report PDF from rga.lis.virginia.gov. PDF confirmed formal letter format with aggregate-only data tables.

### VT — Vermont
- URL: https://liquorandlottery.vermont.gov/sports-wagering (returned 403 Forbidden when fetched (may require browser access or have bot protection))
- Formats: PDF (annual report), HTML (website data)
- Sample files downloaded: 1
- Data fields documented: 5
- Operator breakdown: NO
- Sport breakdown: YES
- Scraping difficulty: high
- Notes: liquorandlottery.vermont.gov returned 403 Forbidden. Downloaded annual report PDF from Vermont Legislature documents (1.2MB, valid PDF). Read pages 1-15 - confirmed FY2024 report covering Jul 2023-Jun 2024. Sports Wagering section on page 12: FanDuel, DraftKings, Fanatics selected. Basketball highes

### WV — West Virginia
- URL: https://business.wvlottery.com/resourcesPayments#other-reports (working - page loads but report downloads are in .zip format)
- Formats: ZIP (containing unknown inner format, likely CSV or Excel)
- Sample files downloaded: 1
- Data fields documented: 3
- Operator breakdown: NO
- Sport breakdown: NO
- Scraping difficulty: medium
- Notes: Fetched business.wvlottery.com - found Sports Wagering Weekly Summary as ZIP download. Searched for operator list, tax rates, GGR formula. WV Code 29-22D confirmed AGR definition and 10% tax. License list PDF download returned HTML (may need auth). No sport breakdown or operator breakdown in public 

### WY — Wyoming
- URL: https://gaming.wyo.gov/revenue-reports/financial-reports/combined-wagering-activity-reports (partially working - page loads but requires JavaScript to render report links)
- Formats: PDF
- Sample files downloaded: 0
- Data fields documented: 5
- Operator breakdown: YES
- Sport breakdown: NO
- Scraping difficulty: hard
- Notes: Fetched gaming.wyo.gov pages - JS rendering prevented full content extraction. Searched for report format details, operator list, tax rates. Found 5 operators (DraftKings, FanDuel, BetMGM, Caesars, Fanatics). Confirmed 10% tax rate, online-only model. Could not download sample reports due to JS-rend
