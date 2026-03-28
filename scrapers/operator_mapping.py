"""
Operator name normalization for US sports betting scrapers.
Maps raw operator names (as they appear in state reports) to standardized names and parent companies.
"""

# (raw_name) -> (standard_name, parent_company)
OPERATOR_MAP = {
    # ---- Flutter Entertainment ----
    'FanDuel': ('FanDuel', 'Flutter Entertainment'),
    'FanDuel Sportsbook': ('FanDuel', 'Flutter Entertainment'),
    'FanDuel (Phoenix Suns / NBA)': ('FanDuel', 'Flutter Entertainment'),
    'FanDuel (replaced GambetDC in 2024)': ('FanDuel', 'Flutter Entertainment'),
    'Flutter/FanDuel': ('FanDuel', 'Flutter Entertainment'),
    'FanDuel Group': ('FanDuel', 'Flutter Entertainment'),
    'Betfair Interactive US': ('FanDuel', 'Flutter Entertainment'),
    'Betfair Interactive US, LLC': ('FanDuel', 'Flutter Entertainment'),
    'BetFair Interactive US, LLC d/b/a FanDuel Sportsbook': ('FanDuel', 'Flutter Entertainment'),
    'Betfair Interactive US LLC': ('FanDuel', 'Flutter Entertainment'),
    'FanDuel (MotorCity Casino Hotel)': ('FanDuel', 'Flutter Entertainment'),
    'FanDuel Sportsbook (CT)': ('FanDuel', 'Flutter Entertainment'),

    # ---- DraftKings Inc ----
    'DraftKings': ('DraftKings', 'DraftKings Inc'),
    'DraftKings Sportsbook': ('DraftKings', 'DraftKings Inc'),
    'DraftKings Sport Book': ('DraftKings', 'DraftKings Inc'),
    'Crown Gaming / DraftKings': ('DraftKings', 'DraftKings Inc'),
    'Crown Gaming/DraftKings': ('DraftKings', 'DraftKings Inc'),
    'DK-Crown': ('DraftKings', 'DraftKings Inc'),
    'DK Crown Holdings, Inc. d/b/a DraftKings': ('DraftKings', 'DraftKings Inc'),
    'DraftKings (TPC Scottsdale / PGA)': ('DraftKings', 'DraftKings Inc'),
    'DraftKings (Bay Mills Indian Community)': ('DraftKings', 'DraftKings Inc'),
    'Golden Nugget': ('DraftKings (Golden Nugget)', 'DraftKings Inc'),
    'Golden Nugget Online Gaming': ('DraftKings (Golden Nugget)', 'DraftKings Inc'),
    'DraftKings (exclusive sole operator under state contract)': ('DraftKings', 'DraftKings Inc'),
    'DraftKings (sole operator via Oregon Lottery Scoreboard platform)': ('DraftKings', 'DraftKings Inc'),

    # ---- PENN Entertainment ----
    'ESPN Bet': ('ESPN Bet', 'PENN Entertainment'),
    'ESPN BET': ('ESPN Bet', 'PENN Entertainment'),
    'ESPNBet': ('ESPN Bet', 'PENN Entertainment'),
    'ESPN BET (Phoenix Speedway / NASCAR)': ('ESPN Bet', 'PENN Entertainment'),
    'ESPN BET (Penn Entertainment)': ('ESPN Bet', 'PENN Entertainment'),
    'Wynn Interactive': ('ESPN Bet', 'PENN Entertainment'),
    'WynnBET': ('ESPN Bet', 'PENN Entertainment'),
    'Wynn': ('WynnBET', 'PENN Entertainment'),
    'Barstool Sportsbook': ('ESPN Bet', 'PENN Entertainment'),
    'Barstool/ESPN BET': ('ESPN Bet', 'PENN Entertainment'),
    'PENN Entertainment / ESPN Bet': ('ESPN Bet', 'PENN Entertainment'),
    'theScore Bet': ('ESPN Bet', 'PENN Entertainment'),
    'PENN Sports Interactive': ('ESPN Bet', 'PENN Entertainment'),
    'theScore': ('ESPN Bet', 'PENN Entertainment'),
    'ESPN Bet (Penn Entertainment)': ('ESPN Bet', 'PENN Entertainment'),

    # ---- Entain/MGM Resorts JV ----
    'BetMGM': ('BetMGM', 'Entain/MGM Resorts'),
    'BetMGM (Arizona Cardinals / NFL)': ('BetMGM', 'Entain/MGM Resorts'),
    'BetMGM (MGM Grand Detroit)': ('BetMGM', 'Entain/MGM Resorts'),
    'BetMGM, LLC d/b/a BetMGM': ('BetMGM', 'Entain/MGM Resorts'),
    'Borgata Online': ('BetMGM', 'Entain/MGM Resorts'),
    'Borgata': ('BetMGM', 'Entain/MGM Resorts'),
    'Party Poker': ('BetMGM', 'Entain/MGM Resorts'),
    'Roar Digital': ('BetMGM', 'Entain/MGM Resorts'),
    'BetMGM (on-premises mobile)': ('BetMGM', 'Entain/MGM Resorts'),

    # ---- Caesars Entertainment ----
    'Caesars Sportsbook': ('Caesars', 'Caesars Entertainment'),
    'Caesars': ('Caesars', 'Caesars Entertainment'),
    "Caesar's": ('Caesars', 'Caesars Entertainment'),
    'Caesars Sport Book': ('Caesars', 'Caesars Entertainment'),
    'Caesars Sportsbook (Arizona Diamondbacks / MLB)': ('Caesars', 'Caesars Entertainment'),
    'William Hill': ('Caesars', 'Caesars Entertainment'),
    'Caesars Interactive NJ': ('Caesars', 'Caesars Entertainment'),
    'Caesars Interactive': ('Caesars', 'Caesars Entertainment'),
    'American Wagering, Inc. d/b/a Caesars Sportsbook': ('Caesars', 'Caesars Entertainment'),
    'Caesars Sportsbook (on-premises mobile)': ('Caesars', 'Caesars Entertainment'),

    # ---- Rush Street Interactive ----
    'BetRivers': ('BetRivers', 'Rush Street Interactive'),
    'Rush Street Interactive': ('BetRivers', 'Rush Street Interactive'),
    'SugarHouse': ('BetRivers', 'Rush Street Interactive'),
    'Play SugarHouse': ('BetRivers', 'Rush Street Interactive'),
    'BetRivers (Arizona Rattlers / AFL)': ('BetRivers', 'Rush Street Interactive'),
    'Rivers Casino': ('BetRivers', 'Rush Street Interactive'),
    'RSI LA, LLC d/b/a BetRivers Sportsbook': ('BetRivers', 'Rush Street Interactive'),
    'BetRivers (Little Traverse Bay Bands of Odawa Indians)': ('BetRivers', 'Rush Street Interactive'),
    'BetRivers (sole online operator, launched Dec 2023, operated by Rush Street Interactive)': ('BetRivers', 'Rush Street Interactive'),

    # ---- Fanatics ----
    'Fanatics Sportsbook': ('Fanatics', 'Fanatics Inc'),
    'Fanatics': ('Fanatics', 'Fanatics Inc'),
    'Fanatics Sportsbook (Tonto Apache Tribe)': ('Fanatics', 'Fanatics Inc'),
    'PointsBet': ('Fanatics', 'Fanatics Inc'),

    # ---- bet365 ----
    'bet365': ('bet365', 'bet365 Group'),
    'Bet365': ('bet365', 'bet365 Group'),
    'Bet 365': ('bet365', 'bet365 Group'),

    # ---- Hard Rock Digital ----
    'Hard Rock Bet': ('Hard Rock Bet', 'Seminole Tribe / Hard Rock Digital'),
    'Hard Rock': ('Hard Rock Bet', 'Seminole Tribe / Hard Rock Digital'),
    'Hard Rock Digital': ('Hard Rock Bet', 'Seminole Tribe / Hard Rock Digital'),

    # ---- Bally's Corporation ----
    'Bally Bet': ('Bally Bet', "Bally's Corporation"),
    "Bally's": ('Bally Bet', "Bally's Corporation"),
    "Bally's Dover (formerly Dover Downs)": ('Bally Bet', "Bally's Corporation"),
    "Bally Bet (Bally's Interactive, LLC)": ('Bally Bet', "Bally's Corporation"),

    # ---- Smaller / Regional ----
    'Betly': ('Betly', 'Delaware North'),
    'Betly (Southland Casino Racing / Delaware North)': ('Betly', 'Delaware North'),
    'Circa Sports': ('Circa Sports', 'Circa Resort & Casino'),
    'Circa': ('Circa Sports', 'Circa Resort & Casino'),
    'Resorts World Bet': ('Resorts World Bet', 'Genting Group'),
    'FOX Bet': ('FOX Bet (defunct)', 'Flutter Entertainment'),
    'Fubo Sportsbook': ('Fubo Sportsbook (defunct)', 'fuboTV'),
    'Unibet': ('Unibet', 'Kindred Group'),
    'BetFred': ('Betfred', 'Betfred Group'),
    'Betfred': ('Betfred', 'Betfred Group'),
    'SuperBook': ('SuperBook', 'SuperBook Sports'),
    'SuperBook Sports': ('SuperBook', 'SuperBook Sports'),
    'Sporttrade': ('Sporttrade', 'Sporttrade Inc'),
    'BetParx': ('BetParx', 'Greenwood Racing'),
    'Parx Casino': ('BetParx', 'Greenwood Racing'),
    'betPARX': ('BetParx', 'Greenwood Racing'),
    'Desert Diamond Sports': ('Desert Diamond', "Tohono O'odham Nation"),
    "Desert Diamond Sports (Tohono O'odham Tribe)": ('Desert Diamond', "Tohono O'odham Nation"),
    'BetSaracen': ('BetSaracen', 'Saracen Casino Resort'),
    'BetSaracen (Saracen Casino Resort)': ('BetSaracen', 'Saracen Casino Resort'),
    'Oaklawn Sports': ('Oaklawn Sports', 'Oaklawn Racing Casino Resort'),
    'Oaklawn Sports (Oaklawn Racing Casino Resort)': ('Oaklawn Sports', 'Oaklawn Racing Casino Resort'),
    'BetCris': ('BetCris', 'Bookmaker.eu'),

    # ---- CT Tribal License Holders → Consumer Brands ----
    'MPI Master Wagering License CT, LLC': ('DraftKings (CT)', 'DraftKings Inc'),
    'Mohegan Digital, LLC': ('FanDuel (CT)', 'Flutter Entertainment'),
    'CT Lottery Corp': ('CT Lottery', 'Fanatics Inc'),
    'CLC XL Center': ('CT Lottery (XL Center)', 'Fanatics Inc'),

    # ---- NH / RI / OR / MT monopoly ----
    'Sports Bet Montana': ('Sports Bet Montana', 'Intralot / Montana Lottery'),
    'Sports Bet Montana (sole operator, run by Intralot via Montana Lottery)': ('Sports Bet Montana', 'Intralot / Montana Lottery'),
    "Sportsbook Rhode Island": ('Sportsbook RI', "IGT / Bally's Corporation / RI Lottery"),
    "Sportsbook Rhode Island (operated by IGT in partnership with Bally's Corporation, managed by Rhode Island Lottery)": ('Sportsbook RI', "IGT / Bally's Corporation / RI Lottery"),
    'Twin River': ('Sportsbook RI (Twin River)', "IGT / Bally's Corporation / RI Lottery"),
    'Tiverton Casino': ('Sportsbook RI (Tiverton)', "IGT / Bally's Corporation / RI Lottery"),
    "Bally's Twin River": ('Sportsbook RI (Twin River)', "IGT / Bally's Corporation / RI Lottery"),
    'RI Online': ('Sportsbook RI (Online)', "IGT / Bally's Corporation / RI Lottery"),
    'Online (Mobile)': ('Sportsbook RI (Online)', "IGT / Bally's Corporation / RI Lottery"),
    'NH Lottery Sports': ('DraftKings', 'DraftKings Inc'),
    'DraftKings NH': ('DraftKings', 'DraftKings Inc'),
    'Oregon Lottery Scoreboard': ('DraftKings', 'DraftKings Inc'),
    'Scoreboard': ('DraftKings', 'DraftKings Inc'),
    'GambetDC': ('GambetDC (defunct)', 'Intralot / DC Lottery'),

    # ---- DC operator names (OLG financials) ----
    'American Wagering, Inc. dba Caesars': ('Caesars (Capital One Arena)', 'Caesars Entertainment'),
    'American Wagering, Inc. (dba Caesars)': ('Caesars (Capital One Arena)', 'Caesars Entertainment'),
    'American Wagering, Inc. dba William Hill': ('Caesars (Capital One Arena)', 'Caesars Entertainment'),
    'American Wagering, Inc. (dba William Hill)': ('Caesars (Capital One Arena)', 'Caesars Entertainment'),
    'BetMGM (Nationals Park)': ('BetMGM (Nationals Park)', 'Entain/MGM Resorts'),
    'FanDuel (Audi Field)': ('FanDuel (Audi Field)', 'Flutter Entertainment'),
    'BetFair Interactive US (dba FanDuel)': ('FanDuel (Audi Field)', 'Flutter Entertainment'),
    'BetFair Interactive US dba FanDuel': ('FanDuel (Audi Field)', 'Flutter Entertainment'),
    'Grand Central': ('Grand Central', 'Grand Central Restaurant & Bowling'),
    'Grand Central H Street': ('Grand Central H Street', 'Grand Central Restaurant & Bowling'),
    'Cloakbook': ('Cloakbook', 'Cloakbook'),
    'Sports & Social': ('Sports & Social', 'Live! Hospitality'),
    'Ugly Mug': ('Ugly Mug', 'Ugly Mug'),
    'DraftKings Inc.': ('DraftKings', 'DraftKings Inc'),
    'PENN Sports Interactive': ('ESPN Bet', 'PENN Entertainment'),
    'Penn Sports Interactive': ('ESPN Bet', 'PENN Entertainment'),

    # ---- Louisiana d/b/a names ----
    'DK Crown Holdings, Inc.': ('DraftKings', 'DraftKings Inc'),
    'FanDuel Online Sportsbook': ('FanDuel', 'Flutter Entertainment'),
    'BetMGM Online Sportsbook': ('BetMGM', 'Entain/MGM Resorts'),

    # ---- NV names ----
    'Atlantis Sports Nevada': ('Atlantis Sports', 'Monarch Casino'),
    'B-Connected Sports': ('B-Connected Sports', "Boyd Gaming"),
    'Circa Sportsbook': ('Circa Sports', 'Circa Resort & Casino'),
    'STN Sports': ('Station Sports', 'Station Casinos'),
    'Westgate SuperBook': ('SuperBook', 'SuperBook Sports'),
    'South Point Sports': ('South Point', 'South Point Hotel Casino'),
    'Wynn Sports': ('Wynn Sports', 'Wynn Resorts'),

    # ---- PA facility names (mixed case) ----
    'Hollywood Casino': ('Hollywood Casino', 'PENN Entertainment'),
    'Rivers - Pittsburgh': ('Rivers Pittsburgh', 'Rush Street Interactive'),
    'Rivers - Philadelphia': ('Rivers Philadelphia', 'Rush Street Interactive'),
    'Parx Casino (PA)': ('Parx Casino', 'Greenwood Racing'),
    "Harrah's": ("Harrah's Philadelphia", 'Caesars Entertainment'),
    'Valley Forge Casino Resort': ('Valley Forge', 'Boyd Gaming'),
    'Mount Airy Casino Resort': ('Mount Airy', 'Mount Airy Casino'),
    'Wind Creek Bethlehem': ('Wind Creek', 'Poarch Band of Creek Indians'),
    'Hollywood Casino Morgantown': ('Hollywood Morgantown', 'PENN Entertainment'),
    'Hollywood Casino York': ('Hollywood York', 'PENN Entertainment'),
    'Live! Casino Pittsburgh': ('Live! Pittsburgh', 'Cordish Companies'),
    'Live! Casino & Hotel Philadelphia': ('Live! Philadelphia', 'Cordish Companies'),
    'Mohegan Sun Pocono': ('Mohegan Sun Pocono', 'Mohegan Tribal Gaming'),
    'Presque Isle Downs': ('Presque Isle', 'Churchill Downs Inc'),
    'Hollywood Casino at The Meadows': ('Hollywood Meadows', 'PENN Entertainment'),
    'Parx Casino Shippensburg': ('Parx Shippensburg', 'Greenwood Racing'),

    # ---- PA facility names (ALL CAPS from PGCB Excel reports) ----
    'HOLLYWOOD CASINO': ('Hollywood Casino', 'PENN Entertainment'),
    'RIVERS - PITTSBURGH': ('Rivers Pittsburgh', 'Rush Street Interactive'),
    'RIVERS - PHILADELPHIA': ('Rivers Philadelphia', 'Rush Street Interactive'),
    'PARX CASINO': ('Parx Casino', 'Greenwood Racing'),
    "HARRAH'S": ("Harrah's Philadelphia", 'Caesars Entertainment'),
    'VALLEY FORGE CASINO': ('Valley Forge', 'Boyd Gaming'),
    'PRESQUE ISLE': ('Presque Isle', 'Churchill Downs Inc'),
    'MOUNT AIRY': ('Mount Airy', 'Mount Airy Casino'),
    'MOHEGAN': ('Mohegan Sun Pocono', 'Mohegan Tribal Gaming'),
    'MOHEGAN - LEHIGH VALLEY': ('Mohegan Lehigh Valley', 'Mohegan Tribal Gaming'),
    'HOLLYWOOD CASINO AT THE MEADOWS': ('Hollywood Meadows', 'PENN Entertainment'),
    'LIVE! CASINO PITTSBURGH': ('Live! Pittsburgh', 'Cordish Companies'),
    'LIVE! CASINO PHILADELPHIA': ('Live! Philadelphia', 'Cordish Companies'),
    'WIND CREEK': ('Wind Creek', 'Poarch Band of Creek Indians'),
    'HOLLYWOOD CASINO MORGANTOWN': ('Hollywood Morgantown', 'PENN Entertainment'),
    'HOLLYWOOD CASINO YORK': ('Hollywood York', 'PENN Entertainment'),
    'PARX SHIPPENSBURG': ('Parx Shippensburg', 'Greenwood Racing'),
    'PARX AT MALVERN': ('Parx Shippensburg', 'Greenwood Racing'),
    'SOUTH PHILADELPHIA RACE AND SPORTSBOOK': ('South Philadelphia', 'Parx Casino / Greenwood Racing'),
    'SUGARHOUSE CASINO': ('BetRivers', 'Rush Street Interactive'),
    'RIVERS': ('BetRivers', 'Rush Street Interactive'),
    'OAKS RACE AND SPORTSBOOK': ('Oaks Race & Sportsbook', 'Parx Casino / Greenwood Racing'),
    'MEADOWS': ('Hollywood Meadows', 'PENN Entertainment'),
    'Wind Creek': ('Wind Creek', 'Poarch Band of Creek Indians'),

    # ---- IN casino/sportsbook names ----
    'Ameristar Casino (East Chicago)': ('Ameristar East Chicago', 'PENN Entertainment'),
    'Ameristar Casino': ('Ameristar East Chicago', 'PENN Entertainment'),
    "Bally's Evansville": ("Bally's Evansville", "Bally's Corporation"),
    'Belterra Casino (Florence)': ('Belterra Casino', 'Boyd Gaming'),
    'Belterra Casino': ('Belterra Casino', 'Boyd Gaming'),
    'Blue Chip Casino (Michigan City)': ('Blue Chip Casino', 'Boyd Gaming'),
    'Blue Chip Casino': ('Blue Chip Casino', 'Boyd Gaming'),
    'Caesars Southern Indiana': ('Caesars Southern IN', 'Caesars Entertainment'),
    'French Lick Casino': ('French Lick', 'French Lick Resort'),
    'French Lick Resort': ('French Lick', 'French Lick Resort'),
    'Hard Rock Casino Northern Indiana': ('Hard Rock Northern IN', 'Seminole Tribe / Hard Rock Digital'),
    'Horseshoe Hammond': ('Horseshoe Hammond', 'Caesars Entertainment'),
    'Horseshoe Indianapolis': ('Horseshoe Indianapolis', 'Caesars Entertainment'),
    "Harrah's Hoosier Park": ("Harrah's Hoosier Park", 'Caesars Entertainment'),
    'Hollywood Lawrenceburg': ('Hollywood Lawrenceburg', 'PENN Entertainment'),
    'Rising Star Casino': ('Rising Star', 'Full House Resorts'),
    'Terre Haute Casino': ('Terre Haute', 'Churchill Downs Inc'),
    'Indiana Grand': ('Indiana Grand', 'Caesars Entertainment'),
    'Indiana Grand Racing & Casino': ('Indiana Grand', 'Caesars Entertainment'),
    'BetAmerica': ('BetAmerica', 'Churchill Downs Inc'),
    'Tropicana Evansville': ('Tropicana Evansville', 'Bally\'s Corporation'),

    # ---- MI casino/platform names ----
    'MGM GRAND DETROIT': ('BetMGM', 'Entain/MGM Resorts'),
    'MOTORCITY CASINO': ('FanDuel', 'Flutter Entertainment'),
    'GREEKTOWN CASINO': ('ESPN Bet', 'PENN Entertainment'),
    'NYX Digital': ('FireKeepers', 'Nottawaseppi Huron Band'),
    'Parx Interactive': ('BetRivers (Gun Lake)', 'Rush Street Interactive'),
    'Rush Street': ('BetRivers', 'Rush Street Interactive'),
    'Pala Interactive': ('Four Winds', 'Pokagon Band'),
    'GAN': ('Soaring Eagle', 'Saginaw Chippewa'),
    'FoxBet': ('FoxBet', 'Flutter Entertainment'),
    ' BetMGM': ('BetMGM', 'Entain/MGM Resorts'),
    ' DraftKings': ('DraftKings', 'DraftKings Inc'),
    ' Penn Sports Interactive / Barstool': ('ESPN Bet', 'PENN Entertainment'),
    'Penn Sports Interactive / Barstool': ('ESPN Bet', 'PENN Entertainment'),

    # ---- MO names ----
    'Ameristar Casino Hotel Kansas City': ('Ameristar KC', 'PENN Entertainment'),
    'Ameristar Casino Resort Spa St. Charles': ('Ameristar St Charles', 'PENN Entertainment'),
    'Hollywood Casino St. Louis': ('Hollywood St Louis', 'PENN Entertainment'),
    "ARGOSY CASINO HOTEL & SPA - RETAIL": ('Argosy Casino', 'PENN Entertainment'),
    "ARGOSY CASINO HOTEL & SPA- RETAIL": ('Argosy Casino', 'PENN Entertainment'),
    "BET365 SPORTSBOOK-BALLY'S KANSAS CITY - MOBILE": ("bet365", "bet365 Group"),
    "BETMGM SPORTSBOOK-AMERISTAR KC - MOBILE": ("BetMGM", "Entain/MGM Resorts"),
    "ARGOSY CASINO - RETAIL": ("Argosy Casino", "PENN Entertainment"),
    "CAESAR'S SPORTSBOOK-HARRAH'S - RETAIL": ("Caesars (Harrah's KC)", "Caesars Entertainment"),
    "CAESAR'S SPORTSBOOK- HORSESHOE - RETAIL": ("Caesars (Horseshoe)", "Caesars Entertainment"),
    "CAESARS SPORTSBOOK-HARRAH'S KANSAS CITY - RETAIL": ("Caesars (Harrah's KC)", "Caesars Entertainment"),
    "CAESARS SPORTSBOOK- HARRAH'S- RETAIL": ("Caesars (Harrah's KC)", "Caesars Entertainment"),
    "CAESARS SPORTSBOOK-HORSESHOE - RETAIL": ("Caesars (Horseshoe)", "Caesars Entertainment"),
    "CENTURY CASINO & HOTEL-CAPE GIRARDEAU - RETAIL": ("Century Casino", "Century Casinos"),
    "CENTURY CASINO CAPE GIRARDEAU - RETAIL": ("Century Casino", "Century Casinos"),
    "RIVER CITY CASINO - RETAIL": ("River City Casino", "PENN Entertainment"),
    "CIRCA SPORTS-AMERISTAR ST CHARLES - MOBILE": ("Circa Sports", "Circa Sports"),
    "DRAFTKINGS SPORTSBOOK-AMERISTAR ST. CHARLES - MOBILE": ("DraftKings", "DraftKings Inc"),
    "FANATICS SPORTSBOOK-ARGOSY CASINO - MOBILE": ("Fanatics", "Fanatics"),
    "FANATICS SPORTSBOOK-RIVER CITY - MOBILE": ("Fanatics", "Fanatics"),
    "FANATICS SPORTSBOOK-AMERISTAR KC - MOBILE": ("Fanatics", "Fanatics Inc"),
    "FANATICS-AMERISTAR KC - RETAIL": ("Fanatics (Ameristar KC)", "Fanatics Inc"),
    "FANATICS - AMERISTAR SC - RETAIL": ("Fanatics (Ameristar SC)", "Fanatics Inc"),
    "FANATICS  SPORTSBOOK- AMERISTAR KC -  RETAIL": ("Fanatics (Ameristar KC)", "Fanatics Inc"),
    "FANATICS  SPORTSBOOK- AMERISTAR SC -  RETAIL": ("Fanatics (Ameristar SC)", "Fanatics Inc"),
    "FANDUEL SPORTSBOOK-HOLLYWOOD ST. LOUIS - MOBILE": ("FanDuel", "Flutter Entertainment"),
    "HOLLYWOOD CASINO ST. LOUIS - RETAIL": ("Hollywood St Louis", "PENN Entertainment"),
    "PENN SPORTS INTERACTIVE-AMERISTAR ST. CHARLES - MOBILE": ("ESPN Bet", "PENN Entertainment"),
    "RIVER CITY CASINO & HOTEL - RETAIL": ("River City Casino", "PENN Entertainment"),

    # ---- WV venue names ----
    # WV reports by casino venue, not by sportsbook operator. Each venue hosts
    # multiple sportsbook skins (e.g., DraftKings, FanDuel, BetMGM) under one
    # master license. Do NOT map venues to individual sportsbook brands.
    'Mountaineer': ('Mountaineer', 'Vici Properties'),
    'Wheeling': ('Wheeling Island', 'Delaware North'),
    'Mardi Gras': ('Mardi Gras', 'Delaware North'),
    'Charles Town': ('Hollywood Charles Town', 'PENN Entertainment'),
    'Greenbrier': ('The Greenbrier', 'The Greenbrier'),

    # ---- IL licensee names (casino entities → sportsbook brands) ----
    'Casino Queen, Inc.': ('DraftKings (Casino Queen)', 'DraftKings Inc'),
    'Fairmount Park, Inc.': ('FanDuel (Fairmount)', 'Flutter Entertainment'),
    'Hawthorne Race Course, Inc.': ('BetMGM (Hawthorne)', 'Entain/MGM Resorts'),
    'Midwest Gaming & Entertainment, LLC': ('BetRivers (Rivers Des Plaines)', 'Rush Street Interactive'),
    'Elgin Riverboat Resort': ('DraftKings (Elgin)', 'DraftKings Inc'),
    'HC Joliet, LLC': ('DraftKings (Joliet)', 'DraftKings Inc'),
    'HC Aurora, LLC': ('DraftKings (Aurora)', 'DraftKings Inc'),
    'Par-A-Dice Gaming Corporation': ('Par-A-Dice', 'Boyd Gaming'),
    'Alton Casino, LLC': ('Alton Casino', 'PENN Entertainment'),
    'Southern Illinois Riverboat/Casino Cruises LLC': ('Harrahs Metropolis', 'Caesars Entertainment'),
    'The Rock Island Boatworks, LLC': ('Jumer\'s Casino', 'Delaware North'),
    'Northside Crown Gaming LLC': ('DraftKings (Wintrust)', 'DraftKings Inc'),
    'FHR-Illinois LLC': ('Fanatics (FHR)', 'Fanatics'),
    '815 Entertainment': ('Hard Rock Rockford', 'Seminole Tribe / Hard Rock Digital'),
    "WALKER'S BLUFF CASINO RESORT, LLC": ("Walker's Bluff", 'Saline County Casino'),

    # ---- MD operator names ----
    'Draft Kings': ('DraftKings', 'DraftKings Inc'),
    'MGM National Harbor': ('BetMGM (MGM National Harbor)', 'Entain/MGM Resorts'),
    'Horseshoe Casino': ('Caesars (Horseshoe)', 'Caesars Entertainment'),
    'Live! Casino': ('FanDuel (Live!)', 'Flutter Entertainment'),
    'Live! Casino (M)': ('FanDuel (Live! Mobile)', 'Flutter Entertainment'),
    'Hollywood Casino': ('Hollywood Casino', 'PENN Entertainment'),
    'Hollywood Casino (M)': ('Hollywood Casino Mobile', 'PENN Entertainment'),
    'Ocean Downs Casino': ('Ocean Downs', 'Churchill Downs Inc'),
    'Bingo World': ('Bingo World', 'Bingo World'),
    'Bingo World (M)': ('Bingo World Mobile', 'Bingo World'),
    'Crab Sports': ('Crab Sports', 'Crab Sports'),
    'Canton Gaming / Canton': ('Canton Gaming', 'Canton Gaming'),
    'Canton Gaming/Pikesville': ('Canton Gaming Pikesville', 'Canton Gaming'),
    'Canton Gaming / Towson': ('Canton Gaming Towson', 'Canton Gaming'),
    'Greenmount OTB': ('Greenmount', 'Greenmount OTB'),
    'Greenmount (M)': ('Greenmount Mobile', 'Greenmount OTB'),
    'Maryland Stadium Sub': ('Maryland Stadium', 'Maryland Stadium Sub'),
    'Maryland Stadium Sub (M)': ('Maryland Stadium Mobile', 'Maryland Stadium Sub'),
    "Long Shot's / Betfred": ('Betfred', 'Betfred'),
    "Long Shot's / Caesars": ('Caesars (Long Shots)', 'Caesars Entertainment'),
    "Long Shot's (M)": ('Long Shots Mobile', 'Long Shots'),
    'Riverboat on the Potomac': ('Riverboat Potomac', 'Riverboat'),
    'Riverboat on the Potomac / Bet365 (M)': ('bet365', 'bet365 Group'),
    'Riverboat on the Potomac / Pointsbet (M)': ('PointsBet', 'PointsBet Holdings'),
    'Whitman Gaming': ('Whitman Gaming', 'Whitman Gaming'),
    'Veterans Services': ('Veterans Services', 'Veterans Services'),

    # ---- DE operator names ----
    'Delaware Park': ('Delaware Park', 'Delaware North'),
    "Bally's Dover": ("Bally's Dover", "Bally's Corporation"),
    'Harrington Raceway': ('Harrington Raceway', 'Independent'),
    'DE Retailers': ('DE Retailers', 'DE Lottery'),

    # ---- MA operator names ----
    'Encore Boston Harbor': ('Encore Boston Harbor', 'Wynn Resorts'),
    'MGM Springfield': ('MGM Springfield', 'MGM Resorts'),
    'Plainridge Park Casino': ('Plainridge Park', 'PENN Entertainment'),
    "Bally's": ("Bally Bet", "Bally's Corporation"),
    'theScore Bet': ('theScore Bet', 'PENN Entertainment'),

    # ---- NJ casino/operator names ----
    "BALLY'S ATLANTIC CITY": ("Bally's AC", "Bally's Corporation"),
    "BORGATA HOTEL CASINO & SPA": ("BetMGM (Borgata)", "Entain/MGM Resorts"),
    "CAESARS ATLANTIC CITY": ("Caesars AC", "Caesars Entertainment"),
    "GOLDEN NUGGET ATLANTIC CITY": ("Golden Nugget", "Fertitta Entertainment"),
    "HARD ROCK HOTEL & CASINO ATLANTIC CITY": ("Hard Rock AC", "Hard Rock Digital"),
    "HARD ROCK HOTEL & CASINO": ("Hard Rock AC", "Hard Rock Digital"),
    "MEADOWLANDS RACETRACK": ("FanDuel (Meadowlands)", "Flutter Entertainment"),
    "MONMOUTH PARK": ("Monmouth Park", "Independent"),
    "OCEAN CASINO RESORT": ("Ocean Casino", "Independent"),
    "RESORTS ATLANTIC CITY": ("Resorts AC (DraftKings)", "DraftKings Inc"),
    "RESORTS DIGITAL GAMING": ("Resorts AC (DraftKings)", "DraftKings Inc"),
    "TROPICANA ATLANTIC CITY": ("Tropicana AC", "Bally's Corporation"),

    # ---- OH operator names ----
    'BELTERRA PARK - FANDUEL': ('FanDuel', 'Flutter Entertainment'),
    'CLEVELAND BROWNS - BALLY\'S INTERACTIVE': ('Bally Bet', 'Bally\'s Corporation'),
    'CLEVELAND GUARDIANS - BET365': ('bet365', 'bet365 Group'),
    'CLEVELAND GUARDIANS - FANATICS': ('Fanatics', 'Fanatics'),
    'COLUMBUS BLUE JACKETS - FANATICS': ('Fanatics', 'Fanatics'),
    'FC CINCINNATI - SUPERBOOK': ('SuperBook', 'SuperBook Sports'),
    'GENEVA SPORTS, LLC - PRIME SPORTS': ('Prime Sports', 'Independent'),
    'HARD ROCK CINCINNATI - SEMINOLE HARD ROCK DIGITAL': ('Hard Rock Bet', 'Hard Rock Digital'),
    'HOF VILLAGE - BETR': ('Betr', 'Betr'),
    'HOLLYWOOD COLUMBUS - PENN INTERACTIVE (ESPN BET)': ('ESPN BET', 'PENN Entertainment'),
    'HOLLYWOOD GAMING AT DAYTON RACEWAY - RSI OH, LLC': ('BetRivers', 'Rush Street Interactive'),
    'HOLLYWOOD TOLEDO - DRAFTKINGS': ('DraftKings', 'DraftKings Inc'),
    'JACK CLEVELAND (BETJACK)': ('BetJACK', 'JACK Entertainment'),
    'MGM NORTHFIELD PARK - BETMGM': ('BetMGM', 'Entain/MGM Resorts'),
    'MIAMI VALLEY GAMING AND RACING - MVGBET': ('MVGBet', 'Independent'),
    'SCIOTO DOWNS - CAESARS SPORTSBOOK': ('Caesars Sportsbook', 'Caesars Entertainment'),
    # OH retail names (same operators, different context)
    'BELTERRA PARK - FANDUEL': ('FanDuel', 'Flutter Entertainment'),
    'CINCINNATI REDS - BETMGM': ('BetMGM', 'Entain/MGM Resorts'),
    'CLEVELAND CAVALIERS - BETMGM': ('BetMGM', 'Entain/MGM Resorts'),
    'HARD ROCK CINCINNATI': ('Hard Rock Bet', 'Hard Rock Digital'),
    'HOLLYWOOD COLUMBUS - ESPN BET': ('ESPN BET', 'PENN Entertainment'),
    'HOLLYWOOD GAMING AT DAYTON RACEWAY - RSI': ('BetRivers', 'Rush Street Interactive'),
    'HOLLYWOOD TOLEDO - DRAFTKINGS': ('DraftKings', 'DraftKings Inc'),
    'JACK CLEVELAND - BETJACK': ('BetJACK', 'JACK Entertainment'),
    'MGM NORTHFIELD PARK - BETMGM': ('BetMGM', 'Entain/MGM Resorts'),
    'MIAMI VALLEY GAMING AND RACING': ('MVGBet', 'Independent'),
    'SCIOTO DOWNS - CAESARS': ('Caesars Sportsbook', 'Caesars Entertainment'),

    # ---- VA operator names (licensed mobile + retail casino sportsbooks) ----
    # Mobile operators (11+ licensed)
    'FanDuel (VA)': ('FanDuel', 'Flutter Entertainment'),
    'DraftKings (VA)': ('DraftKings', 'DraftKings Inc'),
    'BetMGM (VA)': ('BetMGM', 'Entain/MGM Resorts'),
    'Caesars Sportsbook (VA)': ('Caesars', 'Caesars Entertainment'),
    'BetRivers (VA)': ('BetRivers', 'Rush Street Interactive'),
    'Fanatics (VA)': ('Fanatics', 'Fanatics Inc'),
    'ESPN Bet (VA)': ('ESPN Bet', 'PENN Entertainment'),
    'bet365 (VA)': ('bet365', 'bet365 Group'),
    'Hard Rock Bet (VA)': ('Hard Rock Bet', 'Seminole Tribe / Hard Rock Digital'),
    'Bally Bet (VA)': ('Bally Bet', "Bally's Corporation"),
    'theScore Bet (VA)': ('ESPN Bet', 'PENN Entertainment'),
    'Sporttrade (VA)': ('Sporttrade', 'Sporttrade Inc'),
    # Retail casino sportsbooks (3 casinos with sportsbooks as of 2024)
    'Hard Rock Hotel Casino Bristol': ('Hard Rock Bristol', 'Seminole Tribe / Hard Rock Digital'),
    'Rivers Casino Portsmouth': ('Rivers Portsmouth', 'Rush Street Interactive'),
    'Live! Casino Virginia': ('Live! Casino VA', 'Cordish Companies'),
    'Caesars Virginia Casino Resort': ('Caesars Virginia', 'Caesars Entertainment'),
    'The Interim Gaming Hall': ('Interim Gaming Hall', 'Boyd Gaming'),

    # ---- KY operator names (online service providers) ----
    'DraftKings (KY)': ('DraftKings', 'DraftKings Inc'),
    'FanDuel (KY)': ('FanDuel', 'Flutter Entertainment'),
    'bet365 (KY)': ('bet365', 'bet365 Group'),
    'Bet365 (KY)': ('bet365', 'bet365 Group'),
    'BetMGM (KY)': ('BetMGM', 'Entain/MGM Resorts'),
    'Fanatics Sportsbook (KY)': ('Fanatics', 'Fanatics Inc'),
    'Fanatics (KY)': ('Fanatics', 'Fanatics Inc'),
    'Caesars Sportsbook (KY)': ('Caesars', 'Caesars Entertainment'),
    'ESPN BET (KY)': ('ESPN Bet', 'PENN Entertainment'),
    'theScore Bet (KY)': ('ESPN Bet', 'PENN Entertainment'),
    'Circa Sports (KY)': ('Circa Sports', 'Circa Resort & Casino'),
    'Prime Sports': ('Prime Sports', 'Prime Sports / Geneva Sports'),
    'Prime Sports (KY)': ('Prime Sports', 'Prime Sports / Geneva Sports'),
    # KY retail sportsbook locations (track-based)
    'Churchill Downs': ('Churchill Downs', 'Churchill Downs Inc'),
    'Churchill Downs Sportsbook': ('Churchill Downs', 'Churchill Downs Inc'),
    'Keeneland': ('Keeneland', 'Keeneland Association'),
    'Keeneland Sportsbook': ('Keeneland', 'Keeneland Association'),
    'Turfway Park': ('Turfway Park', 'Churchill Downs Inc'),
    'Turfway Park Sportsbook': ('Turfway Park', 'Churchill Downs Inc'),
    'Ellis Park': ('Ellis Park', 'Ellis Park Racing & Gaming'),
    'Ellis Park Sportsbook': ('Ellis Park', 'Ellis Park Racing & Gaming'),
    'Kentucky Downs': ('Kentucky Downs', 'Kentucky Downs'),
    'Kentucky Downs Sportsbook': ('Kentucky Downs', 'Kentucky Downs'),
    'Red Mile': ('Red Mile', 'Keeneland Association'),
    'Red Mile Sportsbook': ('Red Mile', 'Keeneland Association'),
    'Oak Grove': ('Oak Grove Racing & Gaming', 'Churchill Downs Inc'),
    'Oak Grove Racing & Gaming': ('Oak Grove Racing & Gaming', 'Churchill Downs Inc'),
    'Cumberland Run': ('Cumberland Run', 'Keeneland Association'),
    'Cumberland Run Sportsbook': ('Cumberland Run', 'Keeneland Association'),
    "Sandy's": ("Sandy's Racing & Gaming", 'Keeneland Association'),
    "Sandy's Racing & Gaming": ("Sandy's Racing & Gaming", 'Keeneland Association'),

    # ---- WY operator names (online-only, 5 operators) ----
    'DraftKings (WY)': ('DraftKings', 'DraftKings Inc'),
    'FanDuel (WY)': ('FanDuel', 'Flutter Entertainment'),
    'BetMGM (WY)': ('BetMGM', 'Entain/MGM Resorts'),
    'Caesars (WY)': ('Caesars', 'Caesars Entertainment'),
    'Caesars Sportsbook (WY)': ('Caesars', 'Caesars Entertainment'),
    'Fanatics (WY)': ('Fanatics', 'Fanatics Inc'),
    'Fanatics Sportsbook (WY)': ('Fanatics', 'Fanatics Inc'),
    # WY raw names as they may appear in commission reports
    'DK Crown Holdings': ('DraftKings', 'DraftKings Inc'),
    'Betfair Interactive': ('FanDuel', 'Flutter Entertainment'),
    'Roar Digital (WY)': ('BetMGM', 'Entain/MGM Resorts'),
    'American Wagering': ('Caesars', 'Caesars Entertainment'),
    'American Wagering, Inc.': ('Caesars', 'Caesars Entertainment'),
    'Fanatics Betting and Gaming': ('Fanatics', 'Fanatics Inc'),
    'PointsBet (WY)': ('Fanatics', 'Fanatics Inc'),
    'WynnBET (WY)': ('ESPN Bet', 'PENN Entertainment'),
    # WY PDF uppercase raw operator names (exact as extracted from PDF headers/rows)
    'BETMGM': ('BetMGM', 'Entain/MGM Resorts'),
    'CAESARS': ('Caesars', 'Caesars Entertainment'),
    'DRAFTKINGS': ('DraftKings', 'DraftKings Inc'),
    'FANATICS': ('Fanatics', 'Fanatics Inc'),
    'FANDUEL': ('FanDuel', 'Flutter Entertainment'),
    'WYNNBET': ('ESPN Bet', 'PENN Entertainment'),

    # ---- KS operator names (4 casino zones, online + retail) ----
    # Online operators
    'DraftKings (KS)': ('DraftKings', 'DraftKings Inc'),
    'DraftKings Sportsbook (KS)': ('DraftKings', 'DraftKings Inc'),
    'FanDuel (KS)': ('FanDuel', 'Flutter Entertainment'),
    'FanDuel Sportsbook (KS)': ('FanDuel', 'Flutter Entertainment'),
    'BetMGM (KS)': ('BetMGM', 'Entain/MGM Resorts'),
    'BetMGM Sportsbook (KS)': ('BetMGM', 'Entain/MGM Resorts'),
    'Caesars (KS)': ('Caesars', 'Caesars Entertainment'),
    'Caesars Sportsbook (KS)': ('Caesars', 'Caesars Entertainment'),
    'Caesars Kansas Sportsbook': ('Caesars', 'Caesars Entertainment'),
    'ESPN BET (KS)': ('ESPN Bet', 'PENN Entertainment'),
    'ESPN Bet (KS)': ('ESPN Bet', 'PENN Entertainment'),
    'theScore Bet (KS)': ('ESPN Bet', 'PENN Entertainment'),
    'Barstool Sportsbook (KS)': ('ESPN Bet', 'PENN Entertainment'),
    'Fanatics (KS)': ('Fanatics', 'Fanatics Inc'),
    'Fanatics Sportsbook (KS)': ('Fanatics', 'Fanatics Inc'),
    'bet365 (KS)': ('bet365', 'bet365 Group'),
    'Bet365 (KS)': ('bet365', 'bet365 Group'),
    'PointsBet (KS)': ('Fanatics', 'Fanatics Inc'),
    # Casino zone / retail sportsbook names
    'Boot Hill Casino': ('Boot Hill Casino', 'Boot Hill Casino'),
    'Boot Hill Casino & Resort': ('Boot Hill Casino', 'Boot Hill Casino'),
    'Boot Hill Casino Resort': ('Boot Hill Casino', 'Boot Hill Casino'),
    'Kansas Star Casino': ('Kansas Star Casino', 'Kansas Star Casino'),
    'Kansas Star': ('Kansas Star Casino', 'Kansas Star Casino'),
    'Hollywood Casino at Kansas Speedway': ('Hollywood Casino KS', 'PENN Entertainment'),
    'Hollywood Casino (KS)': ('Hollywood Casino KS', 'PENN Entertainment'),
    'Hollywood Casino Kansas': ('Hollywood Casino KS', 'PENN Entertainment'),
    'Kansas Crossing Casino': ('Kansas Crossing Casino', 'Kansas Crossing Casino'),
    'Kansas Crossing Casino and Hotel': ('Kansas Crossing Casino', 'Kansas Crossing Casino'),
    'Kansas Crossing': ('Kansas Crossing Casino', 'Kansas Crossing Casino'),
    # Golden Nugget (Boot Hill partner)
    'Golden Nugget (KS)': ('DraftKings (Golden Nugget)', 'DraftKings Inc'),

    # ---- NV aggregate (no operator breakdown, statewide data) ----
    # NV doesn't report by operator, just aggregate sports pool data
    'Statewide Sports Pool': ('ALL', None),
    'Sports Pool': ('ALL', None),
    'Nevada Sports Pool': ('ALL', None),

    # ---- CO city names ----
    'Black Hawk': ('Black Hawk', 'CO Gaming'),
    'Central City': ('Central City', 'CO Gaming'),
    'Cripple Creek': ('Cripple Creek', 'CO Gaming'),

    # ---- MS region names ----
    'Central': ('Central Region', 'MS Gaming Commission'),
    'Coastal': ('Coastal Region', 'MS Gaming Commission'),
    'Northern': ('Northern Region', 'MS Gaming Commission'),

    # ---- AZ operator names (tribal + commercial franchise licensees) ----
    # Commercial (sports franchise) licensees
    'FanDuel (Phoenix Suns / NBA)': ('FanDuel', 'Flutter Entertainment'),
    'DraftKings (TPC Scottsdale / PGA)': ('DraftKings', 'DraftKings Inc'),
    'BetMGM (Arizona Cardinals / NFL)': ('BetMGM', 'Entain/MGM Resorts'),
    'Caesars Sportsbook (Arizona Diamondbacks / MLB)': ('Caesars', 'Caesars Entertainment'),
    'BetRivers (Arizona Rattlers / AFL)': ('BetRivers', 'Rush Street Interactive'),
    'ESPN BET (Phoenix Speedway / NASCAR)': ('ESPN Bet', 'PENN Entertainment'),
    'Bally Bet (Phoenix Mercury / WNBA)': ('Bally Bet', "Bally's Corporation"),
    # Tribal licensees
    'Fanatics Sportsbook (Tonto Apache Tribe)': ('Fanatics', 'Fanatics Inc'),
    "Desert Diamond Sports (Tohono O'odham Tribe)": ('Desert Diamond', "Tohono O'odham Nation"),
    'SuperBook (Fort Mojave Indian Tribe)': ('SuperBook', 'SuperBook Sports'),
    'Unibet (Quechan Tribe)': ('Unibet', 'Kindred Group'),
    'Betfred (Ak-Chin Indian Community)': ('Betfred', 'Betfred Group'),
    'Sporttrade (Quechan Indian Tribe)': ('Sporttrade', 'Sporttrade Inc'),
    'Golden Nugget (Hualapai Tribe)': ('DraftKings (Golden Nugget)', 'DraftKings Inc'),
    'Hard Rock Bet (Navajo Nation)': ('Hard Rock Bet', 'Seminole Tribe / Hard Rock Digital'),
    'bet365 (San Carlos Apache Tribe)': ('bet365', 'bet365 Group'),
    'Circa Sports (AZ)': ('Circa Sports', 'Circa Resort & Casino'),
    # Common shortened forms in AZ reports
    'FanDuel Sportsbook (AZ)': ('FanDuel', 'Flutter Entertainment'),
    'DraftKings Sportsbook (AZ)': ('DraftKings', 'DraftKings Inc'),
    'Caesars Sportsbook (AZ)': ('Caesars', 'Caesars Entertainment'),
    'BetMGM (AZ)': ('BetMGM', 'Entain/MGM Resorts'),
    'ESPN BET (AZ)': ('ESPN Bet', 'PENN Entertainment'),
    'WynnBET (San Carlos Apache)': ('ESPN Bet', 'PENN Entertainment'),
    'Wynn (San Carlos Apache Tribe)': ('WynnBET', 'PENN Entertainment'),
    # AZ PDF raw names (as they appear in the PDF reports)
    'Caesars (American Wagering)': ('Caesars', 'Caesars Entertainment'),
    'Draft Kings/Crown Gaming': ('DraftKings', 'DraftKings Inc'),
    'Fan Duel': ('FanDuel', 'Flutter Entertainment'),
    'Bally Interactive, LLC': ('Bally Bet', "Bally's Corporation"),
    'Bally Interactive': ('Bally Bet', "Bally's Corporation"),
    'Churchill Downs/Twin Spires': ('TwinSpires', 'Churchill Downs Inc'),
    'Churchill Downs/TwinSpires': ('TwinSpires', 'Churchill Downs Inc'),
    'Desert Diamond Mobile': ('Desert Diamond', "Tohono O'odham Nation"),
    'Digital Gaming USA (Betway)': ('Betway', 'Digital Gaming USA'),
    'Digital Gaming USA': ('Betway', 'Digital Gaming USA'),
    'Fanatics Sports Book': ('Fanatics', 'Fanatics Inc'),
    'PENN / ESPN Bet': ('ESPN Bet', 'PENN Entertainment'),
    'PENN/ESPN Bet': ('ESPN Bet', 'PENN Entertainment'),
    'Penn Sports (Barstool Sports)': ('ESPN Bet', 'PENN Entertainment'),
    'Plannatech': ('Plannatech', 'Plannatech'),
    'RSI (Rush Street Interactive)': ('BetRivers', 'Rush Street Interactive'),
    'Sahara Bets': ('Sahara Bets', 'Sahara Bets'),
    'SBOpco, LLC (SuperBook)': ('SuperBook', 'SuperBook Sports'),
    'SBOpco (SuperBook)': ('SuperBook', 'SuperBook Sports'),
    'Seminole Hard Rock Digital': ('Hard Rock Bet', 'Seminole Tribe / Hard Rock Digital'),
    'Unibet AZ (Kindred)': ('Unibet', 'Kindred Group'),
    'WSI US (WynnBet)': ('WynnBET', 'PENN Entertainment'),
    'Fubo.TV': ('Fubo Sportsbook (defunct)', 'fuboTV'),

    # ---- VT operator names ----
    'FanDuel (VT)': ('FanDuel', 'Flutter Entertainment'),
    'DraftKings (VT)': ('DraftKings', 'DraftKings Inc'),
    'Fanatics (VT)': ('Fanatics', 'Fanatics Inc'),
    'Fanatics Sportsbook (VT)': ('Fanatics', 'Fanatics Inc'),
    'FanDuel Sportsbook (VT)': ('FanDuel', 'Flutter Entertainment'),
    'DraftKings Sportsbook (VT)': ('DraftKings', 'DraftKings Inc'),

    # ---- ME operator names (PDF raw names from GCU MGCU-8600 forms) ----
    'DraftKings (Passamaquoddy Tribe)': ('DraftKings', 'DraftKings Inc'),
    'DraftKings (ME)': ('DraftKings', 'DraftKings Inc'),
    'Passamaquoddy': ('DraftKings', 'DraftKings Inc'),
    'Passamaquoddy Tribe': ('DraftKings', 'DraftKings Inc'),
    'Caesars Sportsbook (Penobscot Nation)': ('Caesars', 'Caesars Entertainment'),
    'Caesars (Penobscot Nation)': ('Caesars', 'Caesars Entertainment'),
    'Caesars (Houlton Band of Maliseet Indians)': ('Caesars', 'Caesars Entertainment'),
    "Caesars (Mi'kmaq Nation)": ('Caesars', 'Caesars Entertainment'),
    'Caesars (ME)': ('Caesars', 'Caesars Entertainment'),
    'William Hill (Penobscot Nation)': ('Caesars', 'Caesars Entertainment'),
    'William Hill (ME)': ('Caesars', 'Caesars Entertainment'),
    'Penobscot Maliseet Micmac': ('Caesars', 'Caesars Entertainment'),
    'Penobscot Nation': ('Caesars', 'Caesars Entertainment'),
    'Oxford Casino Sportsbook': ('Oxford Sportsbook', 'Churchill Downs Inc'),
    'Oxford Casino': ('Oxford Sportsbook', 'Churchill Downs Inc'),
    'Oxford Sportsbook': ('Oxford Sportsbook', 'Churchill Downs Inc'),
    'First Tracks Sportsbook': ('Oddfellahs', 'First Tracks Investments'),
    'First Tracks': ('Oddfellahs', 'First Tracks Investments'),
    'First Tracks Investments': ('Oddfellahs', 'First Tracks Investments'),
    'Oddfellahs': ('Oddfellahs', 'First Tracks Investments'),

    # ---- NE (Nebraska) facility names ----
    'WarHorse Lincoln': ('WarHorse Lincoln', 'WarHorse Gaming'),
    'WarHorse Omaha': ('WarHorse Omaha', 'WarHorse Gaming'),
    'Warhorse Lincoln': ('WarHorse Lincoln', 'WarHorse Gaming'),
    'Warhorse Omaha': ('WarHorse Omaha', 'WarHorse Gaming'),
    'WARHORSE LINCOLN': ('WarHorse Lincoln', 'WarHorse Gaming'),
    'WARHORSE OMAHA': ('WarHorse Omaha', 'WarHorse Gaming'),
    'Grand Island Casino Resort': ('Grand Island Casino Resort', 'Elite Casino Resorts'),
    'Grand Island Casino': ('Grand Island Casino Resort', 'Elite Casino Resorts'),
    'Grand Island Casino & Resort': ('Grand Island Casino Resort', 'Elite Casino Resorts'),
    'GRAND ISLAND CASINO RESORT': ('Grand Island Casino Resort', 'Elite Casino Resorts'),
    "Harrah's Columbus": ("Harrah's Columbus", 'Caesars Entertainment'),
    "Harrahs Columbus": ("Harrah's Columbus", 'Caesars Entertainment'),
    "Harrahs Columbus NE Racing & Casino": ("Harrah's Columbus", 'Caesars Entertainment'),
    "HARRAH'S COLUMBUS": ("Harrah's Columbus", 'Caesars Entertainment'),
    'HARRAHS COLUMBUS': ("Harrah's Columbus", 'Caesars Entertainment'),
    'Lake Mac Casino & Resort': ('Lake Mac Casino & Resort', 'Lake Mac Casino'),
    'Lake Mac Casino': ('Lake Mac Casino & Resort', 'Lake Mac Casino'),
    'LAKE MAC CASINO & RESORT': ('Lake Mac Casino & Resort', 'Lake Mac Casino'),
    'LAKE MAC CASINO': ('Lake Mac Casino & Resort', 'Lake Mac Casino'),
    'Fonner Park': ('Fonner Park', 'Fonner Park'),
    'FONNER PARK': ('Fonner Park', 'Fonner Park'),

    # ---- AR (Arkansas) casino names ----
    'Oaklawn Racing Casino Resort': ('Oaklawn', 'Oaklawn Racing Casino Resort'),
    'Oaklawn': ('Oaklawn', 'Oaklawn Racing Casino Resort'),
    'OAKLAWN RACING CASINO RESORT': ('Oaklawn', 'Oaklawn Racing Casino Resort'),
    'OAKLAWN': ('Oaklawn', 'Oaklawn Racing Casino Resort'),
    'Southland Casino Racing': ('Southland Casino', 'Delaware North'),
    'Southland Casino': ('Southland Casino', 'Delaware North'),
    'Southland': ('Southland Casino', 'Delaware North'),
    'SOUTHLAND CASINO RACING': ('Southland Casino', 'Delaware North'),
    'SOUTHLAND': ('Southland Casino', 'Delaware North'),
    'Saracen Casino Resort': ('Saracen Casino', 'Quapaw Nation'),
    'Saracen Casino': ('Saracen Casino', 'Quapaw Nation'),
    'Saracen': ('Saracen Casino', 'Quapaw Nation'),
    'SARACEN CASINO RESORT': ('Saracen Casino', 'Quapaw Nation'),
    'SARACEN': ('Saracen Casino', 'Quapaw Nation'),

    # ---- MT (Montana) single operator ----
    'Sports Bet Montana': ('Sports Bet Montana', 'Intralot / Montana Lottery'),
    'SportsBet Montana': ('Sports Bet Montana', 'Intralot / Montana Lottery'),
    'SPORTS BET MONTANA': ('Sports Bet Montana', 'Intralot / Montana Lottery'),
    'Montana Sports Bet': ('Sports Bet Montana', 'Intralot / Montana Lottery'),
    'Montana Lottery Sports': ('Sports Bet Montana', 'Intralot / Montana Lottery'),

    # ---- SD (South Dakota) aggregate names ----
    'Deadwood Casinos': ('Deadwood Casinos', 'Deadwood Gaming Association'),
    'DEADWOOD CASINOS': ('Deadwood Casinos', 'Deadwood Gaming Association'),
    'Deadwood': ('Deadwood Casinos', 'Deadwood Gaming Association'),
    'Deadwood Sports': ('Deadwood Casinos', 'Deadwood Gaming Association'),
    'SD Sports Wagering': ('Deadwood Casinos', 'Deadwood Gaming Association'),

    # ---- Additional KS (Kansas) entries ----
    'Boot Hill': ('Boot Hill Casino', 'Boot Hill Casino'),
    'Hollywood': ('Hollywood Casino KS', 'PENN Entertainment'),
    'Hollywood ESPNBet': ('ESPN Bet', 'PENN Entertainment'),
    'KS Crossing': ('Kansas Crossing Casino', 'Chickasaw Nation Industries'),
    "KS Crossing Caesar's": ('Caesars', 'Caesars Entertainment'),
    'Kansas Crossing Casino': ('Kansas Crossing Casino', 'Chickasaw Nation Industries'),
    'White Collar Crime Fund': ('TOTAL', None),
    'Problem Gambling and Addictions Grant Fund': ('TOTAL', None),
    'Attracting Professional Sports to Kansas Fund': ('TOTAL', None),

    # ---- Additional OH (Ohio) entries ----
    'BELTERRA PARK - BETWAY': ('Betway', 'Super Group'),
    'COLUMBUS CREW SC - TIPICO': ('Tipico', 'Tipico Group'),
    'HOLLYWOOD COLUMBUS': ('Hollywood Columbus', 'PENN Entertainment'),
    'HOLLYWOOD GAMING AT DAYTON RACEWAY': ('Hollywood Dayton', 'PENN Entertainment'),
    'HOLLYWOOD GAMING AT MAHONING VALLEY': ('Hollywood Mahoning Valley', 'PENN Entertainment'),
    'HOLLYWOOD TOLEDO': ('Hollywood Toledo', 'PENN Entertainment'),
    'JACK THISTLEDOWN - BETJACK': ('betJACK', 'JACK Entertainment'),

    # ---- Additional MI (Michigan) entries ----
    'TwinSpires': ('TwinSpires', 'Churchill Downs Inc'),

    # ---- Additional IL (Illinois) entries ----
    'FHR-Illinois  LLC': ('FanDuel', 'Flutter Entertainment'),

    # ---- Additional IN (Indiana) entries ----
    'Barstool': ('ESPN Bet', 'PENN Entertainment'),
    'Smarkets': ('Smarkets', 'Smarkets Group'),
    'MaximBet': ('MaximBet', 'Carousel Group'),

    # ---- Additional NJ (New Jersey) entries ----
    'OCEAN RESORTS': ('Ocean Casino', 'Luxor Capital Group'),
    'OCEAN RESORTS CASINO': ('Ocean Casino', 'Luxor Capital Group'),
    'OCEAN RESORT CASINO': ('Ocean Casino', 'Luxor Capital Group'),
    'OCEAN CASINO RESORT AC': ('Ocean Casino', 'Luxor Capital Group'),
    'RESORTS CASINO HOTEL (DGMB CASINO LLC)': ('Resorts Casino', 'Mohegan Gaming'),
    'RESORTS CASINO HOTEL (DGMB CASINO, LLC)': ('Resorts Casino', 'Mohegan Gaming'),
    'RESORTS CASINO HOTEL (DGMB CASINO, LLC.)': ('Resorts Casino', 'Mohegan Gaming'),
    'RESORTS CASNIO HOTEL (DGMB CASINO LLC)': ('Resorts Casino', 'Mohegan Gaming'),
    'RESORTS DIGITAL GAMING, LLC': ('Resorts AC (DraftKings)', 'DraftKings Inc'),
    'FR PARK RACING, LP (FREEHOLD RACEWAY)': ('Freehold Raceway', 'Freehold Raceway'),
    'TROPICANA CASINO & RESORT': ('Tropicana AC', "Bally's Corporation"),
    'TROPICANA CASINO AND RESORT': ('Tropicana AC', "Bally's Corporation"),
    'DARBY/WILLIAM HILL (MONMOUTH PARK)': ('Monmouth Park', 'Caesars Entertainment'),
    'DARBY / MONMOUTH PARK': ('Monmouth Park', 'Caesars Entertainment'),
    'DARBY DEVELOPMENT (MONMOUTH PARK)': ('Monmouth Park', 'Caesars Entertainment'),
    'DARBY DEVELOPMENT(MONMOUTH PARK)': ('Monmouth Park', 'Caesars Entertainment'),
    'BOARDWALK REGENCY': ('Caesars AC (Boardwalk Regency)', 'Caesars Entertainment'),
    'RESORTS CASINO - DGMB': ('Resorts Casino', 'Mohegan Gaming'),

    # ---- Additional NE (Nebraska) entries ----
    'WarHorse Gaming Lincoln': ('WarHorse Lincoln', 'WarHorse Gaming'),
    'WarHorse Gaming Lincoln, LLC': ('WarHorse Lincoln', 'WarHorse Gaming'),
    'WarHorse Gaming Lincoln, LLC (Approved)': ('WarHorse Lincoln', 'WarHorse Gaming'),
    'WarHorse Casino Omaha': ('WarHorse Omaha', 'WarHorse Gaming'),
    'WarHorse Casino Omaha, LLC': ('WarHorse Omaha', 'WarHorse Gaming'),
    'Grand Island Casino & Resort (Approved)': ('Grand Island Casino Resort', 'Elite Casino Resorts'),

    # ---- Additional MA (Massachusetts) entries ----
    "Bally\u2019s": ('Bally Bet', "Bally's Corporation"),

    # ---- Additional MD (Maryland) entries ----
    "Long Shot's": ("Long Shot's", 'Long Shot Sports Bar'),
    'Combined': ('TOTAL', None),

    # ---- IA (Iowa) online operator LLC names ----
    'American Wagering Inc.': ('Caesars', 'Caesars Entertainment'),
    "Bally's Management Group, LLC": ('Bally Bet', "Bally's Corporation"),
    'Betfair Interactive US LLC': ('FanDuel', 'Flutter Entertainment'),
    'Betfred Sports (Iowa) LLC': ('Betfred', 'Betfred Group'),
    'BetMGM, LLC': ('BetMGM', 'Entain/MGM Resorts'),
    'BlueBet Iowa LLC': ('BlueBet (defunct)', 'BlueBet Holdings'),
    'Circa Sports Iowa LLC': ('Circa Sports', 'Circa Resort & Casino'),
    'Crown IA Gaming, LLC': ('DraftKings', 'DraftKings Inc'),
    'Dubuque Racing Association, Ltd.': ('Dubuque Racing', 'Dubuque Racing'),
    'FBG Iowa LLC': ('Fanatics', 'Fanatics Inc'),
    'Hillside (Iowa) LLC': ('bet365', 'bet365 Group'),
    'Penn Sports Interactive, LLC.': ('ESPN Bet', 'PENN Entertainment'),
    'Rush Street Interactive IA, LLC': ('BetRivers', 'Rush Street Interactive'),
    'SCE Partners, LLC': ('SCE Partners (defunct)', 'SCE Partners'),
    'Sporttrade Iowa LLC': ('Sporttrade', 'Sporttrade Inc'),
    'Digital Gaming Corporation USA': ('BetMGM', 'Entain/MGM Resorts'),
    'Elite Hospitality Group, LLC': ('Elite Hospitality Group', 'Elite Casino Resorts'),
    'SBOpco, LLC': ('SuperBook', 'SuperBook Sports'),
    'Score Digital Sports Ventures Inc.': ('ESPN Bet', 'PENN Entertainment'),
    'Sports Information Group, LLC': ('SuperBook', 'SuperBook Sports'),
    'Tipico Iowa, LLC': ('Tipico', 'Tipico Group'),
    'Entity_1': ('UNKNOWN', None),
    'Entity_6': ('UNKNOWN', None),
    'Operator_1': ('UNKNOWN', None),
    'Operator_2': ('UNKNOWN', None),
    'Operator_3': ('UNKNOWN', None),
    'Operator_4': ('UNKNOWN', None),
    'Operator_5': ('UNKNOWN', None),
    'Operator_7': ('UNKNOWN', None),
    # IA casino venue names (retail sportsbooks)
    'Ameristar II': ('Ameristar Council Bluffs', 'PENN Entertainment'),
    'Casino Queen - Marquette': ('Casino Queen Marquette', 'DraftKings Inc'),
    'Catfish Bend Casino': ('Catfish Bend Casino', 'Independent'),
    'Diamond Jo - Dubuque': ('Diamond Jo Dubuque', 'Boyd Gaming'),
    'Diamond Jo - Worth': ('Diamond Jo Worth', 'Boyd Gaming'),
    'Grand Falls Casino Resort': ('Grand Falls Casino', 'Elite Casino Resorts'),
    'Hard Rock Casino': ('Hard Rock Sioux City', 'Seminole Tribe / Hard Rock Digital'),
    "Harrah's Council Bluffs Casino & Hotel": ("Harrah's Council Bluffs", 'Caesars Entertainment'),
    'Horseshoe Casino Council Bluffs': ('Horseshoe Council Bluffs', 'Caesars Entertainment'),
    'Isle Casino Hotel Waterloo': ('Isle Waterloo', 'Caesars Entertainment'),
    'Isle of Capri - Bettendorf': ('Isle of Capri Bettendorf', 'Caesars Entertainment'),
    'Lakeside Casino': ('Lakeside Casino', 'Independent'),
    'Prairie Meadows Racetrack & Casino': ('Prairie Meadows', 'Independent'),
    'Q Casino': ('Q Casino', 'Independent'),
    'Rhythm City Casino': ('Rhythm City Casino', 'Elite Casino Resorts'),
    'Riverside Casino and Golf Resort': ('Riverside Casino', 'Independent'),
    'Wild Rose - Clinton': ('Wild Rose Clinton', 'Wild Rose Entertainment'),
    'Wild Rose -Clinton': ('Wild Rose Clinton', 'Wild Rose Entertainment'),
    'Wild Rose - Emmetsburg': ('Wild Rose Emmetsburg', 'Wild Rose Entertainment'),
    'Wild Rose - Jefferson': ('Wild Rose Jefferson', 'Wild Rose Entertainment'),
    'Totals': ('TOTAL', None),

    # Aggregate / Total indicators
    'Total': ('TOTAL', None),
    'TOTAL': ('TOTAL', None),
    'Statewide Total': ('TOTAL', None),
    'Grand Total': ('TOTAL', None),
    'Combined Total': ('TOTAL', None),
    'ALL': ('TOTAL', None),
    'State Total': ('TOTAL', None),
}

# Build case-insensitive lookup
_OPERATOR_MAP_LOWER = {k.lower(): v for k, v in OPERATOR_MAP.items()}

SPORT_MAP = {
    # Football
    'Pro Football': 'football', 'Pro Football US': 'football',
    'NFL': 'football', 'Football': 'football',
    'NCAA Football': 'football_college', 'NCAA FB': 'football_college',
    'College Football': 'football_college',
    'NCAA Womens BB': 'basketball_college', 'NCAA Hockey': 'hockey',
    # Basketball
    'Pro Basketball': 'basketball', 'NBA': 'basketball', 'Basketball': 'basketball',
    'NCAA Basketball': 'basketball_college', 'NCAA Mens BB': 'basketball_college',
    'College Basketball': 'basketball_college', 'WNBA': 'basketball',
    # Baseball
    'Baseball': 'baseball', 'Pro Baseball': 'baseball',
    'MLB': 'baseball', 'NCAA Baseball': 'baseball',
    # Hockey
    'Ice Hockey': 'hockey', 'Hockey': 'hockey', 'NHL': 'hockey',
    # Soccer
    'Soccer': 'soccer', 'SOCCER': 'soccer', 'MLS': 'soccer',
    # Tennis
    'Tennis': 'tennis', 'TENNIS': 'tennis',
    # Golf
    'Golf': 'golf', 'PGA': 'golf',
    # Combat sports
    'MMA': 'mma', 'MMA/UFC': 'mma', 'UFC': 'mma',
    'Boxing/MMA': 'combat_sports', 'BOXING': 'boxing', 'Boxing': 'boxing',
    # Motorsports
    'Motor Racing': 'motorsports', 'Motor Sports': 'motorsports',
    'NASCAR': 'motorsports', 'FORMULA 1': 'motorsports', 'INDYCAR': 'motorsports',
    'Auto Racing': 'motorsports',
    # Parlay (composite)
    'Parlay': 'parlay', 'Sports Parlay Cards': 'parlay', 'Parlays': 'parlay',
    'Parlay / Combinations': 'parlay', 'Parlays/Combinations': 'parlay',
    'Sports Pari-Mutuel': 'parlay',
    # Table Tennis
    'Table Tennis': 'table_tennis',
    # Other
    'CFL': 'other', 'OLYMPICS': 'other',
    'Specials': 'other', 'Other': 'other', 'OTHER': 'other',
    'Esports': 'esports', 'eSports': 'esports',
    'Cricket': 'cricket', 'Rugby': 'rugby',
    'Lacrosse': 'lacrosse',
}

_SPORT_MAP_LOWER = {k.lower(): v for k, v in SPORT_MAP.items()}


PARENT_TO_BRAND = {
    'Flutter Entertainment': 'FanDuel',
    'DraftKings Inc': 'DraftKings',
    'Entain/MGM Resorts': 'BetMGM',
    'Caesars Entertainment': 'Caesars',
    'PENN Entertainment': 'ESPN Bet',
    'Fanatics Inc': 'Fanatics',
    'Rush Street Interactive': 'BetRivers',
    "Bally's Corporation": 'Bally Bet',
    'Hard Rock Digital': 'Hard Rock Bet',
    'bet365 Group': 'bet365',
    'Digital Gaming USA': 'Betway',
    'Super Group': 'Betway',
    'Churchill Downs Inc': 'TwinSpires',
    'Tipico Group': 'Tipico',
    'Smarkets Group': 'Smarkets',
    'Carousel Group': 'MaximBet',
    'JACK Entertainment': 'betJACK',
    # State monopoly / lottery operators
    "IGT / Bally's Corporation / RI Lottery": 'Sportsbook RI',
    'Intralot / Montana Lottery': 'Sports Bet Montana',
    'Deadwood Gaming Association': 'Deadwood Casinos',
    # Venue-based (report by venue, not by sportsbook skin)
    # For these, the sportsbook brand = the reported name since we can't
    # attribute to a single brand
    'Vici Properties': None,
    'Delaware North': None,
    'The Greenbrier': None,
    'WarHorse Gaming': None,
    'Elite Casino Resorts': None,
    'Lake Mac Casino': None,
    'Fonner Park': None,
    'Boot Hill Casino': None,
    'Chickasaw Nation Industries': None,
    'First Tracks Investments': None,
    'Oaklawn Racing Casino Resort': None,
    'Quapaw Nation': None,
    # IA venue-based (casino retail sportsbooks — no single sportsbook brand attribution)
    'Wild Rose Entertainment': None,
    'Dubuque Racing': None,
    'SCE Partners': None,
    'BlueBet Holdings': None,
    'Long Shot Sports Bar': None,
    'Luxor Capital Group': None,
    'Mohegan Gaming': None,
    'Freehold Raceway': None,
    'Kansas Star Casino': None,
    'Mohegan Tribal Gaming Authority': None,
    'Mashantucket Pequot Tribal Nation': None,
}


# Venue-specific brand overrides — takes priority over PARENT_TO_BRAND.
# For venues where the sportsbook operating the skin differs from the parent's
# default brand (e.g., PENN Entertainment venues partner with different sportsbooks).
REPORTED_TO_BRAND = {
    # PA venues (from PGCB sportsbook brand list)
    'Valley Forge': 'FanDuel',
    'Hollywood Meadows': 'DraftKings',
    'Hollywood York': 'ESPN Bet',
    'Hollywood Morgantown': 'Fanatics',
    'Hollywood Casino': 'Caesars',            # Penn National/Grantville location
    'Presque Isle': 'BetMGM',
    "Harrah's Philadelphia": 'Caesars',
    'Rivers Pittsburgh': 'BetRivers',
    'Rivers Philadelphia': 'BetRivers',
    'Parx Casino': 'betPARX',
    'Oaks Race & Sportsbook': 'betPARX',
    'South Philadelphia': 'betPARX',
    'Parx Shippensburg': 'betPARX',
    'Mount Airy': 'Mount Airy',              # Retail only (formerly FOX Bet)
    'Mohegan Sun Pocono': 'Unibet',
    'Mohegan Lehigh Valley': 'Mohegan Lehigh Valley',  # Retail only
    'Wind Creek': 'Betfred',                 # Closed Jul 2025
    'Live! Pittsburgh': 'Live! Pittsburgh',   # Retail only
    'Live! Philadelphia': 'Live! Philadelphia',  # Retail only
    # IA casino venues — keep venue names (retail sportsbooks, operator varies)
    'Ameristar Council Bluffs': 'Ameristar Council Bluffs',
    'Casino Queen Marquette': 'Casino Queen Marquette',
    'Catfish Bend Casino': 'Catfish Bend Casino',
    'Diamond Jo Dubuque': 'Diamond Jo Dubuque',
    'Diamond Jo Worth': 'Diamond Jo Worth',
    'Grand Falls Casino': 'Grand Falls Casino',
    'Hard Rock Sioux City': 'Hard Rock Sioux City',
    "Harrah's Council Bluffs": "Harrah's Council Bluffs",
    'Horseshoe Council Bluffs': 'Horseshoe Council Bluffs',
    'Isle Waterloo': 'Isle Waterloo',
    'Isle of Capri Bettendorf': 'Isle of Capri Bettendorf',
    'Lakeside Casino': 'Lakeside Casino',
    'Prairie Meadows': 'Prairie Meadows',
    'Q Casino': 'Q Casino',
    'Rhythm City Casino': 'Rhythm City Casino',
    'Riverside Casino': 'Riverside Casino',
    'Wild Rose Clinton': 'Wild Rose Clinton',
    'Wild Rose Emmetsburg': 'Wild Rose Emmetsburg',
    'Wild Rose Jefferson': 'Wild Rose Jefferson',
    'Dubuque Racing': 'Dubuque Racing',
    'SCE Partners (defunct)': 'SCE Partners (defunct)',
    'BlueBet (defunct)': 'BlueBet (defunct)',
}


def get_sportsbook_brand(reported_name: str, parent_company: str = None) -> str:
    """Get the standardized sportsbook brand for cross-state aggregation.

    Priority: REPORTED_TO_BRAND (venue-specific) > PARENT_TO_BRAND (parent-level) > name matching.
    For TOTAL/ALL/UNKNOWN, returns the same value.
    """
    if not reported_name or reported_name in ('TOTAL', 'ALL', 'UNKNOWN'):
        return reported_name

    # 1. Venue-specific override (highest priority)
    brand = REPORTED_TO_BRAND.get(reported_name)
    if brand is not None:
        return brand

    # 2. Parent company mapping
    if parent_company:
        brand = PARENT_TO_BRAND.get(parent_company)
        if brand is not None:
            return brand
        # If parent is in the dict with None value, use reported_name
        if parent_company in PARENT_TO_BRAND:
            return reported_name

    # 3. Fallback: extract brand from reported_name itself
    for brand_name in ['FanDuel', 'DraftKings', 'BetMGM', 'Caesars', 'ESPN Bet',
                       'Fanatics', 'BetRivers', 'Hard Rock Bet', 'Bally Bet',
                       'bet365', 'Betway', 'TwinSpires', 'Unibet', 'betPARX']:
        if brand_name.lower() in reported_name.lower():
            return brand_name

    return reported_name


def normalize_operator(raw_name: str, state_code: str = None) -> str:
    """Normalize a raw operator name to a standard name."""
    if not raw_name or str(raw_name).strip() in ('', 'nan', 'None', 'NaN'):
        return 'UNKNOWN'
    raw_clean = str(raw_name).strip()

    # Exact match
    if raw_clean in OPERATOR_MAP:
        return OPERATOR_MAP[raw_clean][0]

    # Case-insensitive exact match
    if raw_clean.lower() in _OPERATOR_MAP_LOWER:
        return _OPERATOR_MAP_LOWER[raw_clean.lower()][0]

    # Partial match — known operator name contained in raw string
    for key, (std, _) in OPERATOR_MAP.items():
        if len(key) > 4 and key.lower() in raw_clean.lower():
            return std

    # Return as-is (will be flagged in validation)
    return raw_clean


def get_parent_company(standard_name: str) -> str:
    """Get the parent company for a standardized operator name."""
    if not standard_name:
        return None
    for _, (std, parent) in OPERATOR_MAP.items():
        if std == standard_name:
            return parent
    return None


def normalize_sport(raw_sport) -> str:
    """Normalize a raw sport category to a standard name."""
    if not raw_sport or str(raw_sport).strip() in ('', 'nan', 'None', 'NaN'):
        return None
    clean = str(raw_sport).strip()

    # Exact match
    if clean in SPORT_MAP:
        return SPORT_MAP[clean]

    # Case-insensitive match
    if clean.lower() in _SPORT_MAP_LOWER:
        return _SPORT_MAP_LOWER[clean.lower()]

    return 'other'
