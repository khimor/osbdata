// Operator colors — SACRED, matching design system
export const OPERATOR_COLORS = {
  'FanDuel':       '#1a6ce8',
  'DraftKings':    '#3ea84e',
  'BetMGM':        '#c4a040',
  'Caesars':       '#7c5cf0',
  'ESPN Bet':      '#e04040',
  'theScore Bet':  '#e04040',
  'Fanatics':      '#f07020',
  'BetRivers':     '#20b0d0',
  'bet365':        '#2e7028',
  'Hard Rock Bet': '#d050e0',
  'Bally Bet':     '#f472b6',
  'WynnBET':       '#a3e635',
  'Unibet':        '#0ea5e9',
  'Other':         '#555570',
};

// State colors for stacked charts
export const STATE_COLORS = {
  'NY': '#6488f0',
  'NJ': '#40d0b0',
  'PA': '#e0a040',
  'IL': '#f06060',
  'OH': '#7c5cf0',
  'MI': '#20b0d0',
  'AZ': '#d050e0',
  'MD': '#f07020',
  'VA': '#2dd4a0',
  'NC': '#a855f7',
  'IN': '#84cc16',
  'CO': '#fb923c',
  'CT': '#5090e0',
  'MA': '#e0a040',
  'Other': '#555570',
};

export const STATE_NAMES = {
  AL:'Alabama',AK:'Alaska',AZ:'Arizona',AR:'Arkansas',CA:'California',
  CO:'Colorado',CT:'Connecticut',DE:'Delaware',DC:'Washington DC',FL:'Florida',
  GA:'Georgia',HI:'Hawaii',ID:'Idaho',IL:'Illinois',IN:'Indiana',
  IA:'Iowa',KS:'Kansas',KY:'Kentucky',LA:'Louisiana',ME:'Maine',
  MD:'Maryland',MA:'Massachusetts',MI:'Michigan',MN:'Minnesota',MS:'Mississippi',
  MO:'Missouri',MT:'Montana',NE:'Nebraska',NV:'Nevada',NH:'New Hampshire',
  NJ:'New Jersey',NM:'New Mexico',NY:'New York',NC:'North Carolina',ND:'North Dakota',
  OH:'Ohio',OK:'Oklahoma',OR:'Oregon',PA:'Pennsylvania',RI:'Rhode Island',
  SC:'South Carolina',SD:'South Dakota',TN:'Tennessee',TX:'Texas',UT:'Utah',
  VT:'Vermont',VA:'Virginia',WA:'Washington',WV:'West Virginia',WI:'Wisconsin',WY:'Wyoming',
};

// Sport colors
export const SPORT_COLORS = {
  'Football':   '#2dd4a0',
  'Basketball': '#e0a040',
  'Baseball':   '#f06060',
  'Parlay':     '#7c5cf0',
  'Other':      '#555570',
  'Soccer':     '#20b0d0',
  'Hockey':     '#5090e0',
  'Tennis':     '#d050e0',
  'Golf':       '#84cc16',
  'MMA':        '#f07020',
};

export function getOperatorColor(name) {
  return OPERATOR_COLORS[name] || OPERATOR_COLORS['Other'];
}

export function getSportColor(name) {
  return SPORT_COLORS[name] || SPORT_COLORS['Other'];
}

export function getStateColor(code) {
  return STATE_COLORS[code] || STATE_COLORS['Other'];
}
