"""
service/industry_classifier.py
==============================

Maps Indian-listed companies into a FIXED 24-label industry taxonomy used
by the news pipeline (news_scratch/remap_industries.py). Designed for the
backend's company_master table — see database.py columns:

    industry              TEXT
    industry_source       TEXT  -- 'indian_api' | 'keyword' | 'llm' | 'manual'
    industry_updated_at   TIMESTAMPTZ

Classification cascade (cheapest → most expensive):

  1. _api_industry_keyword_match()
        Indian API returns its own free-form industry string in
        /stock?name=... → companyProfile.industry. The vocabulary is small
        and stable (~250 strings). We have a curated map below from those
        strings to canonical labels.
  2. _keyword_match() on Indian API industry, then on company name.
        Catches strings the curated map missed (uses substrings: 'bank',
        'pharma', 'cement', etc.).
  3. classify_with_llm() — batched gpt-4.1-nano JSON call. Optional;
        only used by the backfill admin endpoint when --use-llm is set.

The classifier returns a (label, source) tuple; label is None if nothing
matched and LLM is disabled. The label string is ALWAYS one of CANON_NAMES
verbatim, or None.
"""
from __future__ import annotations

import json
import logging
import os
import time
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger("uvicorn.error")

# ---------------------------------------------------------------------------
# Canonical 24-label taxonomy (must stay in sync with
# news_scratch/remap_industries.py::TAXONOMY)
# ---------------------------------------------------------------------------
TAXONOMY: List[Tuple[str, str]] = [
    ("Information Technology & Software",
     "software, IT services, cloud, SaaS, enterprise tech, digital platforms, AI, cybersecurity, semiconductors where tech-led"),
    ("Telecommunications & Connectivity",
     "telecom operators, broadband, wireless, towers, network infrastructure, satellite communications"),
    ("Media, Entertainment & Gaming",
     "TV, streaming, content, music, publishing, esports, online gaming, digital entertainment"),
    ("Financial Services",
     "banks, NBFCs, lending, payments, fintech, investment firms, exchanges, brokers, wealth management, asset management"),
    ("Insurance",
     "life insurance, general insurance, health insurance, reinsurance"),
    ("Real Estate & Property Services",
     "developers, commercial and residential real estate, RE services, property management"),
    ("Infrastructure & Construction",
     "EPC, civil construction, roads, rail infra, ports infra, urban infra, construction services"),
    ("Industrial Manufacturing & Engineering",
     "capital goods, machinery, industrial equipment, engineering services, automation, electrical equipment, factory systems"),
    ("Automobiles & Mobility",
     "passenger vehicles, commercial vehicles, two-wheelers, auto components, EVs, mobility platforms, transport equipment"),
    ("Aerospace & Defence",
     "defence manufacturing, weapons systems, aerospace, aviation systems, military electronics, strategic technology"),
    ("Transportation & Logistics",
     "shipping, ports, freight, logistics, warehousing, rail transport, cargo handling, supply-chain services"),
    ("Aviation & Travel",
     "airlines, airports, aviation services, travel services, tourism, hospitality-linked travel businesses"),
    ("Energy",
     "oil & gas, refining, fuel retail, coal, power generation, utilities, renewable energy, biofuels, energy storage, hydrogen"),
    ("Chemicals & Materials",
     "specialty chemicals, industrial chemicals, petrochemicals, industrial gases, fertilizers, paints, coatings, advanced materials"),
    ("Metals & Mining",
     "steel, aluminium, copper, ferrous/non-ferrous metals, mining, mineral extraction, metal processing"),
    ("Building Materials",
     "cement, ceramics, glass, cables where construction-led, pipes, insulation, tiles, other construction materials"),
    ("Agriculture & Agribusiness",
     "farming, seeds, crop inputs, agri services, plantations, sugar, ethanol, seafood, marine products, agri exports"),
    ("Food, Beverage & Consumer Staples",
     "dairy, packaged food, beverages, alcohol, household staples, food processing, daily-use products"),
    ("Consumer Durables & Discretionary",
     "electronics, white goods, appliances, batteries for consumer use, home products, long-life discretionary goods"),
    ("Retail & E-Commerce",
     "consumer retail, online commerce, marketplaces, omnichannel commerce, specialty retail"),
    ("Healthcare & Life Sciences",
     "pharmaceuticals, biotech, hospitals, diagnostics, medical devices, healthcare services"),
    ("Textiles, Apparel & Luxury Goods",
     "textiles, garments, apparel, footwear, leather, fashion manufacturing, related exports"),
    ("Environmental & Waste Services",
     "recycling, water treatment, waste management, pollution control, environmental solutions"),
    ("Diversified Conglomerates / Holding Companies",
     "companies spanning multiple unrelated sectors where no single industry is dominant"),
]
CANON_NAMES = [t[0] for t in TAXONOMY]
CANON_SET = set(CANON_NAMES)
TAXONOMY_TEXT = "\n".join(f"  - {name} — {desc}" for name, desc in TAXONOMY)

# ---------------------------------------------------------------------------
# Layer 1: hand-curated map from Indian API's `industry` strings to canonical
# labels. Sample of API vocabulary observed across our news_scratch runs.
# Keys are lowercased; checked for exact match first, then for substring.
# ---------------------------------------------------------------------------
_INDIAN_API_INDUSTRY_MAP: Dict[str, str] = {
    # IT & Software
    "software & programming": "Information Technology & Software",
    "computer services": "Information Technology & Software",
    "computer hardware": "Information Technology & Software",
    "computer storage devices": "Information Technology & Software",
    "computer peripherals": "Information Technology & Software",
    "computer networks": "Information Technology & Software",
    "scientific & technical instruments": "Information Technology & Software",
    "semiconductors": "Information Technology & Software",
    "office equipment": "Information Technology & Software",
    # Indian listings classified under "Business Services" are overwhelmingly
    # BPM / IT-enabled services (Sagility, eClerx, Affle, Quess, etc.).
    "business services": "Information Technology & Software",
    # Telecom
    "communications services": "Telecommunications & Connectivity",
    "communications equipment": "Telecommunications & Connectivity",
    "wireless communications": "Telecommunications & Connectivity",
    # Media / entertainment / gaming
    "broadcasting & cable tv": "Media, Entertainment & Gaming",
    "motion pictures": "Media, Entertainment & Gaming",
    "printing & publishing": "Media, Entertainment & Gaming",
    "advertising": "Media, Entertainment & Gaming",
    "casinos & gaming": "Media, Entertainment & Gaming",
    "recreational activities": "Media, Entertainment & Gaming",
    "toys": "Consumer Durables & Discretionary",
    "toys & games": "Consumer Durables & Discretionary",
    # Education — not its own bucket in our 24-label taxonomy. Most Indian
    # listed "Schools" rows are IT-training / edtech (NIIT, Aptech).
    "schools": "Information Technology & Software",
    "education services": "Information Technology & Software",
    # Financial Services
    "regional banks": "Financial Services",
    "money center banks": "Financial Services",
    "investment services": "Financial Services",
    "consumer financial services": "Financial Services",
    "consumer lending": "Financial Services",
    "mortgage investment": "Financial Services",
    "miscellaneous financial services": "Financial Services",
    "s&ls/savings banks": "Financial Services",
    # Insurance
    "insurance (life)": "Insurance",
    "insurance (prop. & casualty)": "Insurance",
    "insurance (accident & health)": "Insurance",
    "insurance (miscellaneous)": "Insurance",
    # Real Estate
    "real estate operations": "Real Estate & Property Services",
    "reit": "Real Estate & Property Services",
    "reit – residential": "Real Estate & Property Services",
    "reit – diversified": "Real Estate & Property Services",
    "reit – industrial": "Real Estate & Property Services",
    "reit - residential": "Real Estate & Property Services",
    "reit - diversified": "Real Estate & Property Services",
    "reit - industrial": "Real Estate & Property Services",
    # Infrastructure / Construction
    "construction services": "Infrastructure & Construction",
    "construction & agricultural machinery": "Infrastructure & Construction",
    "construction – raw materials": "Infrastructure & Construction",
    "constr. – supplies & fixtures": "Infrastructure & Construction",
    # Industrial / Engineering
    "misc. capital goods": "Industrial Manufacturing & Engineering",
    "miscellaneous capital goods": "Industrial Manufacturing & Engineering",
    "industrial machinery": "Industrial Manufacturing & Engineering",
    "electrical utilities equipment": "Industrial Manufacturing & Engineering",
    "industrial conglomerate": "Industrial Manufacturing & Engineering",
    "machinery (industrial)": "Industrial Manufacturing & Engineering",
    "machinery & equipment": "Industrial Manufacturing & Engineering",
    # Indian API "Electronic Instr. & Controls" covers electrical equipment
    # makers (ABB, Siemens, Polycab cables, KEI cables, Apar Industries) —
    # almost entirely industrial electrical equipment.
    "electronic instr. & controls": "Industrial Manufacturing & Engineering",
    "electronic instruments & controls": "Industrial Manufacturing & Engineering",
    "electronic instruments": "Industrial Manufacturing & Engineering",
    # Paper / packaging — capital-intensive industrial goods.
    "paper & paper products": "Industrial Manufacturing & Engineering",
    "containers & packaging": "Chemicals & Materials",
    # Auto
    "auto & truck manufacturers": "Automobiles & Mobility",
    "auto & truck parts": "Automobiles & Mobility",
    "tires": "Automobiles & Mobility",
    # Indian listings tagged "Recreational Products" are TVS Motor / Bajaj
    # two-wheelers / leisure vehicles — map to Automobiles.
    "recreational products": "Automobiles & Mobility",
    # Aerospace / Defence
    "aerospace & defense": "Aerospace & Defence",
    # Transport / Logistics
    "trucking": "Transportation & Logistics",
    "railroads": "Transportation & Logistics",
    "water transport": "Transportation & Logistics",
    "marine transportation": "Transportation & Logistics",
    "misc. transportation": "Transportation & Logistics",
    # Aviation / Travel
    "airline": "Aviation & Travel",
    "airlines": "Aviation & Travel",
    "hotels & motels": "Aviation & Travel",
    "casinos & gambling": "Aviation & Travel",
    "travel services": "Aviation & Travel",
    # Energy
    "oil & gas operations": "Energy",
    "oil well services & equipment": "Energy",
    "coal": "Energy",
    "electric utilities": "Energy",
    "natural gas utilities": "Energy",
    "water utilities": "Environmental & Waste Services",
    # Chemicals & Materials
    "chemicals – plastics & rubber": "Chemicals & Materials",
    "chemical manufacturing": "Chemicals & Materials",
    "chemicals": "Chemicals & Materials",
    "fertilizers": "Chemicals & Materials",
    "specialty chemicals": "Chemicals & Materials",
    # Metals & Mining
    "iron & steel": "Metals & Mining",
    "metal mining": "Metals & Mining",
    "non-metallic mining": "Metals & Mining",
    "misc. fabricated products": "Metals & Mining",
    "aluminum": "Metals & Mining",
    "gold & silver": "Metals & Mining",
    # Building Materials
    "construction materials": "Building Materials",
    # Agriculture
    "crops": "Agriculture & Agribusiness",
    "agriculture operations": "Agriculture & Agribusiness",
    "fishing & farming": "Agriculture & Agribusiness",
    "sugar": "Agriculture & Agribusiness",
    # Food / Beverage / Consumer Staples
    "beverages (alcoholic)": "Food, Beverage & Consumer Staples",
    "beverages (non-alcoholic)": "Food, Beverage & Consumer Staples",
    "food processing": "Food, Beverage & Consumer Staples",
    "fish/livestock": "Food, Beverage & Consumer Staples",
    "tobacco": "Food, Beverage & Consumer Staples",
    "personal & household products": "Food, Beverage & Consumer Staples",
    "personal & household prods.": "Food, Beverage & Consumer Staples",
    # Consumer Durables / Discretionary
    "appliances & tools": "Consumer Durables & Discretionary",
    "audio & video equipment": "Consumer Durables & Discretionary",
    "jewelry & silverware": "Consumer Durables & Discretionary",
    "photography": "Consumer Durables & Discretionary",
    "furniture & fixtures": "Consumer Durables & Discretionary",
    "office supplies": "Consumer Durables & Discretionary",
    # Retail / E-Commerce
    "retail (apparel)": "Retail & E-Commerce",
    "retail (department & discount)": "Retail & E-Commerce",
    "retail (drugs)": "Retail & E-Commerce",
    "retail (grocery)": "Retail & E-Commerce",
    "retail (specialty)": "Retail & E-Commerce",
    "retail (catalog & mail order)": "Retail & E-Commerce",
    "retail (technology)": "Retail & E-Commerce",
    "retail (home improvement)": "Retail & E-Commerce",
    "retail (restaurants)": "Retail & E-Commerce",
    "restaurants": "Retail & E-Commerce",
    # Healthcare
    "biotechnology & drugs": "Healthcare & Life Sciences",
    "major drugs": "Healthcare & Life Sciences",
    "healthcare facilities": "Healthcare & Life Sciences",
    "medical equipment & supplies": "Healthcare & Life Sciences",
    "biotech": "Healthcare & Life Sciences",
    # Textiles / Apparel
    "apparel/accessories": "Textiles, Apparel & Luxury Goods",
    "footwear": "Textiles, Apparel & Luxury Goods",
    "textiles – non apparel": "Textiles, Apparel & Luxury Goods",
    # Environmental
    "waste management services": "Environmental & Waste Services",
    "pollution & treatment controls": "Environmental & Waste Services",
    # Conglomerates
    "conglomerates": "Diversified Conglomerates / Holding Companies",
}

# ---------------------------------------------------------------------------
# Layer 2: keyword cascade. Substring match on lowercased company name OR
# Indian API industry. Sorted longest-first so more specific terms win.
# ---------------------------------------------------------------------------
_KEYWORDS: List[Tuple[str, str]] = [
    # IT & Software
    ("information technology", "Information Technology & Software"),
    ("software", "Information Technology & Software"),
    ("infotech", "Information Technology & Software"),
    ("infosys", "Information Technology & Software"),
    ("it services", "Information Technology & Software"),
    ("technologies", "Information Technology & Software"),
    ("technology", "Information Technology & Software"),
    ("cloud", "Information Technology & Software"),
    ("saas", "Information Technology & Software"),
    ("semiconductor", "Information Technology & Software"),
    ("cybersecurity", "Information Technology & Software"),
    ("artificial intelligence", "Information Technology & Software"),
    ("data systems", "Information Technology & Software"),
    # Telecom
    ("telecom", "Telecommunications & Connectivity"),
    ("broadband", "Telecommunications & Connectivity"),
    ("wireless", "Telecommunications & Connectivity"),
    ("communication", "Telecommunications & Connectivity"),
    ("tower", "Telecommunications & Connectivity"),
    # Media / entertainment / gaming
    ("entertainment", "Media, Entertainment & Gaming"),
    ("gaming", "Media, Entertainment & Gaming"),
    ("esports", "Media, Entertainment & Gaming"),
    ("e-sports", "Media, Entertainment & Gaming"),
    ("streaming", "Media, Entertainment & Gaming"),
    ("publishing", "Media, Entertainment & Gaming"),
    ("broadcasting", "Media, Entertainment & Gaming"),
    ("media", "Media, Entertainment & Gaming"),
    # Financial Services
    ("financial services", "Financial Services"),
    ("financial", "Financial Services"),
    ("finserv", "Financial Services"),
    ("finance", "Financial Services"),
    ("housing finance", "Financial Services"),
    ("micro finance", "Financial Services"),
    ("microfinance", "Financial Services"),
    ("banking", "Financial Services"),
    ("nbfc", "Financial Services"),
    ("fintech", "Financial Services"),
    ("payments", "Financial Services"),
    ("asset management", "Financial Services"),
    ("wealth management", "Financial Services"),
    ("broker", "Financial Services"),
    ("capital", "Financial Services"),
    ("securities", "Financial Services"),
    ("bank ", "Financial Services"),
    ("bank.", "Financial Services"),
    (" bank", "Financial Services"),
    # Insurance
    ("insurance", "Insurance"),
    ("reinsurance", "Insurance"),
    ("assurance", "Insurance"),
    # Real Estate
    ("real estate", "Real Estate & Property Services"),
    ("realty", "Real Estate & Property Services"),
    ("realtors", "Real Estate & Property Services"),
    ("estates", "Real Estate & Property Services"),
    ("properties", "Real Estate & Property Services"),
    ("developers", "Real Estate & Property Services"),
    # Infrastructure
    ("infrastructure", "Infrastructure & Construction"),
    ("infra", "Infrastructure & Construction"),
    ("construction", "Infrastructure & Construction"),
    ("constructions", "Infrastructure & Construction"),
    ("epc", "Infrastructure & Construction"),
    ("roads", "Infrastructure & Construction"),
    ("ports", "Transportation & Logistics"),
    # Industrial
    # NOTE: bare "industries" is too generic (Reliance Industries, Hindalco
    # Industries, Sun Pharmaceutical Industries) — never used as a keyword.
    ("industrial", "Industrial Manufacturing & Engineering"),
    ("machinery", "Industrial Manufacturing & Engineering"),
    ("capital goods", "Industrial Manufacturing & Engineering"),
    ("engineering", "Industrial Manufacturing & Engineering"),
    ("engineers", "Industrial Manufacturing & Engineering"),
    ("automation", "Industrial Manufacturing & Engineering"),
    ("electricals", "Industrial Manufacturing & Engineering"),
    ("electric equipment", "Industrial Manufacturing & Engineering"),
    ("electrical equipment", "Industrial Manufacturing & Engineering"),
    # Auto
    ("automobile", "Automobiles & Mobility"),
    ("automotive", "Automobiles & Mobility"),
    ("auto component", "Automobiles & Mobility"),
    ("auto parts", "Automobiles & Mobility"),
    ("two-wheeler", "Automobiles & Mobility"),
    ("two wheeler", "Automobiles & Mobility"),
    ("passenger vehicle", "Automobiles & Mobility"),
    ("commercial vehicle", "Automobiles & Mobility"),
    ("electric vehicle", "Automobiles & Mobility"),
    ("motors", "Automobiles & Mobility"),
    ("tyres", "Automobiles & Mobility"),
    ("tires", "Automobiles & Mobility"),
    # Defence / Aerospace
    ("defence", "Aerospace & Defence"),
    ("defense", "Aerospace & Defence"),
    ("aerospace", "Aerospace & Defence"),
    ("weapons", "Aerospace & Defence"),
    ("military", "Aerospace & Defence"),
    ("dynamics", "Aerospace & Defence"),
    ("shipbuilders", "Aerospace & Defence"),  # MAZDOCK / GRSE / Cochin Shipyard
    ("shipyard", "Aerospace & Defence"),
    # Transport / Logistics
    ("logistics", "Transportation & Logistics"),
    ("shipping", "Transportation & Logistics"),
    ("freight", "Transportation & Logistics"),
    ("warehous", "Transportation & Logistics"),
    ("supply chain", "Transportation & Logistics"),
    ("container", "Transportation & Logistics"),
    # Aviation / Travel
    ("airline", "Aviation & Travel"),
    ("airport", "Aviation & Travel"),
    ("aviation", "Aviation & Travel"),
    ("travel", "Aviation & Travel"),
    ("tourism", "Aviation & Travel"),
    ("hotels", "Aviation & Travel"),
    ("hotel ", "Aviation & Travel"),
    ("hospitality", "Aviation & Travel"),
    ("resorts", "Aviation & Travel"),
    # Energy
    ("oil and gas", "Energy"),
    ("oil & gas", "Energy"),
    ("refining", "Energy"),
    ("fuel retail", "Energy"),
    ("petroleum", "Energy"),
    ("petrol", "Energy"),
    ("gas corporation", "Energy"),
    ("gas distribution", "Energy"),
    ("coal", "Energy"),
    ("power generation", "Energy"),
    ("power corporation", "Energy"),
    ("power grid", "Energy"),
    ("power ltd", "Energy"),
    ("power limited", "Energy"),
    ("renewable", "Energy"),
    ("biofuel", "Energy"),
    ("hydrogen", "Energy"),
    ("energy storage", "Energy"),
    ("solar", "Energy"),
    ("wind power", "Energy"),
    ("windmill", "Energy"),
    # Chemicals & Materials
    ("petrochemical", "Chemicals & Materials"),
    ("chemicals", "Chemicals & Materials"),
    ("chemical", "Chemicals & Materials"),
    ("fertilizer", "Chemicals & Materials"),
    ("fertiliser", "Chemicals & Materials"),
    ("paints", "Chemicals & Materials"),
    ("paint", "Chemicals & Materials"),
    ("coatings", "Chemicals & Materials"),
    ("dyes", "Chemicals & Materials"),
    ("pigments", "Chemicals & Materials"),
    ("polymers", "Chemicals & Materials"),
    ("industrial gas", "Chemicals & Materials"),
    # Metals & Mining
    ("steel", "Metals & Mining"),
    ("aluminium", "Metals & Mining"),
    ("aluminum", "Metals & Mining"),
    ("copper", "Metals & Mining"),
    ("zinc", "Metals & Mining"),
    ("mining", "Metals & Mining"),
    ("minerals", "Metals & Mining"),
    ("metals", "Metals & Mining"),
    ("ferrous", "Metals & Mining"),
    ("alloys", "Metals & Mining"),
    # Building Materials
    ("cement", "Building Materials"),
    ("ceramic", "Building Materials"),
    ("ceramics", "Building Materials"),
    ("glass", "Building Materials"),
    ("tiles", "Building Materials"),
    ("pipes", "Building Materials"),
    ("cables", "Building Materials"),
    ("insulation", "Building Materials"),
    ("building material", "Building Materials"),
    # Agriculture
    ("agriculture", "Agriculture & Agribusiness"),
    ("agritech", "Agriculture & Agribusiness"),
    ("agro tech", "Agriculture & Agribusiness"),
    ("agro", "Agriculture & Agribusiness"),
    ("farming", "Agriculture & Agribusiness"),
    ("seeds", "Agriculture & Agribusiness"),
    ("sugar", "Agriculture & Agribusiness"),
    ("ethanol", "Agriculture & Agribusiness"),
    ("seafood", "Agriculture & Agribusiness"),
    ("plantation", "Agriculture & Agribusiness"),
    ("plantations", "Agriculture & Agribusiness"),
    ("tea", "Agriculture & Agribusiness"),
    ("coffee", "Agriculture & Agribusiness"),
    # Food / Beverage / Consumer Staples
    ("dairy", "Food, Beverage & Consumer Staples"),
    ("packaged food", "Food, Beverage & Consumer Staples"),
    ("beverage", "Food, Beverage & Consumer Staples"),
    ("beverages", "Food, Beverage & Consumer Staples"),
    ("alcohol", "Food, Beverage & Consumer Staples"),
    ("breweries", "Food, Beverage & Consumer Staples"),
    ("brewery", "Food, Beverage & Consumer Staples"),
    ("distilleries", "Food, Beverage & Consumer Staples"),
    ("fmcg", "Food, Beverage & Consumer Staples"),
    ("consumer staples", "Food, Beverage & Consumer Staples"),
    ("food processing", "Food, Beverage & Consumer Staples"),
    ("foods", "Food, Beverage & Consumer Staples"),
    ("nestle", "Food, Beverage & Consumer Staples"),
    ("hindustan unilever", "Food, Beverage & Consumer Staples"),
    # Consumer Durables / Discretionary
    ("consumer durable", "Consumer Durables & Discretionary"),
    ("white goods", "Consumer Durables & Discretionary"),
    ("appliance", "Consumer Durables & Discretionary"),
    ("appliances", "Consumer Durables & Discretionary"),
    ("consumer electronic", "Consumer Durables & Discretionary"),
    ("electronics", "Consumer Durables & Discretionary"),
    ("batteries", "Consumer Durables & Discretionary"),
    ("watches", "Consumer Durables & Discretionary"),
    ("furniture", "Consumer Durables & Discretionary"),
    # Retail / E-Commerce
    ("e-commerce", "Retail & E-Commerce"),
    ("ecommerce", "Retail & E-Commerce"),
    ("marketplace", "Retail & E-Commerce"),
    ("retail", "Retail & E-Commerce"),
    ("stores", "Retail & E-Commerce"),
    # Healthcare
    ("pharmaceutical", "Healthcare & Life Sciences"),
    ("pharmaceuticals", "Healthcare & Life Sciences"),
    ("pharma", "Healthcare & Life Sciences"),
    ("biotech", "Healthcare & Life Sciences"),
    ("biotechnology", "Healthcare & Life Sciences"),
    ("hospital", "Healthcare & Life Sciences"),
    ("hospitals", "Healthcare & Life Sciences"),
    ("diagnostic", "Healthcare & Life Sciences"),
    ("diagnostics", "Healthcare & Life Sciences"),
    ("medical device", "Healthcare & Life Sciences"),
    ("healthcare", "Healthcare & Life Sciences"),
    ("life science", "Healthcare & Life Sciences"),
    ("life sciences", "Healthcare & Life Sciences"),
    ("drugs", "Healthcare & Life Sciences"),
    ("therapeutics", "Healthcare & Life Sciences"),
    # Textiles
    ("textile", "Textiles, Apparel & Luxury Goods"),
    ("textiles", "Textiles, Apparel & Luxury Goods"),
    ("apparel", "Textiles, Apparel & Luxury Goods"),
    ("garment", "Textiles, Apparel & Luxury Goods"),
    ("garments", "Textiles, Apparel & Luxury Goods"),
    ("footwear", "Textiles, Apparel & Luxury Goods"),
    ("leather", "Textiles, Apparel & Luxury Goods"),
    ("luxury", "Textiles, Apparel & Luxury Goods"),
    ("fashions", "Textiles, Apparel & Luxury Goods"),
    ("spinning mills", "Textiles, Apparel & Luxury Goods"),
    # Environmental
    ("recycling", "Environmental & Waste Services"),
    ("water treatment", "Environmental & Waste Services"),
    ("waste management", "Environmental & Waste Services"),
    ("pollution", "Environmental & Waste Services"),
    ("environmental", "Environmental & Waste Services"),
    # Conglomerates
    ("conglomerate", "Diversified Conglomerates / Holding Companies"),
    ("holding company", "Diversified Conglomerates / Holding Companies"),
    ("holdings", "Diversified Conglomerates / Holding Companies"),
    # Catch-all that should run LAST (energy is wide but high-confidence in ctx)
    ("utilities", "Energy"),
    ("energy", "Energy"),
]
_KEYWORDS.sort(key=lambda kv: len(kv[0]), reverse=True)


def _api_industry_keyword_match(api_industry: Optional[str]) -> Optional[str]:
    """Map Indian API's industry string via the curated lookup → keyword fallback."""
    if not api_industry:
        return None
    s = api_industry.strip().lower()
    if not s:
        return None
    if s in _INDIAN_API_INDUSTRY_MAP:
        return _INDIAN_API_INDUSTRY_MAP[s]
    # Substring match on the curated map's keys (for slight wording diffs).
    for k, v in _INDIAN_API_INDUSTRY_MAP.items():
        if k in s:
            return v
    # Final fallback: run the keyword cascade on the api industry text.
    return _keyword_match(s)


def _keyword_match(text: Optional[str]) -> Optional[str]:
    """Substring scan against the keyword cascade."""
    if not text:
        return None
    s = f" {text.lower()} "
    for kw, canon in _KEYWORDS:
        if kw in s:
            return canon
    return None


def classify(
    company_name: str,
    api_industry: Optional[str] = None,
) -> Tuple[Optional[str], str]:
    """
    Returns (canonical_label, source).

    source ∈ {'indian_api', 'keyword', 'unknown'}.

    Always picks the most specific match available; never returns a string
    outside CANON_NAMES.
    """
    name = (company_name or "").strip()
    api_ind = (api_industry or "").strip()

    # 1. Indian API industry → curated map
    hit = _api_industry_keyword_match(api_ind)
    if hit and hit in CANON_SET:
        return hit, "indian_api"

    # 2. Keyword cascade on company_name
    hit = _keyword_match(name)
    if hit and hit in CANON_SET:
        return hit, "keyword"

    return None, "unknown"


# ---------------------------------------------------------------------------
# Layer 3: LLM fallback (optional). Used by backfill admin endpoint when
# the deterministic cascade produces None for a row.
# ---------------------------------------------------------------------------
# Show only the labels (no descriptions) in the prompt's valid-labels block to
# stop the model from echoing back "<label> — <description>".
_TAXONOMY_LABELS_LIST = "\n".join(f"  - {name}" for name in CANON_NAMES)

_LLM_SYSTEM_PROMPT = f"""You classify Indian-listed companies into a FIXED industry taxonomy.

VALID LABELS — return EXACTLY one of these strings, character-for-character:
{_TAXONOMY_LABELS_LIST}

Hints (descriptions for context only, DO NOT include in the output):
{TAXONOMY_TEXT}

Rules:
  - For each item, pick exactly ONE industry — the most relevant.
  - Prefer the most specific industry over general ones.
  - Use "Diversified Conglomerates / Holding Companies" ONLY for true holdcos / multi-sector plays where no single industry dominates.
  - NEVER invent new labels. NEVER return a label that is not in the VALID LABELS list above.
  - The "industry" field must be ONLY the label string. Do NOT append descriptions, em-dashes, or any other text.

Output: a JSON object with key "classifications" whose value is an ARRAY.
Each element must be an object with:
  {{"id": <int>, "industry": "<canonical label>"}}

Return one entry per input item, preserving ids. No extra keys, no prose.
"""


# Normalisation table for common LLM-invented labels → canonical equivalents.
# Keys must be lowercase. Used by _coerce_to_canon().
_LLM_LABEL_ALIASES: Dict[str, str] = {
    "jewellery, gems & watches": "Consumer Durables & Discretionary",
    "jewelry, gems & watches": "Consumer Durables & Discretionary",
    "gems & jewellery": "Consumer Durables & Discretionary",
    "gems and jewellery": "Consumer Durables & Discretionary",
    "paper & packaging": "Industrial Manufacturing & Engineering",
    "packaging": "Chemicals & Materials",
    "paper": "Industrial Manufacturing & Engineering",
    "education": "Information Technology & Software",
    "education services": "Information Technology & Software",
    "edtech": "Information Technology & Software",
    "business services": "Information Technology & Software",
    "professional services": "Information Technology & Software",
    "consulting": "Information Technology & Software",
    "logistics": "Transportation & Logistics",
    "shipping": "Transportation & Logistics",
    "ports": "Transportation & Logistics",
    "utilities": "Energy",
    "power": "Energy",
    "renewables": "Energy",
    "telecom": "Telecommunications & Connectivity",
    "telecommunications": "Telecommunications & Connectivity",
    "media": "Media, Entertainment & Gaming",
    "entertainment": "Media, Entertainment & Gaming",
    "gaming": "Media, Entertainment & Gaming",
    "real estate": "Real Estate & Property Services",
    "reit": "Real Estate & Property Services",
    "construction": "Infrastructure & Construction",
    "infrastructure": "Infrastructure & Construction",
    "automobiles": "Automobiles & Mobility",
    "auto": "Automobiles & Mobility",
    "defence": "Aerospace & Defence",
    "defense": "Aerospace & Defence",
    "aerospace": "Aerospace & Defence",
    "aviation": "Aviation & Travel",
    "travel": "Aviation & Travel",
    "hospitality": "Aviation & Travel",
    "energy": "Energy",
    "oil & gas": "Energy",
    "chemicals": "Chemicals & Materials",
    "specialty chemicals": "Chemicals & Materials",
    "metals": "Metals & Mining",
    "mining": "Metals & Mining",
    "metals & mining": "Metals & Mining",
    "cement": "Building Materials",
    "building materials": "Building Materials",
    "agriculture": "Agriculture & Agribusiness",
    "agribusiness": "Agriculture & Agribusiness",
    "food": "Food, Beverage & Consumer Staples",
    "beverages": "Food, Beverage & Consumer Staples",
    "consumer staples": "Food, Beverage & Consumer Staples",
    "fmcg": "Food, Beverage & Consumer Staples",
    "consumer durables": "Consumer Durables & Discretionary",
    "consumer discretionary": "Consumer Durables & Discretionary",
    "retail": "Retail & E-Commerce",
    "e-commerce": "Retail & E-Commerce",
    "ecommerce": "Retail & E-Commerce",
    "healthcare": "Healthcare & Life Sciences",
    "pharmaceuticals": "Healthcare & Life Sciences",
    "pharma": "Healthcare & Life Sciences",
    "life sciences": "Healthcare & Life Sciences",
    "biotechnology": "Healthcare & Life Sciences",
    "textiles": "Textiles, Apparel & Luxury Goods",
    "apparel": "Textiles, Apparel & Luxury Goods",
    "luxury goods": "Textiles, Apparel & Luxury Goods",
    "environmental services": "Environmental & Waste Services",
    "waste management": "Environmental & Waste Services",
    "conglomerate": "Diversified Conglomerates / Holding Companies",
    "conglomerates": "Diversified Conglomerates / Holding Companies",
    "holding company": "Diversified Conglomerates / Holding Companies",
    "diversified": "Diversified Conglomerates / Holding Companies",
    "financials": "Financial Services",
    "banking": "Financial Services",
    "banks": "Financial Services",
    "insurance services": "Insurance",
    "information technology": "Information Technology & Software",
    "it": "Information Technology & Software",
    "software": "Information Technology & Software",
    "technology": "Information Technology & Software",
}


def _coerce_to_canon(raw: str) -> Optional[str]:
    """
    Best-effort normalisation of an LLM 'industry' field back to a canonical
    24-label name. Handles three failure modes seen in practice:

      1. Exact match — returns as-is.
      2. "<label> — <description>" — the model echoed the prompt; strip
         the em-dash / hyphen tail and re-check.
      3. Invented label not in CANON_SET — fall back to a hand-curated alias
         table (above) keyed on lowercased label.
    """
    if not raw:
        return None
    if raw in CANON_SET:
        return raw

    # Strip "— description" / "- description" tails ("Label — words").
    for sep in (" — ", " – ", " - "):
        if sep in raw:
            head = raw.split(sep, 1)[0].strip()
            if head in CANON_SET:
                return head

    # Lowercased alias lookup.
    return _LLM_LABEL_ALIASES.get(raw.strip().lower())


def classify_with_llm(
    items: List[Dict[str, Any]],
    model: str = "gpt-4.1-nano",
    batch_size: int = 25,
    max_retries: int = 3,
) -> Dict[int, str]:
    """
    LLM fallback for rows the deterministic cascade couldn't classify.

    items: list of {"id": int, "company_name": str, "api_industry": str?}.
    Returns {id -> canonical_label}. Items the LLM also can't place are
    simply absent from the returned dict.
    """
    if not items:
        return {}
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        logger.warning("classify_with_llm: OPENAI_API_KEY not set; skipping LLM fallback")
        return {}

    try:
        from openai import OpenAI  # local import; only loaded when needed
    except ImportError:
        logger.warning("classify_with_llm: openai package not installed")
        return {}
    client = OpenAI(api_key=api_key)

    out: Dict[int, str] = {}
    for start in range(0, len(items), batch_size):
        batch = items[start:start + batch_size]
        user_lines = ["ITEMS:"]
        for it in batch:
            user_lines.append(
                f"- id: {it['id']}\n"
                f"  company_name: {it.get('company_name', '')}\n"
                f"  api_industry: {it.get('api_industry') or '(none)'}"
            )
        user_msg = "\n".join(user_lines)

        last_err: Optional[Exception] = None
        for attempt in range(max_retries):
            try:
                resp = client.chat.completions.create(
                    model=model,
                    temperature=0.0,
                    response_format={"type": "json_object"},
                    messages=[
                        {"role": "system", "content": _LLM_SYSTEM_PROMPT},
                        {"role": "user", "content": user_msg},
                    ],
                    max_tokens=1200,
                )
                data = json.loads(resp.choices[0].message.content or "{}")
                for v in data.get("classifications") or []:
                    try:
                        vid = int(v.get("id"))
                    except (TypeError, ValueError):
                        continue
                    raw = (v.get("industry") or "").strip()
                    label = _coerce_to_canon(raw)
                    if label:
                        out[vid] = label
                break
            except Exception as e:  # noqa: BLE001
                last_err = e
                time.sleep(1.5 * (attempt + 1))
        else:
            logger.warning("classify_with_llm: batch starting at %s failed: %s", start, last_err)
    return out
