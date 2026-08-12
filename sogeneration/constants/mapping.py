"""Static mapping data for sales-order generation.

Pure data only — no imports, no logic, no DB access. Anything in here can be
edited by a non-developer without touching the processing code.
"""

# ── Shutter finishes ──────────────────────────────────────────────────────────

SHUTTER_FINISH_MAPPING = {
    "Sandalwood":      "Courtyard Clay Gloss",
    "Soundcloud":      "Mistfield Gloss",
    "Washed Earth":    "Canyon Ridge Gloss",
    "Starlight White": "Glacier Veil Gloss",
    "Asteroid Belt":   "Industrial Bay Matte",
}

PRELAM_FINISHES = {
    "Back Painted Fluted Glass Ivory Matt (Prelam)",
    "Back Painted Fluted Glass Ash Matt (Prelam)",
    "Back Painted Fluted Glass Biscuit Matt (Prelam)",
    "Back Painted Fluted Glass Maple Bronze Gloss (Prelam)",
    "Back Painted Frosted Glass Beige Matt (Prelam)",
    "Back Painted Frosted Glass Graphite Matt (Prelam)",
    "Back Painted Sandstone Gloss (Prelam)",
    "Back Painted Pebble Gloss (Prelam)",
    "Fluted Glass Vanilla Matt (Prelam)",
    "Fluted Glass Coffee Matt (Prelam)",
    "Fluted Glass Onyx Matt (Prelam)",
    "Fluted Glass Snow Gloss (Prelam)",
    "Fluted Glass Caramel Gloss (Prelam)",
    "Fluted Glass Black Gloss (Prelam)",
    "Sandwich Glass Bronze Veil (Prelam)",
    "Sandwich Glass Bronze Grid (Prelam)",
    "Frosted Glass Mist (Prelam)",
    "Fluted Glass Ridge (Prelam)",
    "Fluted Glass Fine Ridge (Prelam)",
    "Textured Glass Glacier (Prelam)",
    "Clear Glass (Prelam)",
    "Black Tinted Glass (Prelam)",
    "Clear Fluted Glass (Prelam)",
    "Brown Tinted Glass (Prelam)",
    "Brown Fluted Glass (Prelam)",
}


# ── Glass shutter profiles ────────────────────────────────────────────────────

GLASS_SHUTTER_PROFILE_MAPPING = {
    "KAPS-59 MB": "GLASS SHUTTER PROFILE: Matt Black ( KAPS-59 MB )",
    "KAPS-59 MG": "GLASS SHUTTER PROFILE: Matt Gold ( KAPS-59 MG )",
    "KAPS-59 SS": "GLASS SHUTTER PROFILE: Silver ( KAPS-59 SS )",
    "SCP-06 MB":  "GLASS SHUTTER PROFILE: Matt Black ( SCP-06 MB )",
    "SCP-06 MG":  "GLASS SHUTTER PROFILE: Matt Gold ( SCP-06 MG )",
    "SCP-06 SS":  "GLASS SHUTTER PROFILE: Silver ( SCP-06 SS )",
    "KSP-01 MB":  "GLASS SHUTTER PROFILE: Matt Black ( KSP-01 MB )",
    "KSP-01 MG":  "GLASS SHUTTER PROFILE: Matt Gold ( KSP-01 MG )",
    "KSP-01 SS":  "GLASS SHUTTER PROFILE: Silver ( KSP-01 SS )",
    "BGK-01":     "GLASS SHUTTER PROFILE: Rose Gold 45 mm Profile",
    "BGK-04":     "GLASS SHUTTER PROFILE: Gold 45 mm Profile",
    "BGK-05":     "GLASS SHUTTER PROFILE: Black 45 mm Profile",
    "BGK-07":     "GLASS SHUTTER PROFILE: Champagne 45 mm Profile",
    "BGK-06":     "GLASS SHUTTER PROFILE: Silver 45 mm Profile",
}

GLASS_SHUTTER_MODELS = set(GLASS_SHUTTER_PROFILE_MAPPING.keys())


# ── Cabinet model groups ──────────────────────────────────────────────────────

LIGHT_CABINET = [
    "MK-0619", "MK-0946", "MK-0469", "MK-0940", "MK-0620", "MK-0947",
    "MK-0614", "MK-0941", "MK-0621", "MK-0948", "MK-0470", "MK-0942",
    "MK-0697", "MK-0969", "MK-0626", "MK-0965", "MK-0465", "MK-0972",
    "MK-0714", "MK-0971", "MK-0616", "MK-0954", "MK-0615", "MK-0943",
    "MK-0462", "MK-0968", "MK-0625", "MK-0966", "MK-1071", "MK-1070",
    "MK-1068", "MK-1069", "MK-0617", "MK-0955", "MK-1072", "MK-0979",
    "MK-0810", "MK-0975", "MK-0455", "MK-0944", "MK-0458", "MK-0970",
    "MK-0457", "MK-0974", "MK-0522", "MK-0973", "MK-0523", "MK-0967",
]

# MK-* models that must be processed through the filler path.
MK_FIL_MODELS = {
    "MK-0777", "MK-1145", "MK-0766", "MK-0834",
    "MK-0775", "MK-0835", "MK-0725", "MK-0836", "MK-1263",
}

# P-series models processed as fillers, each followed by an M-CF-217 line.
P_FIL_MODELS = {"P1725-AA", "P1724-AA", "P1723-AA", "P1722-AA"}


# ── Filler model → cabinet category (from the filler reference sheet) ─────────

LC_FILLERS = {
    "FIL-0001", "FIL-0002", "FIL-0003",
    "FIL-0043", "FIL-0044", "FIL-0045",
}

UC_FILLERS = {
    "FIL-0004", "FIL-0005", "FIL-0006", "FIL-0007",
    "FIL-0008", "FIL-0009", "FIL-0058", "FIL-0059",
    "FIL-0060", "FIL-0061",
}

LOFT_FILLERS = {
    "FIL-0010", "FIL-0011", "FIL-0012", "FIL-0013", "FIL-0014",
    "FIL-0015", "FIL-0016", "FIL-0017", "FIL-0018", "FIL-0019",
    "FIL-0020", "FIL-0021", "FIL-0022", "FIL-0023", "FIL-0024",
    "FIL-0025", "FIL-0026", "FIL-0027", "FIL-0028", "FIL-0029",
    "FIL-0030", "FIL-0037", "FIL-0038", "FIL-0039", "FIL-0040",
    "FIL-0041", "FIL-0042", "FIL-0046", "FIL-0047", "FIL-0048",
    "FIL-0049", "FIL-0050", "FIL-0051", "FIL-0052", "FIL-0053",
    "FIL-0054", "FIL-0055", "FIL-0056", "FIL-0057",
}

# Extra filler codes injected per category (qty 1 each, once per sheet).
FILLER_EXTRAS = {
    "LC":   ["FIL-0001"],
    "UC":   ["FIL-0005"],
    "Loft": ["FIL-0056"],
}

# Description used on the injected extra-filler rows.
FILLER_EXTRA_SUFFIX = "AD"
FILLER_EXTRA_FINISH = "Glacier Veil Matte"


# ── Gola → skirting mapping (Sheet1) ──────────────────────────────────────────

GOLA_TO_SKIRTING = {
    "HG3L-AT": "PVCSE-10-AT-30",
    "HG3L-BG": "PVCSE-10-BG-30",
    "HG3L-SF": "PVCSE-10-BF-30",
    "9299225": "PVCSE-10-BL-30",
    "9345616": "PVCSE-10-SL-30",
    "9345615": "PVCSE-10-BF-30",
}

# ── Infurnia code → odoo_code (Sheet2) ────────────────────────────────────────
# Used only as a fallback when the DB lookup does not resolve the code.
INFURNIA_TO_ODOO = {
    # gola profiles
    "HG3L-AT": "HW-0981",
    "HG3L-BG": "HW-0272",
    "HG3L-SF": "PR-119",
    "9299225": "PR-028",
    "9345616": "PR-105",
    "9345615": "PR-104",
    # skirting seals
    "PVCSE-10-AT-30": "HW-0861",
    "PVCSE-10-BG-30": "HW-0880",
    "PVCSE-10-BF-30": "HW-0872",
    "PVCSE-10-BL-30": "HW-0846",
    "PVCSE-10-SL-30": "HW-0857",
}

# 1 skirting line per 1 gola line. Change if the ratio is not 1:1.
SKIRTING_QTY_RATIO = 1

# Report a gola row in the Failed sheet when its matching skirting seal was not
# punched anywhere in the input sheet.
FLAG_MISSING_SKIRTING = True

# Drop that gola (and its Point 1 accessory) from the output as well.
# False = punch the gola and its PR line anyway, and treat the Failed entry as
# a warning for the design team.
DROP_GOLA_WITHOUT_SKIRTING = False

# Auto-emit the skirting line alongside a gola. Off by default: a gola only
# survives the check above when its skirting is already punched in the sheet,
# and that skirting row generates its own output line. Turning this on will
# produce a duplicate skirting line when the gola is processed first.
AUTO_ADD_SKIRTING = False

# Do not add a skirting line if an identical one already exists for the
# same cabinet position (e.g. the input already carried it explicitly).
SKIP_DUPLICATE_SKIRTING = True


# ── Point 1: gola profile → PR-021, qty = 2 × number of LC cabinets ──────────
# Punched once per sheet when any of these golas survives the skirting check.
GOLA_LC_ACCESSORY = {
    "HG3L-AT": "PR-021",
    "HG3L-BG": "PR-021",
    "HG3L-SF": "PR-021",
    "9345614": "PR-021",
    "9345616": "PR-021",
    "9345615": "PR-021",
}

# qty = LC cabinet count × this
GOLA_LC_ACCESSORY_MULTIPLIER = 2

# A cabinet counts as LC when its description carries the LC token, e.g.
# "[MK-0654] LC-1000-1S-450-LH-H".
LC_DESCRIPTION_PATTERN = r"\bLC\b"


# ── Point 2: skirting seal → two accessory codes, qty = 2 × line qty ─────────
SKIRTING_ACCESSORIES = {
    "PVCSE-10-AT-30": ["HW-0860", "HW-0862"],
    "PVCSE-10-BG-30": ["HW-0875", "HW-0881"],
    "PVCSE-10-BF-30": ["HW-0871", "HW-0873"],
    "PVCSE-10-BL-30": ["HW-0845", "HW-0847"],
    "PVCSE-10-SL-30": ["HW-0856", "HW-0858"],
}

# qty = the skirting line's own quantity × this
SKIRTING_ACCESSORY_MULTIPLIER = 2


# ── Output sheet ──────────────────────────────────────────────────────────────

COLUMN_ORDER = [
    "Customer",
    "GST Treatment",
    "POC",
    "Cabinet Position",
    "Tag",
    "Project Name",
    "Order Lines/Product",
    "Order Lines/Description",
    "Order Lines / Quantity",
]

# Static product codes emitted by the pipeline.
SERVICE_CHARGE_PRODUCT = "SR-0001"
P_FIL_COMPANION_PRODUCT = "M-CF-217"
PSU_PRODUCT = "PR-047"
LIGHT_LENGTH_PRODUCT = "PR-048"

# 1 PR-047 per this many light cabinets (rounded up).
PSU_CABINETS_PER_UNIT = 5

# 1 PR-048 per this many metres of light length (rounded up).
LIGHT_METRES_PER_UNIT = 5