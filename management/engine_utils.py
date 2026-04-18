import uuid
import math
import re
from statistics import median


# ---------------------------
# BASIC UTILITIES
# ---------------------------

def ensure_uuid(val=None):
    try:
        return str(uuid.UUID(str(val)))
    except Exception:
        return str(uuid.uuid4())


def safe_float(val, default=0.0):
    try:
        if val is None:
            return default
        return float(val)
    except Exception:
        return default


def clip(value, min_value=0.0, max_value=1.0):
    return max(min_value, min(max_value, value))


# ---------------------------
# NORMALIZATION
# ---------------------------

def robust_median(values):
    vals = [safe_float(v) for v in values if v is not None]
    if not vals:
        return 0.0
    return median(vals)


def robust_scale(value, values):
    """
    Robust scaling using median & MAD (Median Absolute Deviation)
    """
    # 🔥 FIX: pastikan values iterable
    if values is None:
        return 0.0

    if not isinstance(values, (list, tuple)):
        values = [values]

    vals = [safe_float(v) for v in values if v is not None]

    if not vals:
        return 0.0

    med = median(vals)
    mad = median([abs(v - med) for v in vals]) or 1e-9

    return (safe_float(value) - med) / mad


# ---------------------------
# MATH / TREND
# ---------------------------

def weighted_mean(values, weights):
    total_weight = sum(weights)
    if total_weight == 0:
        return 0.0
    return sum(v * w for v, w in zip(values, weights)) / total_weight


def safe_pct_change(current, previous):
    current = safe_float(current)
    previous = safe_float(previous)

    if previous == 0:
        return 0.0

    return (current - previous) / abs(previous)


def ewma_last(values, alpha=0.3):
    """
    Exponentially Weighted Moving Average (last value)
    """
    if not values:
        return 0.0

    ewma = values[0]
    for v in values[1:]:
        ewma = alpha * v + (1 - alpha) * ewma

    return ewma


# ---------------------------
# SCORING HELPERS
# ---------------------------

def apply_direction(value, direction="higher_better"):
    """
    Normalize direction:
    - higher_better → 그대로
    - lower_better → dibalik
    """
    value = safe_float(value)

    if direction == "lower_better":
        return -value
    return value


# ---------------------------
# STRING NORMALIZATION
# ---------------------------

def normalize_country_cd(code):
    if not code:
        return "UNKNOWN"
    return str(code).strip().upper()


def normalize_country_nm(name, code):
    if code:
        return str(code).strip().lower()
    if name:
        return str(name).strip().lower()
    return "unknown"


def normalize_domain(domain):
    if not domain:
        return ""

    domain = domain.lower().strip()

    # remove protocol
    domain = re.sub(r"https?://", "", domain)

    # remove www
    domain = re.sub(r"^www\.", "", domain)

    # remove trailing slash
    domain = domain.rstrip("/")

    return domain


# ---------------------------
# LABEL / TAGGING
# ---------------------------

def pick_top_labels(labels, top_n=3):
    if not labels:
        return []

    from collections import Counter

    counter = Counter(labels)
    sorted_items = counter.most_common(top_n)

    return [k for k, _ in sorted_items]