from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime
import math
import uuid
from zoneinfo import ZoneInfo
from collections import defaultdict
import pandas as pd

from .database import insert_df, query_df

from .engine_utils import (
    apply_direction,
    clip,
    ensure_uuid,
    ewma_last,
    pick_top_labels,
    normalize_domain,
    normalize_country_cd,
    normalize_country_nm,
    robust_median,
    robust_scale,
    safe_float,
    safe_pct_change,
    weighted_mean,
)


STATUS_TABLE = "hris_trendHorizone.fact_site_country_status_history"
EVENT_TABLE = "hris_trendHorizone.fact_change_event_long"

SCORABLE_JOIN_STATUSES = {
    "OK",
    "SOURCE_ONLY_NO_META",
    "SOURCE_ONLY_NO_META_ADSENSE",
    "SOURCE_ONLY_NO_META_ADX",
}

NEGATIVE_ROOT_LABELS = {
    "TRAFFIC_DROP",
    "SERVING_DROP",
    "YIELD_DROP",
    "VIEWABILITY_DROP",
    "EFFICIENCY_DROP",
    "REVENUE_DROP",
    "NEGATIVE_MIXED",
    "NEG_ADJUSTMENT",
    "RED_FLAG_IVT",
    "RED_FLAG_CLICK_STRESS",
    "RED_FLAG_SERVING_SUPPRESSION",
    "RED_FLAG_ATTENTION_COLLAPSE",
    "RED_FLAG_COUNTER_CORRECTION",
    "RED_FLAG_FUNNEL_DIVERGENCE",
}

POSITIVE_ROOT_LABELS = {
    "POSITIVE_EXPANSION",
    "POSITIVE_RECOVERY",
}

COUNTER_CORRECTION_COLUMNS = {
    "meta_spend",
    "meta_clicks",
    "meta_lpv",
    "adsense_estimated_earnings",
    "adsense_page_views",
    "adsense_clicks",
    "adsense_ad_requests",
    "adsense_matched_ad_requests",
    "adsense_impressions",
    "adx_revenue",
    "adx_impressions",
    "adx_clicks",
    "adx_total_requests",
    "adx_responses_served",
    "blended_revenue",
}

SOURCE_MODE_ADSENSE_ONLY = "ADSENSE_ONLY"
SOURCE_MODE_ADX_ONLY = "ADX_ONLY"
SOURCE_MODE_MIXED = "MIXED"

BLENDED_DUPLICATE_COLUMNS_BY_SOURCE_MODE = {
    SOURCE_MODE_ADSENSE_ONLY: {
        "blended_revenue",
        "attention_quality_blended",
        "active_time_blended",
        "viewability_gap_abs",
    },
    SOURCE_MODE_ADX_ONLY: {
        "blended_revenue",
        "request_to_impression_eff",
        "attention_quality_blended",
        "active_time_blended",
        "viewability_gap_abs",
    },
    SOURCE_MODE_MIXED: set(),
}

SOURCE_SCOPE_BLOCKLIST_BY_SOURCE_MODE = {
    SOURCE_MODE_ADSENSE_ONLY: {"adx"},
    SOURCE_MODE_ADX_ONLY: {"adsense"},
    SOURCE_MODE_MIXED: set(),
}


@dataclass(frozen=True)
class MetricRule:
    column: str
    source_scope: str
    metric_group: str
    metric_type: str
    funnel_stage: str
    metric_role: str
    expected_direction: str
    score_method: str
    base_weight: float
    positive_weight: float = 1.0
    negative_weight: float = 1.0
    adjustment_weight: float = 0.0
    ivt_weight: float = 0.0
    volume_gate_column: str = ""
    min_volume: float = 0.0
    denominator_column: str = ""
    numerator_column: str = ""
    deadband_pct: float = 0.05
    deadband_abs: float = 0.0
    same_hour_lookback_days: int = 30
    min_history_points: int = 6
    ewma_alpha: float = 0.30
    sigma_clip: float = 3.0
    family_key: str = ""
    family_rank: int = 1
    range_low: float | None = None
    range_high: float | None = None
    band_dynamic: bool = False
    band_quantile_low: float = 0.20
    band_quantile_high: float = 0.80
    band_sigma: float = 1.25
    band_min_width_pct: float = 0.10
    provisional_metric: bool = False
    freshness_min_confidence_factor: float = 0.60
    freshness_full_confidence_hours: int = 6
    label_positive: str = ""
    label_negative: str = ""
    requires_source_match: bool = True
    requires_join_status_ok: bool = False

    @property
    def rule_key(self) -> str:
        return f"{self.source_scope}:{self.column}"


RULES: list[MetricRule] = [
    MetricRule("meta_spend", "meta", "control", "counter", "acquisition_cost", "adjustment_guardrail", "NEUTRAL", "HOURLY_INCREMENT_Z", 0.00, adjustment_weight=1.25, deadband_pct=0.03, family_key="meta_budget", family_rank=2, label_negative="META_SPEND_DROP", requires_source_match=False),
    # MetricRule("meta_budget_pacing_index", "meta", "control", "band", "budget_pacing", "pacing", "RANGE_GOOD", "BAND_TARGET", 0.55, 0.75, 1.10, ivt_weight=0.30, volume_gate_column="meta_daily_budget", min_volume=1, family_key="meta_budget", family_rank=1, range_low=0.70, range_high=1.30, band_dynamic=True, band_quantile_low=0.20, band_quantile_high=0.80, band_sigma=1.00, band_min_width_pct=0.08, label_positive="BUDGET_PACING_IN_RANGE", label_negative="BUDGET_PACING_OUT_OF_RANGE", requires_source_match=True),
    MetricRule("meta_avg_cpc", "meta", "efficiency", "level", "acquisition_cost", "guardrail", "DOWN_GOOD", "EWMA_LEVEL_Z", 0.85, 0.85, 1.05, ivt_weight=0.20, volume_gate_column="meta_clicks", min_volume=20, family_key="meta_efficiency", family_rank=1, label_positive="META_CPC_IMPROVING", label_negative="META_CPC_RISING", requires_source_match=False),
    MetricRule("meta_clicks", "meta", "traffic", "counter", "acquisition", "primary", "UP_GOOD", "HOURLY_INCREMENT_Z", 1.10, 0.95, 1.10, adjustment_weight=0.75, ivt_weight=0.25, deadband_pct=0.03, family_key="meta_traffic_volume", family_rank=2, label_positive="META_TRAFFIC_UP", label_negative="META_TRAFFIC_DOWN", requires_source_match=False),
    MetricRule("meta_lpv", "meta", "traffic", "counter", "landing", "primary", "UP_GOOD", "HOURLY_INCREMENT_Z", 1.35, 0.95, 1.15, adjustment_weight=1.00, ivt_weight=0.35, deadband_pct=0.03, family_key="meta_traffic_volume", family_rank=1, label_positive="LPV_UP", label_negative="LPV_DOWN", requires_source_match=False),
    MetricRule("meta_lpv_rate", "meta", "efficiency", "rate", "landing_quality", "derived", "UP_GOOD", "PROPORTION_Z", 1.10, 0.90, 1.15, ivt_weight=0.55, volume_gate_column="meta_clicks", min_volume=20, denominator_column="meta_clicks", numerator_column="meta_lpv", family_key="meta_traffic_quality", family_rank=1, deadband_pct=0.04, label_positive="LPV_RATE_IMPROVING", label_negative="LPV_RATE_DROP", requires_source_match=False),
    MetricRule("meta_frequency", "meta", "quality", "band", "fatigue", "guardrail", "RANGE_GOOD", "BAND_TARGET", 0.45, 0.70, 1.00, ivt_weight=0.65, family_key="meta_fatigue", family_rank=1, range_low=1.0, range_high=3.5, band_dynamic=True, band_quantile_low=0.20, band_quantile_high=0.80, band_sigma=1.00, band_min_width_pct=0.08, label_positive="FREQUENCY_IN_RANGE", label_negative="FREQUENCY_OUT_OF_RANGE", requires_source_match=False),

    MetricRule("adsense_estimated_earnings", "adsense", "revenue", "counter", "monetization", "primary", "UP_GOOD", "HOURLY_INCREMENT_Z", 1.70, 0.95, 1.15, adjustment_weight=1.60, ivt_weight=0.45, deadband_pct=0.03, family_key="adsense_revenue", family_rank=1, provisional_metric=True, freshness_min_confidence_factor=0.55, freshness_full_confidence_hours=8, label_positive="ADSENSE_REVENUE_UP", label_negative="ADSENSE_REVENUE_DROP"),
    MetricRule("adsense_page_views", "adsense", "traffic", "counter", "onsite_traffic", "primary", "UP_GOOD", "HOURLY_INCREMENT_Z", 0.95, 0.90, 1.05, adjustment_weight=0.80, ivt_weight=0.20, deadband_pct=0.03, family_key="adsense_page_yield", family_rank=3, label_positive="PAGE_VIEWS_UP", label_negative="PAGE_VIEWS_DOWN"),
    MetricRule("adsense_clicks", "adsense", "engagement", "counter", "ad_engagement", "secondary", "UP_GOOD", "HOURLY_INCREMENT_Z", 0.75, 0.85, 1.05, adjustment_weight=0.60, ivt_weight=0.70, deadband_pct=0.03, family_key="adsense_click_yield", family_rank=2, label_positive="ADSENSE_CLICKS_UP", label_negative="ADSENSE_CLICKS_DOWN"),
    MetricRule("adsense_cost_per_click", "adsense", "yield", "level", "click_yield", "derived", "UP_GOOD", "EWMA_LEVEL_Z", 0.85, 0.85, 1.10, ivt_weight=0.65, volume_gate_column="adsense_clicks", min_volume=20, denominator_column="adsense_clicks", family_key="adsense_click_yield", family_rank=1, deadband_pct=0.05, provisional_metric=True, freshness_min_confidence_factor=0.60, freshness_full_confidence_hours=8, label_positive="ADSENSE_CPC_UP", label_negative="ADSENSE_CPC_DOWN"),
    MetricRule("adsense_page_views_rpm", "adsense", "yield", "level", "page_yield", "primary_derived", "UP_GOOD", "EWMA_LEVEL_Z", 1.25, 0.90, 1.15, ivt_weight=0.80, volume_gate_column="adsense_page_views", min_volume=500, denominator_column="adsense_page_views", family_key="adsense_page_yield", family_rank=1, deadband_pct=0.05, provisional_metric=True, freshness_min_confidence_factor=0.60, freshness_full_confidence_hours=8, label_positive="RPM_UP", label_negative="RPM_DOWN"),
    MetricRule("adsense_ad_requests", "adsense", "delivery", "counter", "request", "primary", "UP_GOOD", "HOURLY_INCREMENT_Z", 0.95, 0.90, 1.05, adjustment_weight=1.00, ivt_weight=0.20, deadband_pct=0.03, family_key="adsense_request_volume", family_rank=3, label_positive="AD_REQUESTS_UP", label_negative="AD_REQUESTS_DOWN"),
    MetricRule("adsense_matched_ad_requests", "adsense", "delivery", "counter", "match", "primary", "UP_GOOD", "HOURLY_INCREMENT_Z", 1.05, 0.90, 1.10, adjustment_weight=1.00, ivt_weight=0.35, deadband_pct=0.03, family_key="adsense_request_volume", family_rank=2, label_positive="MATCHED_REQUESTS_UP", label_negative="MATCHED_REQUESTS_DOWN"),
    MetricRule("adsense_impressions", "adsense", "delivery", "counter", "render", "primary", "UP_GOOD", "HOURLY_INCREMENT_Z", 1.00, 0.90, 1.10, adjustment_weight=0.90, ivt_weight=0.35, deadband_pct=0.03, family_key="adsense_request_volume", family_rank=1, label_positive="IMPRESSIONS_UP", label_negative="IMPRESSIONS_DOWN"),
    MetricRule("adsense_ad_requests_coverage", "adsense", "delivery", "rate", "fill", "primary", "UP_GOOD", "PROPORTION_Z", 1.25, 0.80, 1.25, ivt_weight=1.00, volume_gate_column="adsense_ad_requests", min_volume=200, denominator_column="adsense_ad_requests", numerator_column="adsense_matched_ad_requests", family_key="adsense_request_rate", family_rank=1, deadband_pct=0.03, label_positive="COVERAGE_UP", label_negative="COVERAGE_DROP"),
    MetricRule("adsense_active_view_viewability", "adsense", "quality", "rate", "viewability", "primary", "UP_GOOD", "PROPORTION_Z", 0.85, 0.80, 1.15, ivt_weight=0.70, volume_gate_column="adsense_impressions", min_volume=500, denominator_column="adsense_impressions", family_key="adsense_viewability", family_rank=1, deadband_pct=0.03, label_positive="VIEWABILITY_UP", label_negative="VIEWABILITY_DROP"),
    MetricRule("adsense_active_view_measurability", "adsense", "quality", "rate", "measurement", "support", "UP_GOOD", "PROPORTION_Z", 0.45, 0.70, 1.00, ivt_weight=0.40, volume_gate_column="adsense_impressions", min_volume=500, denominator_column="adsense_impressions", family_key="adsense_viewability", family_rank=2, deadband_pct=0.03, label_positive="MEASURABILITY_UP", label_negative="MEASURABILITY_DROP"),
    MetricRule("adsense_active_view_time", "adsense", "quality", "level", "attention", "support", "UP_GOOD", "EWMA_LEVEL_Z", 0.55, 0.75, 1.00, ivt_weight=0.70, volume_gate_column="adsense_impressions", min_volume=500, family_key="adsense_attention", family_rank=1, deadband_pct=0.04, label_positive="ACTIVE_VIEW_TIME_UP", label_negative="ACTIVE_VIEW_TIME_DOWN"),

    MetricRule("adx_revenue", "adx", "revenue", "counter", "monetization", "primary", "UP_GOOD", "HOURLY_INCREMENT_Z", 1.80, 0.95, 1.20, adjustment_weight=1.70, ivt_weight=0.45, deadband_pct=0.03, family_key="adx_revenue", family_rank=1, provisional_metric=True, freshness_min_confidence_factor=0.55, freshness_full_confidence_hours=8, label_positive="ADX_REVENUE_UP", label_negative="ADX_REVENUE_DROP"),
    MetricRule("adx_impressions", "adx", "delivery", "counter", "render", "primary", "UP_GOOD", "HOURLY_INCREMENT_Z", 1.05, 0.90, 1.10, adjustment_weight=0.90, ivt_weight=0.35, deadband_pct=0.03, family_key="adx_request_volume", family_rank=1, label_positive="ADX_IMPRESSIONS_UP", label_negative="ADX_IMPRESSIONS_DOWN"),
    MetricRule("adx_clicks", "adx", "engagement", "counter", "ad_engagement", "secondary", "UP_GOOD", "HOURLY_INCREMENT_Z", 0.70, 0.85, 1.05, adjustment_weight=0.50, ivt_weight=0.75, deadband_pct=0.03, family_key="adx_click_yield", family_rank=2, label_positive="ADX_CLICKS_UP", label_negative="ADX_CLICKS_DOWN"),
    MetricRule("adx_avg_ecpm", "adx", "yield", "level", "yield", "primary", "UP_GOOD", "EWMA_LEVEL_Z", 1.35, 0.90, 1.15, ivt_weight=0.80, volume_gate_column="adx_impressions", min_volume=500, denominator_column="adx_impressions", family_key="adx_yield", family_rank=1, deadband_pct=0.05, provisional_metric=True, freshness_min_confidence_factor=0.60, freshness_full_confidence_hours=8, label_positive="ECPM_UP", label_negative="ECPM_DOWN"),
    MetricRule("adx_cpc", "adx", "yield", "level", "click_yield", "derived", "UP_GOOD", "EWMA_LEVEL_Z", 0.80, 0.85, 1.10, ivt_weight=0.65, volume_gate_column="adx_clicks", min_volume=20, denominator_column="adx_clicks", family_key="adx_click_yield", family_rank=1, deadband_pct=0.05, provisional_metric=True, freshness_min_confidence_factor=0.60, freshness_full_confidence_hours=8, label_positive="ADX_CPC_UP", label_negative="ADX_CPC_DOWN"),
    MetricRule("adx_total_requests", "adx", "delivery", "counter", "request", "primary", "UP_GOOD", "HOURLY_INCREMENT_Z", 0.95, 0.90, 1.05, adjustment_weight=1.00, ivt_weight=0.20, deadband_pct=0.03, family_key="adx_request_volume", family_rank=3, label_positive="ADX_REQUESTS_UP", label_negative="ADX_REQUESTS_DOWN"),
    MetricRule("adx_responses_served", "adx", "delivery", "counter", "match", "primary", "UP_GOOD", "HOURLY_INCREMENT_Z", 1.05, 0.90, 1.10, adjustment_weight=1.00, ivt_weight=0.35, deadband_pct=0.03, family_key="adx_request_volume", family_rank=2, label_positive="RESPONSES_SERVED_UP", label_negative="RESPONSES_SERVED_DOWN"),
    MetricRule("adx_match_rate", "adx", "delivery", "rate", "match", "primary", "UP_GOOD", "PROPORTION_Z", 1.20, 0.80, 1.25, ivt_weight=1.00, volume_gate_column="adx_total_requests", min_volume=200, denominator_column="adx_total_requests", numerator_column="adx_responses_served", family_key="adx_request_rate", family_rank=2, deadband_pct=0.03, label_positive="MATCH_RATE_UP", label_negative="MATCH_RATE_DROP"),
    MetricRule("adx_total_fill_rate", "adx", "delivery", "rate", "fill", "primary", "UP_GOOD", "PROPORTION_Z", 1.30, 0.80, 1.30, ivt_weight=1.05, volume_gate_column="adx_total_requests", min_volume=200, denominator_column="adx_total_requests", numerator_column="adx_impressions", family_key="adx_request_rate", family_rank=1, deadband_pct=0.03, label_positive="FILL_RATE_UP", label_negative="FILL_RATE_DROP"),
    MetricRule("adx_active_view_pct_viewable", "adx", "quality", "rate", "viewability", "primary", "UP_GOOD", "PROPORTION_Z", 0.85, 0.80, 1.15, ivt_weight=0.70, volume_gate_column="adx_impressions", min_volume=500, denominator_column="adx_impressions", family_key="adx_viewability", family_rank=1, deadband_pct=0.03, label_positive="ADX_VIEWABILITY_UP", label_negative="ADX_VIEWABILITY_DROP"),
    MetricRule("adx_active_view_avg_time_sec", "adx", "quality", "level", "attention", "support", "UP_GOOD", "EWMA_LEVEL_Z", 0.55, 0.75, 1.00, ivt_weight=0.70, volume_gate_column="adx_impressions", min_volume=500, family_key="adx_attention", family_rank=1, deadband_pct=0.04, label_positive="ADX_ACTIVE_VIEW_TIME_UP", label_negative="ADX_ACTIVE_VIEW_TIME_DOWN"),

    MetricRule("blended_revenue", "blended", "revenue", "counter", "monetization", "composite_primary", "UP_GOOD", "HOURLY_INCREMENT_Z", 1.95, 0.95, 1.20, adjustment_weight=1.80, ivt_weight=0.55, deadband_pct=0.03, family_key="blended_revenue", family_rank=1, provisional_metric=True, freshness_min_confidence_factor=0.55, freshness_full_confidence_hours=8, label_positive="BLENDED_REVENUE_UP", label_negative="BLENDED_REVENUE_DROP", requires_source_match=False),
    MetricRule("revenue_per_lpv", "blended", "yield", "level", "monetization_efficiency", "composite", "UP_GOOD", "EWMA_LEVEL_Z", 1.45, 0.90, 1.15, ivt_weight=1.00, volume_gate_column="meta_lpv", min_volume=20, denominator_column="meta_lpv", family_key="blended_efficiency", family_rank=1, deadband_pct=0.05, provisional_metric=True, freshness_min_confidence_factor=0.60, freshness_full_confidence_hours=8, label_positive="REVENUE_PER_LPV_UP", label_negative="REVENUE_PER_LPV_DOWN", requires_source_match=False),
    MetricRule("roi_proxy", "blended", "efficiency", "level", "unit_economics", "composite", "UP_GOOD", "EWMA_LEVEL_Z", 1.35, 0.90, 1.15, ivt_weight=0.70, volume_gate_column="meta_spend", min_volume=500, denominator_column="meta_spend", family_key="blended_efficiency", family_rank=2, deadband_pct=0.05, provisional_metric=True, freshness_min_confidence_factor=0.60, freshness_full_confidence_hours=8, label_positive="ROI_PROXY_UP", label_negative="ROI_PROXY_DOWN", requires_source_match=False),
    MetricRule("meta_cost_per_lpv", "blended", "efficiency", "level", "acquisition_cost", "composite", "DOWN_GOOD", "EWMA_LEVEL_Z", 1.00, 0.85, 1.10, ivt_weight=0.45, volume_gate_column="meta_lpv", min_volume=20, denominator_column="meta_lpv", family_key="blended_efficiency", family_rank=3, deadband_pct=0.05, label_positive="COST_PER_LPV_DOWN", label_negative="COST_PER_LPV_UP", requires_source_match=False),
    MetricRule("request_to_impression_eff", "blended", "delivery", "rate", "render_efficiency", "composite", "UP_GOOD", "PROPORTION_Z", 1.25, 0.90, 1.20, ivt_weight=1.10, volume_gate_column="total_requests_blended", min_volume=300, denominator_column="total_requests_blended", numerator_column="total_impressions_blended", family_key="blended_delivery", family_rank=1, deadband_pct=0.03, label_positive="REQUEST_EFFICIENCY_UP", label_negative="REQUEST_EFFICIENCY_DOWN", requires_source_match=False),
    MetricRule("revenue_per_request_total", "blended", "yield", "level", "request_yield", "composite", "UP_GOOD", "EWMA_LEVEL_Z", 0.75, 0.85, 1.10, ivt_weight=0.90, volume_gate_column="total_requests_blended", min_volume=300, denominator_column="total_requests_blended", family_key="blended_delivery", family_rank=2, deadband_pct=0.05, provisional_metric=True, freshness_min_confidence_factor=0.60, freshness_full_confidence_hours=8, label_positive="REQUEST_YIELD_UP", label_negative="REQUEST_YIELD_DOWN", requires_source_match=False),
    MetricRule("click_pressure", "blended", "quality", "level", "click_stress", "composite", "DOWN_GOOD", "EWMA_LEVEL_Z", 0.45, 0.75, 1.00, ivt_weight=1.65, volume_gate_column="meta_lpv", min_volume=20, denominator_column="meta_lpv", family_key="blended_click_pressure", family_rank=1, deadband_pct=0.06, label_positive="CLICK_PRESSURE_DOWN", label_negative="CLICK_PRESSURE_UP", requires_source_match=False),
    MetricRule("request_density", "blended", "quality", "level", "request_density", "composite", "DOWN_GOOD", "EWMA_LEVEL_Z", 0.35, 0.75, 1.00, ivt_weight=1.15, volume_gate_column="meta_lpv", min_volume=20, denominator_column="meta_lpv", family_key="blended_click_pressure", family_rank=2, deadband_pct=0.06, label_positive="REQUEST_DENSITY_DOWN", label_negative="REQUEST_DENSITY_UP", requires_source_match=False),
    MetricRule("attention_quality_blended", "blended", "quality", "rate", "viewability", "composite", "UP_GOOD", "PROPORTION_Z", 0.90, 0.80, 1.10, ivt_weight=0.95, volume_gate_column="total_impressions_blended", min_volume=500, denominator_column="total_impressions_blended", family_key="blended_attention", family_rank=1, deadband_pct=0.03, label_positive="ATTENTION_QUALITY_UP", label_negative="ATTENTION_QUALITY_DROP", requires_source_match=True),
    MetricRule("active_time_blended", "blended", "quality", "level", "attention", "composite", "UP_GOOD", "EWMA_LEVEL_Z", 0.75, 0.80, 1.00, ivt_weight=0.75, volume_gate_column="total_impressions_blended", min_volume=500, family_key="blended_attention", family_rank=2, deadband_pct=0.04, label_positive="ACTIVE_TIME_BLENDED_UP", label_negative="ACTIVE_TIME_BLENDED_DOWN", requires_source_match=False),
    MetricRule("viewability_gap_abs", "blended", "quality", "level", "source_alignment", "composite", "DOWN_GOOD", "EWMA_LEVEL_Z", 0.35, 0.75, 1.00, ivt_weight=0.85, volume_gate_column="total_impressions_blended", min_volume=500, family_key="blended_attention", family_rank=3, deadband_pct=0.05, label_positive="VIEWABILITY_GAP_DOWN", label_negative="VIEWABILITY_GAP_UP", requires_source_match=False),
]


RAW_JOIN_COLUMNS = [
    "batch_id",
    "run_time",
    "run_date",
    "run_hour",
    "entity_key",
    "site",
    "country_code",
    "country_name",
    "date",
    "mapped_revenue_source",
    "revenue_value",
    "join_status",
    "meta_daily_budget",
    "meta_spend",
    "meta_avg_cpc",
    "meta_clicks",
    "meta_lpv",
    "meta_lpv_rate",
    "meta_frequency",
    "adsense_estimated_earnings",
    "adsense_page_views",
    "adsense_clicks",
    "adsense_cost_per_click",
    "adsense_page_views_rpm",
    "adsense_ad_requests",
    "adsense_matched_ad_requests",
    "adsense_impressions",
    "adsense_ad_requests_coverage",
    "adsense_active_view_viewability",
    "adsense_active_view_measurability",
    "adsense_active_view_time",
    "adx_revenue",
    "adx_impressions",
    "adx_clicks",
    "adx_avg_ecpm",
    "adx_cpc",
    "adx_total_requests",
    "adx_responses_served",
    "adx_match_rate",
    "adx_total_fill_rate",
    "adx_active_view_pct_viewable",
    "adx_active_view_avg_time_sec",
]

RATE_RULE_COLUMNS = {r.column for r in RULES if r.score_method == "PROPORTION_Z"}
RAW_RATE_COLUMNS = sorted(set(RATE_RULE_COLUMNS) & set(RAW_JOIN_COLUMNS))
DERIVED_COLUMNS = sorted({r.column for r in RULES if r.column not in RAW_JOIN_COLUMNS})
REQUIRED_METRIC_COLUMNS = sorted(
    {r.column for r in RULES}
    | {r.volume_gate_column for r in RULES if r.volume_gate_column}
    | {r.denominator_column for r in RULES if r.denominator_column}
    | {r.numerator_column for r in RULES if r.numerator_column}
    | {
        "blended_revenue",
        "total_clicks_blended",
        "total_requests_blended",
        "total_impressions_blended",
        "revenue_per_lpv",
        "roi_proxy",
        "meta_cost_per_lpv",
        "request_to_impression_eff",
        "revenue_per_request_total",
        "click_pressure",
        "request_density",
        "attention_quality_blended",
        "active_time_blended",
        "viewability_gap_abs",
    }
)

COUNTER_RULES = {r.column for r in RULES if r.score_method == "HOURLY_INCREMENT_Z"}

STATUS_COMPAT_COLUMNS = [
    "batch_id",
    "run_time",
    "run_date",
    "run_hour",
    "entity_key",
    "site",
    "country_code",
    "country_name",
    "date",
    "mapped_revenue_source",
    "join_status",
    "status_scope",
    "spend",
    "revenue_value",
    "health_score",
    "adjustment_score",
    "confidence",
    "positive_signal_count",
    "negative_signal_count",
    "neutral_signal_count",
    "skipped_signal_count",
    "adjustment_drop_count",
    "traffic_score",
    "delivery_score",
    "yield_score",
    "quality_score",
    "revenue_score",
    "efficiency_score",
    "engagement_score",
    "control_score",
    "top_positive_labels",
    "top_negative_labels",
    "top_positive_headers",
    "top_negative_headers",
    "root_cause_label",
    "final_label",
    "reason_summary",
]

STATUS_EXTENDED_COLUMNS = STATUS_COMPAT_COLUMNS + [
    "ivt_risk_score",
    "decision_margin",
    "positive_streak",
    "negative_streak",
    "adjustment_streak",
    "ivt_streak",
    "persistence_score",
    "hysteresis_applied",
    "persistence_label",
    "composite_positive_count",
    "composite_negative_count",
    "ivt_click_stress_score",
    "ivt_serving_score",
    "ivt_attention_score",
    "ivt_counter_score",
    "ivt_funnel_score",
]

EVENT_COMPAT_COLUMNS = [
    "batch_id",
    "run_time",
    "run_date",
    "run_hour",
    "entity_key",
    "site",
    "country_code",
    "country_name",
    "date",
    "mapped_revenue_source",
    "join_status",
    "source_scope",
    "header_name",
    "metric_group",
    "metric_type",
    "funnel_stage",
    "metric_role",
    "rule_key",
    "score_method",
    "expected_direction",
    "prev_value",
    "cur_value",
    "delta_abs",
    "delta_pct",
    "current_increment",
    "baseline_center",
    "baseline_scale",
    "volume_gate_value",
    "volume_gate_pass",
    "denominator_value",
    "confidence",
    "signal_strength",
    "health_component",
    "adjustment_component",
    "change_class",
    "event_label",
    "event_reason",
    "note",
]

EVENT_EXTENDED_COLUMNS = EVENT_COMPAT_COLUMNS + [
    "ivt_component",
    "ivt_capacity",
    "baseline_source",
    "hist_points",
    "z_raw",
    "directional_z",
    "day_type",
    "is_composite",
]

BASELINE_CONFIDENCE = {
    "SITE_SAME_HOUR_DAYTYPE": 1.00,
    "SITE_SAME_HOUR": 0.92,
    "SITE_SAME_HOUR_DAYTYPE_EXT": 0.86,
    "SITE_SAME_HOUR_EXT": 0.80,
    "SITE_ALL_HOURS_DAYTYPE": 0.78,
    "SITE_ALL_HOURS": 0.72,
    "SITE_ALL_HOURS_DAYTYPE_EXT": 0.66,
    "SITE_ALL_HOURS_EXT": 0.60,
    "NO_HISTORY": 0.35,
}


def _sql_date(value: date) -> str:
    return value.isoformat()


def _safe_div(numerator: float, denominator: float, default: float = 0.0) -> float:
    num = safe_float(numerator)
    den = safe_float(denominator)
    if abs(den) < 1e-12:
        return default
    return num / den


def _normalize_rate_value(value: float) -> float:
    x = safe_float(value)
    if abs(x) > 1.5:
        return x / 100.0
    return x


def _day_type_for(value: date | datetime | pd.Timestamp | str | None) -> str:
    if value is None or value == "":
        return "UNKNOWN"
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return "UNKNOWN"
    return "WEEKEND" if int(ts.weekday()) >= 5 else "WEEKDAY"


def _family_rank_factor(rank: int) -> float:
    rank = max(1, int(rank or 1))
    return 1.0 / math.pow(rank, 0.8)


def _coerce_insert_df(table: str, df: pd.DataFrame, columns: list[str]) -> None:
    if df.empty:
        df["is_current_batch"] = False
        return df

    keep = [c for c in columns if c in df.columns]
    insert_df(table, df[keep])


def _counter_drop_magnitude(prev_value: float, increment: float) -> float:
    prev_abs = max(abs(safe_float(prev_value)), 1.0)
    return clip(abs(safe_float(increment)) / prev_abs, 0.0, 1.0)


def _mapped_source_mode(mapped_revenue_source: str | None) -> str | None:
    value = str(mapped_revenue_source or "").strip().lower()
    if not value:
        return None
    has_adsense = "adsense" in value
    has_adx = any(token in value for token in ["adx", "ad exchange", "ad_exchange", "google_adx"])
    if has_adsense and has_adx:
        return SOURCE_MODE_MIXED
    if has_adsense:
        return SOURCE_MODE_ADSENSE_ONLY
    if has_adx:
        return SOURCE_MODE_ADX_ONLY
    return None


def _source_activity_flags(row: pd.Series | dict) -> tuple[bool, bool]:
    adsense_active = any(safe_float(row.get(c)) > 0 for c in ["adsense_estimated_earnings", "adsense_page_views", "adsense_ad_requests", "adsense_impressions"])
    adx_active = any(safe_float(row.get(c)) > 0 for c in ["adx_revenue", "adx_impressions", "adx_total_requests", "adx_responses_served"])
    return adsense_active, adx_active


def _infer_source_mode(row: pd.Series | dict) -> str:
    mapped_mode = _mapped_source_mode(row.get("mapped_revenue_source"))
    adsense_active, adx_active = _source_activity_flags(row)
    if mapped_mode == SOURCE_MODE_MIXED:
        return SOURCE_MODE_MIXED
    if adsense_active and adx_active:
        return SOURCE_MODE_MIXED
    if mapped_mode == SOURCE_MODE_ADSENSE_ONLY and not adx_active:
        return SOURCE_MODE_ADSENSE_ONLY
    if mapped_mode == SOURCE_MODE_ADX_ONLY and not adsense_active:
        return SOURCE_MODE_ADX_ONLY
    if adsense_active:
        return SOURCE_MODE_ADSENSE_ONLY
    if adx_active:
        return SOURCE_MODE_ADX_ONLY
    return mapped_mode or SOURCE_MODE_MIXED


def _get_source_mode(row: pd.Series | dict) -> str:
    value = str(row.get("source_mode", "") or "").strip().upper()
    if value in {SOURCE_MODE_ADSENSE_ONLY, SOURCE_MODE_ADX_ONLY, SOURCE_MODE_MIXED}:
        return value
    return _infer_source_mode(row)


def _blended_metric_allowed(row: pd.Series | dict, column: str) -> bool:
    mode = _get_source_mode(row)
    return column not in BLENDED_DUPLICATE_COLUMNS_BY_SOURCE_MODE.get(mode, set())


# def _source_scope_allowed(row: pd.Series | dict, source_scope: str) -> bool:
#     mode = _get_source_mode(row)
#     return source_scope not in SOURCE_SCOPE_BLOCKLIST_BY_SOURCE_MODE.get(mode, set())

def _source_scope_allowed(row: pd.Series | dict, source_scope: str) -> bool:
    mode = _get_source_mode(row)
    # meta metrics selalu boleh
    if source_scope == "meta":
        return True
    # ADX metrics
    if source_scope == "adx":
        return mode in ("ADX_ONLY", "MIXED")
    # Adsense metrics
    if source_scope == "adsense":
        return mode in ("ADSENSE_ONLY", "MIXED")
    # Blended metrics → hanya kalau dua source aktif
    if source_scope == "blended":
        return mode == "MIXED"
    # unknown scope
    return False


def _filter_history_by_source_mode(samples: pd.DataFrame, row: pd.Series | dict, rule: MetricRule) -> pd.DataFrame:
    if samples.empty or rule.source_scope == "meta" or "source_mode" not in samples.columns:
        return samples
    mode = _get_source_mode(row)
    if mode == SOURCE_MODE_MIXED:
        return samples
    scoped = samples[samples["source_mode"] == mode].copy()
    return scoped if not scoped.empty else samples


def _counter_correction_columns_for_mode(source_mode: str) -> set[str]:
    if source_mode == SOURCE_MODE_ADSENSE_ONLY:
        return {
            c for c in COUNTER_CORRECTION_COLUMNS
            if not c.startswith("adx_") and c != "blended_revenue"
        }
    if source_mode == SOURCE_MODE_ADX_ONLY:
        return {
            c for c in COUNTER_CORRECTION_COLUMNS
            if not c.startswith("adsense_") and c != "blended_revenue"
        }
    return set(COUNTER_CORRECTION_COLUMNS)


def _load_join_history(target_date: date, domain: str, lookback_days: int = 35) -> pd.DataFrame:
    sql = f"""
        SELECT *
        FROM hris_trendHorizone.fact_join_hourly
        WHERE date >= toDate('{_sql_date(target_date)}') - INTERVAL {int(lookback_days)} DAY
        AND date <= toDate('{_sql_date(target_date)}')
    """
    if domain:
        sql += f" AND site = '{domain}'"

    sql += " ORDER BY site, country_code, date, run_hour, run_time"
    df = query_df(sql)
    if df.empty:
        return df
    out = df.copy()
    out["site"] = out["site"].map(normalize_domain)
    out["country_code"] = out["country_code"].map(normalize_country_cd)
    out["country_name"] = [normalize_country_nm(c, n) for c, n in zip(out["country_code"], out["country_name"])]
    out["entity_key"] = out["entity_key"].astype(str)
    out["entity_base_key"] = out["site"] + "|" + out["country_code"]
    out["batch_id"] = out["batch_id"].astype(str)
    out["run_time"] = pd.to_datetime(out["run_time"], utc=True, errors="coerce")
    out["run_date"] = pd.to_datetime(out["run_date"], errors="coerce").dt.date
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.date
    out["day_type"] = out["date"].map(_day_type_for)
    out = out.sort_values(["entity_key", "date", "run_hour", "run_time", "batch_id"]).drop_duplicates(subset=["entity_key", "date", "run_hour"], keep="last")
    for col in RAW_JOIN_COLUMNS + ["revenue_value"]:
        if col in out.columns:
            out[col] = out[col].map(safe_float) if col not in {"batch_id", "run_time", "run_date", "entity_key", "site", "country_code", "country_name", "date", "mapped_revenue_source", "join_status"} else out[col]
        else:
            out[col] = 0.0
    for col in RAW_RATE_COLUMNS:
        out[col] = out[col].map(_normalize_rate_value)
    out = _compute_derived_features(out)
    out = out.sort_values(["entity_key", "date", "run_hour"]).reset_index(drop=True)
    grouped = out.groupby("entity_key", sort=False)
    extra_cols = {}

    for col in REQUIRED_METRIC_COLUMNS:
        prev_series = grouped[col].shift(1)
        extra_cols[f"prev__{col}"] = prev_series
        extra_cols[f"delta__{col}"] = out[col] - prev_series.fillna(0.0)

    out = pd.concat([out, pd.DataFrame(extra_cols, index=out.index)], axis=1).copy()
    out["is_current_batch"] = out["date"].eq(target_date)
    return out


def _compute_derived_features(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    def col(name: str) -> pd.Series:
        if name not in out.columns:
            out[name] = 0.0
        return out[name].astype(float)

    blended_revenue = col("adsense_estimated_earnings") + col("adx_revenue")
    total_clicks = col("adsense_clicks") + col("adx_clicks")
    total_requests = col("adsense_ad_requests") + col("adx_total_requests")
    total_impressions = col("adsense_impressions") + col("adx_impressions")

    out["blended_revenue"] = blended_revenue
    out["total_clicks_blended"] = total_clicks
    out["total_requests_blended"] = total_requests
    out["total_impressions_blended"] = total_impressions

    run_hour_series = pd.to_numeric(out.get("run_hour", 0), errors="coerce")
    if not isinstance(run_hour_series, pd.Series):
        run_hour_series = pd.Series([run_hour_series] * len(out), index=out.index)
    expected_day_progress = ((run_hour_series.fillna(0.0).clip(lower=0.0, upper=23.0) + 1.0) / 24.0).clip(lower=(1.0 / 24.0), upper=1.0)
    out["meta_budget_utilization"] = [
        _safe_div(s, b, 0.0) if safe_float(b) > 0 else 0.0
        for s, b in zip(col("meta_spend"), col("meta_daily_budget"))
    ]
    # out["meta_budget_pacing_index"] = [
    #     _safe_div(util, progress, 0.0) if safe_float(budget) > 0 else 0.0
    #     for util, progress, budget in zip(out["meta_budget_utilization"], expected_day_progress, col("meta_daily_budget"))
    # ]

    out["meta_cost_per_lpv"] = [
        _safe_div(s, lpv, 0.0) for s, lpv in zip(col("meta_spend"), col("meta_lpv"))
    ]
    out["revenue_per_lpv"] = [
        _safe_div(r, lpv, 0.0) for r, lpv in zip(blended_revenue, col("meta_lpv"))
    ]
    out["roi_proxy"] = [
        _safe_div(r - s, s, 0.0) if safe_float(s) > 0 else 0.0
        for r, s in zip(blended_revenue, col("meta_spend"))
    ]

    out["request_to_impression_eff"] = [
        _safe_div(imp, req, 0.0) for imp, req in zip(total_impressions, total_requests)
    ]
    out["revenue_per_request_total"] = [
        _safe_div(r, req, 0.0) for r, req in zip(blended_revenue, total_requests)
    ]
    out["click_pressure"] = [
        _safe_div(clicks, lpv, 0.0) for clicks, lpv in zip(total_clicks, col("meta_lpv"))
    ]
    out["request_density"] = [
        _safe_div(req, lpv, 0.0) for req, lpv in zip(total_requests, col("meta_lpv"))
    ]

    ads_imp = col("adsense_impressions")
    adx_imp = col("adx_impressions")
    ads_view = col("adsense_active_view_viewability")
    adx_view = col("adx_active_view_pct_viewable")
    ads_time = col("adsense_active_view_time")
    adx_time = col("adx_active_view_avg_time_sec")

    blended_attention_quality = []
    blended_active_time = []
    blended_view_gap = []
    for i in range(len(out)):
        w1 = max(ads_imp.iloc[i], 0.0)
        w2 = max(adx_imp.iloc[i], 0.0)
        total_w = w1 + w2
        if total_w > 0:
            blended_attention_quality.append(((ads_view.iloc[i] * w1) + (adx_view.iloc[i] * w2)) / total_w)
            blended_active_time.append(((ads_time.iloc[i] * w1) + (adx_time.iloc[i] * w2)) / total_w)
        else:
            blended_attention_quality.append(weighted_mean([ads_view.iloc[i], adx_view.iloc[i]], [1.0, 1.0]))
            blended_active_time.append(weighted_mean([ads_time.iloc[i], adx_time.iloc[i]], [1.0, 1.0]))
        blended_view_gap.append(abs(ads_view.iloc[i] - adx_view.iloc[i]))

    out["attention_quality_blended"] = blended_attention_quality
    out["active_time_blended"] = blended_active_time
    out["viewability_gap_abs"] = blended_view_gap
    out["source_mode"] = out.apply(_infer_source_mode, axis=1)
    return out


def _load_recent_status_history(target_date: date, lookback_days: int = 7) -> pd.DataFrame:
    sql_candidates = [
        f"""
        SELECT batch_id, run_time, run_date, run_hour, entity_key, site, country_code, date,
               health_score, adjustment_score, ivt_risk_score, confidence,
               final_label, root_cause_label,
               positive_streak, negative_streak, adjustment_streak, ivt_streak
        FROM {STATUS_TABLE}
        WHERE date >= toDate('{_sql_date(target_date)}') - INTERVAL {int(lookback_days)} DAY
          AND date <= toDate('{_sql_date(target_date)}')
        ORDER BY site, country_code, date, run_hour, run_time
        """,
        f"""
        SELECT batch_id, run_time, run_date, run_hour, entity_key, site, country_code, date,
               health_score, adjustment_score, confidence,
               final_label, root_cause_label
        FROM {STATUS_TABLE}
        WHERE date >= toDate('{_sql_date(target_date)}') - INTERVAL {int(lookback_days)} DAY
          AND date <= toDate('{_sql_date(target_date)}')
        ORDER BY site, country_code, date, run_hour, run_time
        """,
    ]
    df = pd.DataFrame()
    for sql in sql_candidates:
        try:
            df = query_df(sql)
            break
        except Exception:
            continue
    if df.empty:
        df["is_current_batch"] = False
        return df

    out = df.copy()
    out["site"] = out["site"].map(normalize_domain)
    out["country_code"] = out["country_code"].map(normalize_country_cd)
    out["entity_base_key"] = out["site"] + "|" + out["country_code"]
    out["run_time"] = pd.to_datetime(out["run_time"], utc=True, errors="coerce")
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.date
    out["run_hour"] = out["run_hour"].map(lambda x: int(safe_float(x)))
    for col in ["health_score", "adjustment_score", "ivt_risk_score", "confidence", "positive_streak", "negative_streak", "adjustment_streak", "ivt_streak"]:
        if col not in out.columns:
            out[col] = 0.0
        out[col] = out[col].map(safe_float)
    out["final_label"] = out.get("final_label", "").astype(str)
    out["root_cause_label"] = out.get("root_cause_label", "").astype(str)
    return out


def _source_has_activity(row: pd.Series, source_scope: str) -> bool:
    if source_scope == "meta":
        return any(safe_float(row.get(c)) > 0 for c in ["meta_spend", "meta_clicks", "meta_lpv", "meta_daily_budget"])
    if source_scope == "adsense":
        adsense_active, _ = _source_activity_flags(row)
        return adsense_active
    if source_scope == "adx":
        _, adx_active = _source_activity_flags(row)
        return adx_active
    if source_scope == "blended":
        return any(safe_float(row.get(c)) > 0 for c in ["meta_lpv", "meta_spend", "adsense_estimated_earnings", "adx_revenue", "total_requests_blended", "total_impressions_blended"])
    return False


def _select_window(samples: pd.DataFrame, current_day: date, rule: MetricRule, current_day_type: str, scope_label: str) -> tuple[pd.DataFrame, str]:
    if samples.empty:
        return samples, "NO_HISTORY"

    tmp = samples.copy()
    tmp["days_ago"] = (pd.to_datetime(current_day) - pd.to_datetime(tmp["date"])) .dt.days
    primary_window = max(7, int(rule.same_hour_lookback_days))
    extended_window = max(primary_window + 14, primary_window * 2)
    tmp = tmp[(tmp["days_ago"] >= 1) & (tmp["days_ago"] <= extended_window)]
    if tmp.empty:
        return tmp, "NO_HISTORY"

    same_day_type = tmp[tmp["day_type"] == current_day_type].copy()
    candidates = [
        (same_day_type[same_day_type["days_ago"] <= primary_window], f"{scope_label}_DAYTYPE"),
        (tmp[tmp["days_ago"] <= primary_window], scope_label),
        (same_day_type, f"{scope_label}_DAYTYPE_EXT"),
        (tmp, f"{scope_label}_EXT"),
    ]
    min_points = max(3, int(rule.min_history_points))
    for part, label in candidates:
        if len(part) >= min_points:
            return part.sort_values(["date", "run_hour", "run_time"]), label
    non_empty = [(part, label) for part, label in candidates if not part.empty]
    if non_empty:
        part, label = max(non_empty, key=lambda x: len(x[0]))
        return part.sort_values(["date", "run_hour", "run_time"]), label
    return tmp, "NO_HISTORY"


def _level_baseline(samples: pd.DataFrame, rule: MetricRule) -> tuple[float, float, int]:
    if samples.empty:
        return 0.0, max(rule.deadband_abs, 1e-6), 0
    values = samples[rule.column].astype(float).tolist()
    weights = samples[rule.denominator_column].astype(float).tolist() if rule.denominator_column and rule.denominator_column in samples.columns else [1.0] * len(values)
    if rule.score_method == "EWMA_LEVEL_Z":
        center = ewma_last(values, alpha=rule.ewma_alpha)
    elif rule.score_method == "PROPORTION_Z":
        center = weighted_mean(values, weights)
    else:
        center = robust_median(values)
    scale = robust_scale(values, center)
    floor = max(abs(center) * rule.deadband_pct, rule.deadband_abs, 1e-6)
    scale = max(scale, floor)
    return center, scale, len(values)


def _counter_baseline(samples: pd.DataFrame, rule: MetricRule) -> tuple[float, float, int]:
    if samples.empty:
        return 0.0, max(rule.deadband_abs, 1e-6), 0
    values = samples[f"delta__{rule.column}"].astype(float).tolist()
    center = robust_median(values)
    scale = robust_scale(values, center)
    floor = max(abs(center) * rule.deadband_pct, rule.deadband_abs, 1e-6)
    scale = max(scale, floor)
    return center, scale, len(values)


def _band_range(samples: pd.DataFrame, rule: MetricRule) -> tuple[float, float, float, float, int]:
    values: list[float] = []
    if not samples.empty and rule.column in samples.columns:
        for value in samples[rule.column].tolist():
            val = safe_float(value)
            if math.isfinite(val):
                values.append(val)

    hist_points = len(values)
    static_low = safe_float(rule.range_low) if rule.range_low is not None else None
    static_high = safe_float(rule.range_high) if rule.range_high is not None else None
    min_half_width = max(rule.deadband_abs, 1e-6)

    if rule.band_dynamic and hist_points >= max(3, rule.min_history_points // 2):
        series = pd.Series(values, dtype=float)
        center = robust_median(values)
        scale = robust_scale(values, center)
        q_low = float(series.quantile(rule.band_quantile_low))
        q_high = float(series.quantile(rule.band_quantile_high))
        min_half_width = max(min_half_width, abs(center) * rule.band_min_width_pct)
        low = min(q_low, center - (rule.band_sigma * scale))
        high = max(q_high, center + (rule.band_sigma * scale))
        if ((high - low) / 2.0) < min_half_width:
            low = center - min_half_width
            high = center + min_half_width
        if static_low is not None:
            low = max(low, static_low)
        if static_high is not None:
            high = min(high, static_high)
        if high <= low:
            if static_low is not None and static_high is not None and static_high > static_low:
                low = static_low
                high = static_high
            else:
                low = center - min_half_width
                high = center + min_half_width
        center = (low + high) / 2.0
        width = max((high - low) / 2.0, min_half_width, 1e-6)
        return low, high, center, width, hist_points

    if static_low is not None and static_high is not None and static_high > static_low:
        center = (static_low + static_high) / 2.0
        width = max((static_high - static_low) / 2.0, abs(center) * rule.band_min_width_pct, min_half_width, 1e-6)
        return static_low, static_high, center, width, hist_points

    center = robust_median(values) if values else 0.0
    width = max(abs(center) * max(rule.band_min_width_pct, rule.deadband_pct), min_half_width, 1e-6)
    low = center - width
    high = center + width
    return low, high, center, width, hist_points


def _band_distance(value: float, low: float, high: float, center: float, width: float) -> float:
    val = safe_float(value)
    band_width = max(width, 1e-6)
    if val < low:
        return (low - val) / band_width
    if val > high:
        return (val - high) / band_width
    return abs(val - center) / band_width * 0.25


def _band_absolute_component(distance: float) -> float:
    dist = max(safe_float(distance), 0.0)
    if dist <= 0.25:
        return 0.18 * (1.0 - (dist / 0.25))
    return -clip((dist - 0.25) / 0.75, 0.0, 1.0)


def _freshness_confidence_factor(cur: pd.Series, rule: MetricRule) -> float:
    if not rule.provisional_metric:
        return 1.0
    ts = pd.to_datetime(cur.get("run_time"), utc=True, errors="coerce")
    if pd.isna(ts):
        return 1.0
    now_utc = pd.Timestamp.now(tz="UTC")
    hours_old = max((now_utc - ts).total_seconds() / 3600.0, 0.0)
    full_hours = max(int(rule.freshness_full_confidence_hours or 0), 1)
    floor = clip(rule.freshness_min_confidence_factor, 0.0, 1.0)
    if hours_old >= full_hours:
        return 1.0
    progress = clip(hours_old / full_hours, 0.0, 1.0)
    return floor + ((1.0 - floor) * progress)


def _event_template(cur: pd.Series, rule: MetricRule, batch_uuid: uuid.UUID) -> dict:
    print(f"current_data: {cur}")
    return {
        "batch_id": str(batch_uuid),
        # ✅ TAMBAHKAN INI
        "family_key": rule.family_key or rule.column,
        "run_time": cur["run_time"].to_pydatetime() if hasattr(cur["run_time"], "to_pydatetime") else cur["run_time"],
        "run_date": cur["run_date"],
        "run_hour": int(cur["run_hour"]),
        "entity_key": cur["entity_key"],
        "site": cur["site"],
        "country_code": cur["country_code"],
        "country_name": cur["country_name"],
        "date": cur["date"],
        "mapped_revenue_source": cur.get("mapped_revenue_source", ""),
        "join_status": cur.get("join_status", ""),
        "source_mode": _get_source_mode(cur),
        "source_scope": rule.source_scope,
        "header_name": rule.column,
        "metric_group": rule.metric_group,
        "metric_type": rule.metric_type,
        "funnel_stage": rule.funnel_stage,
        "metric_role": rule.metric_role,
        "rule_key": rule.rule_key,
        "score_method": rule.score_method,
        "expected_direction": rule.expected_direction,
        "prev_value": safe_float(cur.get(f"prev__{rule.column}")),
        "cur_value": safe_float(cur.get(rule.column)),
        "delta_abs": 0.0,
        "delta_pct": 0.0,
        "current_increment": safe_float(cur.get(f"delta__{rule.column}")),
        "baseline_center": 0.0,
        "baseline_scale": 0.0,
        "baseline_source": "NO_HISTORY",
        "hist_points": 0,
        "volume_gate_value": 0.0,
        "volume_gate_pass": 1,
        "denominator_value": 0.0,
        "confidence": 0.0,
        "z_raw": 0.0,
        "directional_z": 0.0,
        "signal_strength": 0.0,
        "health_component": 0.0,
        "adjustment_component": 0.0,
        "ivt_component": 0.0,
        "ivt_capacity": 0.0,
        "change_class": "SKIPPED",
        "event_label": "",
        "event_reason": "",
        "day_type": cur.get("day_type", "UNKNOWN"),
        "is_composite": 0,
        "note": "",
    }

def _evaluate_rule(cur: pd.Series, same_hour: pd.DataFrame, all_hours: pd.DataFrame, rule: MetricRule, batch_uuid: uuid.UUID) -> dict:
    event = _event_template(cur, rule, batch_uuid)
    cur_value = event["cur_value"]
    prev_value = event["prev_value"]
    raw_prev_value = cur.get(f"prev__{rule.column}")
    prev_is_missing = pd.isna(raw_prev_value)
    event["delta_abs"] = cur_value - prev_value
    event["delta_pct"] = safe_pct_change(cur_value, prev_value)
    if not _source_scope_allowed(cur, rule.source_scope):
        event["event_reason"] = "SOURCE_SCOPE_DISABLED_BY_MODE"
        event["note"] = f"source_mode={event['source_mode']}"
        return event

    if rule.source_scope == "blended" and not _blended_metric_allowed(cur, rule.column):
        event["event_reason"] = "BLENDED_DUPLICATE_DISABLED"
        event["note"] = f"source_mode={event['source_mode']}"
        return event

    if rule.requires_source_match and not _source_has_activity(cur, rule.source_scope):
        event["event_reason"] = "SOURCE_SCOPE_SKIPPED"
        return event

    if rule.requires_join_status_ok and str(cur.get("join_status")) not in SCORABLE_JOIN_STATUSES:
        event["event_reason"] = "JOIN_STATUS_NOT_SCORABLE"
        return event

    if rule.volume_gate_column:
        event["volume_gate_value"] = safe_float(cur.get(rule.volume_gate_column))
        if event["volume_gate_value"] < rule.min_volume:
            event["volume_gate_pass"] = 0
            event["confidence"] = clip(event["volume_gate_value"] / max(rule.min_volume, 1.0), 0.0, 1.0)
            event["event_reason"] = "LOW_VOLUME_GATE"
            return event

    if rule.denominator_column:
        event["denominator_value"] = safe_float(cur.get(rule.denominator_column))

    same_hour_scoped = _filter_history_by_source_mode(same_hour, cur, rule)
    all_hours_scoped = _filter_history_by_source_mode(all_hours, cur, rule)
    selected, baseline_source = _select_window(same_hour_scoped, cur["date"], rule, cur.get("day_type", "UNKNOWN"), "SITE_SAME_HOUR")
    if len(selected) < max(3, rule.min_history_points // 2):
        selected, baseline_source = _select_window(all_hours_scoped, cur["date"], rule, cur.get("day_type", "UNKNOWN"), "SITE_ALL_HOURS")
    print(f"family_rank: {rule.family_rank}")
    family_factor = _family_rank_factor(rule.family_rank)
    print(f"family_factor: {family_factor}")
    join_status = str(cur.get("join_status"))
    join_conf = 1.0 if join_status == "OK" else 0.75 if join_status.startswith("SOURCE_ONLY_NO_META") else 0.25
    print(f"score_method: {rule.score_method}")
    if rule.score_method == "BAND_TARGET":
        low, high, center, width, hist_points = _band_range(selected, rule)
        dist = _band_distance(cur_value, low, high, center, width)
        print(f"dist_data: {dist}")
        prev_dist = dist if prev_is_missing else _band_distance(prev_value, low, high, center, width)
        movement_component = 0.0 if prev_is_missing else clip(prev_dist - dist, -1.0, 1.0)
        print(f"movement_component: {movement_component}")
        absolute_component = _band_absolute_component(dist)
        print(f"absolute_component: {absolute_component}")
        signal_strength = clip((movement_component * 0.65) + (absolute_component * 0.35), -1.0, 1.0)
        print(f"signal_strength: {signal_strength}")
        if dist > 0.25 and signal_strength > 0:
            signal_strength = min(signal_strength, 0.15)

        history_conf = clip(hist_points / max(rule.min_history_points, 1), 0.25, 1.0) if hist_points > 0 else 0.25
        prev_conf = 0.60 if prev_is_missing else 1.0
        baseline_conf = BASELINE_CONFIDENCE.get(baseline_source, 0.35)
        freshness_factor = _freshness_confidence_factor(cur, rule)
        event["confidence"] = clip(((history_conf * 0.35) + (baseline_conf * 0.20) + (prev_conf * 0.25) + (join_conf * 0.20)) * freshness_factor, 0.0, 1.0)

        sign_weight = rule.positive_weight if signal_strength > 0 else rule.negative_weight if signal_strength < 0 else 1.0
        event["baseline_center"] = center
        event["baseline_scale"] = width
        event["baseline_source"] = baseline_source
        event["hist_points"] = hist_points
        event["signal_strength"] = signal_strength
        event["health_component"] = signal_strength * rule.base_weight * sign_weight * family_factor * event["confidence"]
        print(f"ivt_weight: {rule.ivt_weight}")
        event["ivt_capacity"] = rule.ivt_weight * family_factor * event["confidence"]
        event["ivt_component"] = max(0.0, -signal_strength) * event["ivt_capacity"]
        print(f"data_event_full: {event}")
        if signal_strength > 0.02:
            event["change_class"] = "POSITIVE"
            event["event_label"] = rule.label_positive or f"{rule.column.upper()}_IN_RANGE"
        elif signal_strength < -0.02:
            event["change_class"] = "NEGATIVE"
            event["event_label"] = rule.label_negative or f"{rule.column.upper()}_OUT_OF_RANGE"
        else:
            event["change_class"] = "NEUTRAL"

        if dist <= 0.25:
            event["event_reason"] = "IN_TARGET_BAND"
        elif movement_component > 0.10:
            event["event_reason"] = "OUTSIDE_TARGET_BAND_RECOVERING"
        else:
            event["event_reason"] = "OUTSIDE_TARGET_BAND"

        note_parts = [
            f"baseline_source={baseline_source}",
            f"hist_points={hist_points}",
            f"band=({low:.4f},{high:.4f})",
            f"family_rank={rule.family_rank}",
            f"source_mode={event['source_mode']}",
        ]
        if freshness_factor < 0.999:
            note_parts.append(f"freshness_factor={freshness_factor:.3f}")
        event["note"] = ";".join(note_parts)
        return event

    if rule.score_method == "HOURLY_INCREMENT_Z":
        print(f"[DEBUG] Raw rule: {rule}")
        center, scale, hist_points = _counter_baseline(selected, rule)
        raw_delta = event["current_increment"] - center
        deadband = max(abs(center) * rule.deadband_pct, rule.deadband_abs, 1e-6)
        if abs(raw_delta) <= deadband:
            raw_delta = 0.0
        z_raw = raw_delta / max(scale, 1e-6)
        directional_z = apply_direction(z_raw, rule.expected_direction)
        signal_strength = clip(directional_z / max(rule.sigma_clip, 1.0), -1.0, 1.0)
        event["event_reason"] = "COUNTER_DECREASE" if event["current_increment"] < 0 else "INCREMENT_SHIFT"
    elif rule.score_method in {"EWMA_LEVEL_Z", "PROPORTION_Z"}:
        center, scale, hist_points = _level_baseline(selected, rule)
        diff = cur_value - center
        deadband = max(abs(center) * rule.deadband_pct, rule.deadband_abs, 1e-6)
        if abs(diff) <= deadband:
            diff = 0.0
        if rule.score_method == "PROPORTION_Z":
            denom = max(event["denominator_value"], 1.0)
            p = clip(center, 1e-6, 1 - 1e-6)
            se = math.sqrt(max(p * (1 - p), 1e-6) / denom)
            z_raw = diff / max(se, scale, 1e-6)
            event["event_reason"] = "PROPORTION_SHIFT"
        else:
            z_raw = diff / max(scale, 1e-6)
            event["event_reason"] = "LEVEL_SHIFT"
        directional_z = apply_direction(z_raw, rule.expected_direction)
        signal_strength = clip(directional_z / max(rule.sigma_clip, 1.0), -1.0, 1.0)
    else:
        event["event_reason"] = "UNSUPPORTED_RULE"
        return event

    history_conf = clip(hist_points / max(rule.min_history_points, 1), 0.25, 1.0)
    volume_conf = 1.0 if event["volume_gate_value"] == 0 else clip(event["volume_gate_value"] / max(rule.min_volume or event["volume_gate_value"], 1.0), 0.0, 1.0)
    baseline_conf = BASELINE_CONFIDENCE.get(baseline_source, 0.35)
    freshness_factor = _freshness_confidence_factor(cur, rule)
    event["confidence"] = clip(((history_conf * 0.40) + (baseline_conf * 0.20) + (volume_conf * 0.20) + (join_conf * 0.20)) * freshness_factor, 0.0, 1.0)

    sign_weight = rule.positive_weight if signal_strength > 0 else rule.negative_weight if signal_strength < 0 else 1.0
    event["baseline_center"] = center
    event["baseline_scale"] = scale
    event["baseline_source"] = baseline_source
    event["hist_points"] = hist_points
    event["z_raw"] = z_raw
    event["directional_z"] = directional_z
    event["signal_strength"] = signal_strength
    event["health_component"] = signal_strength * rule.base_weight * sign_weight * event["confidence"] * family_factor
    event["ivt_capacity"] = rule.ivt_weight * event["confidence"] * family_factor
    event["ivt_component"] = max(0.0, -signal_strength) * event["ivt_capacity"]

    # if rule.adjustment_weight > 0 and rule.column in COUNTER_CORRECTION_COLUMNS and event["current_increment"] < 0 and safe_float(prev_value) > 0:
    # if (
    #     rule.adjustment_weight > 0
    #     and rule.column in COUNTER_CORRECTION_COLUMNS
    #     and event["current_increment"] < 0
    #     and safe_float(prev_value) > 0
    #     and _source_scope_allowed(cur, rule.source_scope)
    #     and (
    #         not rule.requires_source_match
    #         or _source_has_activity(cur, rule.source_scope)
    #     )
    # ):
    if (
        rule.adjustment_weight > 0
        and rule.column in COUNTER_CORRECTION_COLUMNS
        and event["change_class"] == "NEGATIVE"   # ⭐ WAJIB
        and event["event_reason"] not in {
            "WITHIN_DEADBAND",
            "LOW_VOLUME_GATE",
            "SOURCE_SCOPE_SKIPPED",
        }
        and event["current_increment"] < 0
        and safe_float(prev_value) > 0
    ):
        magnitude = max(abs(signal_strength), _counter_drop_magnitude(prev_value, event["current_increment"]))
        event["adjustment_component"] = -magnitude * rule.adjustment_weight * event["confidence"] * family_factor
        event["adjustment_capacity"] = (
            rule.adjustment_weight
            * event["confidence"]
            * family_factor
        )
        event["ivt_component"] += magnitude * 0.55 * event["ivt_capacity"]

    if signal_strength > 0.02:
        event["change_class"] = "POSITIVE"
        event["event_label"] = rule.label_positive or f"{rule.column.upper()}_UP"
    elif signal_strength < -0.02:
        event["change_class"] = "NEGATIVE"
        event["event_label"] = rule.label_negative or f"{rule.column.upper()}_DOWN"
    else:
        event["change_class"] = "NEUTRAL"
        event["event_reason"] = "WITHIN_DEADBAND"

    note_parts = [
        f"baseline_source={baseline_source}",
        f"hist_points={hist_points}",
        f"family_rank={rule.family_rank}",
        f"source_mode={event['source_mode']}",
    ]
    if freshness_factor < 0.999:
        note_parts.append(f"freshness_factor={freshness_factor:.3f}")
    event["note"] = ";".join(note_parts)
    return event


def _signal_lookup(events: list[dict], header_name: str) -> dict:
    hits = [e for e in events if e["header_name"] == header_name and e["change_class"] != "SKIPPED"]
    if not hits:
        return {"signal_strength": 0.0, "confidence": 0.0, "cur_value": 0.0, "delta_abs": 0.0, "ivt_component": 0.0}
    best = max(hits, key=lambda e: abs(safe_float(e.get("signal_strength"))))
    return best


def _composite_event_template(cur: pd.Series, batch_uuid: uuid.UUID, header_name: str, source_scope: str, metric_group: str, funnel_stage: str, metric_role: str, expected_direction: str) -> dict:
    return {
        "batch_id": str(batch_uuid),
        "run_time": cur["run_time"].to_pydatetime() if hasattr(cur["run_time"], "to_pydatetime") else cur["run_time"],
        "run_date": cur["run_date"],
        "run_hour": int(cur["run_hour"]),
        "entity_key": cur["entity_key"],
        "site": cur["site"],
        "country_code": cur["country_code"],
        "country_name": cur["country_name"],
        "date": cur["date"],
        "mapped_revenue_source": cur.get("mapped_revenue_source", ""),
        "join_status": cur.get("join_status", ""),
        "source_mode": _get_source_mode(cur),
        "source_scope": source_scope,
        "header_name": header_name,
        "metric_group": metric_group,
        "metric_type": "composite",
        "funnel_stage": funnel_stage,
        "metric_role": metric_role,
        "rule_key": f"composite:{header_name}",
        "score_method": "COMPOSITE_RULE",
        "expected_direction": expected_direction,
        "prev_value": 0.0,
        "cur_value": 0.0,
        "delta_abs": 0.0,
        "delta_pct": 0.0,
        "current_increment": 0.0,
        "baseline_center": 0.0,
        "baseline_scale": 0.0,
        "baseline_source": "COMPOSITE",
        "hist_points": 0,
        "volume_gate_value": 0.0,
        "volume_gate_pass": 1,
        "denominator_value": 0.0,
        "confidence": 0.0,
        "z_raw": 0.0,
        "directional_z": 0.0,
        "signal_strength": 0.0,
        "health_component": 0.0,
        "adjustment_component": 0.0,
        "ivt_component": 0.0,
        "ivt_capacity": 0.0,
        "change_class": "SKIPPED",
        "event_label": "",
        "event_reason": "",
        "day_type": cur.get("day_type", "UNKNOWN"),
        "is_composite": 1,
        "note": "",
    }


def _avg_conf(*events: dict) -> float:
    vals = [safe_float(e.get("confidence")) for e in events if e]
    return clip(sum(vals) / max(len(vals), 1), 0.0, 1.0)


def _evaluate_composite_events(cur: pd.Series, row_events: list[dict], batch_uuid: uuid.UUID) -> list[dict]:
    out: list[dict] = []
    source_mode = _get_source_mode(cur)
    composite_source_scope = "blended"
    if source_mode == SOURCE_MODE_ADSENSE_ONLY:
        composite_source_scope = "adsense"
    elif source_mode == SOURCE_MODE_ADX_ONLY:
        composite_source_scope = "adx"

    lpv = _signal_lookup(row_events, "meta_lpv")
    lpv_rate = _signal_lookup(row_events, "meta_lpv_rate")
    freq = _signal_lookup(row_events, "meta_frequency")
    # budget_pacing = _signal_lookup(row_events, "meta_budget_pacing_index")
    rev_per_lpv = _signal_lookup(row_events, "revenue_per_lpv")
    roi_proxy = _signal_lookup(row_events, "roi_proxy")
    blended_rev = _signal_lookup(row_events, "blended_revenue")
    ads_rev = _signal_lookup(row_events, "adsense_estimated_earnings")
    adx_rev = _signal_lookup(row_events, "adx_revenue")
    ads_req = _signal_lookup(row_events, "adsense_ad_requests")
    adx_req = _signal_lookup(row_events, "adx_total_requests")
    coverage = _signal_lookup(row_events, "adsense_ad_requests_coverage")
    match_rate = _signal_lookup(row_events, "adx_match_rate")
    fill_rate = _signal_lookup(row_events, "adx_total_fill_rate")
    req_eff = _signal_lookup(row_events, "request_to_impression_eff")
    ads_clicks = _signal_lookup(row_events, "adsense_clicks")
    adx_clicks = _signal_lookup(row_events, "adx_clicks")
    ads_cpc = _signal_lookup(row_events, "adsense_cost_per_click")
    adx_cpc = _signal_lookup(row_events, "adx_cpc")
    adx_ecpm = _signal_lookup(row_events, "adx_avg_ecpm")
    ads_rpm = _signal_lookup(row_events, "adsense_page_views_rpm")
    att_quality = _signal_lookup(row_events, "attention_quality_blended")
    att_time = _signal_lookup(row_events, "active_time_blended")
    ads_view = _signal_lookup(row_events, "adsense_active_view_viewability")
    ads_time = _signal_lookup(row_events, "adsense_active_view_time")
    adx_view = _signal_lookup(row_events, "adx_active_view_pct_viewable")
    adx_time = _signal_lookup(row_events, "adx_active_view_avg_time_sec")
    click_pressure = _signal_lookup(row_events, "click_pressure")
    request_density = _signal_lookup(row_events, "request_density")

    def active_events(items: list[dict]) -> list[dict]:
        return [
            e for e in items
            if e and (
                safe_float(e.get("confidence")) > 0
                or abs(safe_float(e.get("signal_strength"))) > 0
                or abs(safe_float(e.get("cur_value"))) > 0
            )
        ]

    def signal_values(items: list[dict]) -> list[float]:
        return [safe_float(e.get("signal_strength")) for e in active_events(items)]

    def avg_conf_items(items: list[dict]) -> float:
        return _avg_conf(*active_events(items))

    def mode_note(note: str) -> str:
        note = str(note or "").strip()
        prefix = f"source_mode={source_mode}"
        return prefix if not note else f"{prefix};{note}"

    if source_mode == SOURCE_MODE_ADSENSE_ONLY:
        request_events = [ads_req]
        click_events = [ads_clicks]
        yield_events = [ads_cpc, ads_rpm, rev_per_lpv]
        delivery_events = [coverage, req_eff]
        attention_events = [ads_view, ads_time]
        primary_revenue = ads_rev
        quality_view_metric = ads_view
        quality_time_metric = ads_time
    elif source_mode == SOURCE_MODE_ADX_ONLY:
        request_events = [adx_req]
        click_events = [adx_clicks]
        yield_events = [adx_cpc, adx_ecpm, rev_per_lpv]
        delivery_events = [match_rate, fill_rate]
        attention_events = [adx_view, adx_time]
        primary_revenue = adx_rev
        quality_view_metric = adx_view
        quality_time_metric = adx_time
    else:
        request_events = [ads_req, adx_req]
        click_events = [ads_clicks, adx_clicks]
        yield_events = [ads_cpc, adx_cpc, adx_ecpm, ads_rpm, rev_per_lpv]
        delivery_events = [coverage, match_rate, fill_rate, req_eff]
        attention_events = [att_quality, att_time]
        primary_revenue = blended_rev
        quality_view_metric = att_quality
        quality_time_metric = att_time

    req_values = signal_values(request_events)
    click_values = signal_values(click_events)
    yield_values = signal_values(yield_events)
    delivery_values = signal_values(delivery_events)
    attention_values = signal_values(attention_events)

    req_signal = max(req_values) if req_values else 0.0
    click_signal = max(click_values) if click_values else 0.0
    yield_floor = min(yield_values) if yield_values else 0.0
    delivery_floor = min(delivery_values) if delivery_values else 0.0
    attention_floor = min(attention_values) if attention_values else 0.0

    def push(evt: dict) -> None:
        if evt:
            evt["source_mode"] = source_mode
            evt["note"] = mode_note(evt.get("note", ""))
            out.append(evt)

    if safe_float(lpv["signal_strength"]) > 0.25 and min(safe_float(rev_per_lpv["signal_strength"]), safe_float(roi_proxy["signal_strength"])) < -0.20:
        conf = avg_conf_items([lpv, rev_per_lpv, roi_proxy])
        evt = _composite_event_template(cur, batch_uuid, "cx_traffic_without_value", composite_source_scope, "ivt_funnel", "funnel_divergence", "relationship", "DOWN_GOOD")
        evt["confidence"] = conf
        evt["signal_strength"] = -clip((safe_float(lpv["signal_strength"]) + abs(min(safe_float(rev_per_lpv["signal_strength"]), safe_float(roi_proxy["signal_strength"])))) / 2.0, 0.0, 1.0)
        evt["health_component"] = -1.10 * conf
        evt["ivt_component"] = 1.05 * conf
        evt["ivt_capacity"] = 1.10 * conf
        evt["change_class"] = "NEGATIVE"
        evt["event_label"] = "TRAFFIC_WITHOUT_VALUE"
        evt["event_reason"] = "LPV_UP_BUT_VALUE_DOWN"
        evt["note"] = "lpv strong positive while rev_per_lpv / roi_proxy weakening"
        push(evt)

    if click_signal > 0.30 and yield_floor < -0.25:
        conf = avg_conf_items(click_events + yield_events)
        evt = _composite_event_template(cur, batch_uuid, "cx_click_spike_without_yield", composite_source_scope, "ivt_click_stress", "click_pressure", "relationship", "DOWN_GOOD")
        evt["confidence"] = conf
        evt["signal_strength"] = -clip((click_signal + abs(yield_floor)) / 2.0, 0.0, 1.0)
        evt["health_component"] = -0.95 * conf
        evt["ivt_component"] = 1.25 * conf
        evt["ivt_capacity"] = 1.30 * conf
        evt["change_class"] = "NEGATIVE"
        evt["event_label"] = "CLICK_SPIKE_WITHOUT_YIELD"
        evt["event_reason"] = "CLICKS_UP_BUT_YIELD_DOWN"
        evt["note"] = "traffic clicks are rising faster than monetization quality"
        push(evt)

    if req_signal > 0.15 and delivery_floor < -0.20:
        conf = avg_conf_items(request_events + delivery_events)
        evt = _composite_event_template(cur, batch_uuid, "cx_serving_suppression", composite_source_scope, "ivt_serving", "serving_health", "relationship", "DOWN_GOOD")
        evt["confidence"] = conf
        evt["signal_strength"] = -clip((req_signal + abs(delivery_floor)) / 2.0, 0.0, 1.0)
        evt["health_component"] = -1.15 * conf
        evt["ivt_component"] = 1.35 * conf
        evt["ivt_capacity"] = 1.35 * conf
        evt["change_class"] = "NEGATIVE"
        evt["event_label"] = "SERVING_SUPPRESSION"
        evt["event_reason"] = "REQUESTS_UP_BUT_MATCH_FILL_DOWN"
        evt["note"] = "inventory demand rose while serving efficiency weakened"
        push(evt)

    if attention_floor < -0.22:
        conf = avg_conf_items(attention_events)
        evt = _composite_event_template(cur, batch_uuid, "cx_attention_collapse", composite_source_scope, "ivt_attention", "attention", "relationship", "DOWN_GOOD")
        evt["confidence"] = conf
        evt["signal_strength"] = -clip(abs(attention_floor), 0.0, 1.0)
        evt["health_component"] = -0.75 * conf
        evt["ivt_component"] = 0.95 * conf
        evt["ivt_capacity"] = 1.00 * conf
        evt["change_class"] = "NEGATIVE"
        evt["event_label"] = "ATTENTION_COLLAPSE"
        evt["event_reason"] = "VIEWABILITY_AND_ACTIVE_TIME_DOWN"
        evt["note"] = "user attention weakened across viewability/time indicators"
        push(evt)

    correction_details = []
    correction_severity = 0.0
    for col in sorted(_counter_correction_columns_for_mode(source_mode)):
        if col in BLENDED_DUPLICATE_COLUMNS_BY_SOURCE_MODE.get(source_mode, set()):
            continue
        prev_value = safe_float(cur.get(f"prev__{col}"))
        current_increment = safe_float(cur.get(f"delta__{col}"))
        if prev_value > 0 and current_increment < 0:
            magnitude = _counter_drop_magnitude(prev_value, current_increment)
            if magnitude >= 0.01:
                correction_details.append(f"{col}:{current_increment:.4f}")
                correction_severity += magnitude
    if correction_details:
        conf = 0.95
        sev = clip(correction_severity / max(len(correction_details), 1), 0.0, 1.0)
        evt = _composite_event_template(cur, batch_uuid, "cx_counter_correction_cluster", composite_source_scope, "ivt_counter", "counter_adjustment", "relationship", "DOWN_GOOD")
        evt["confidence"] = conf
        evt["signal_strength"] = -sev
        evt["health_component"] = -1.20 * sev * conf
        evt["adjustment_component"] = -1.55 * sev * conf
        evt["ivt_component"] = 1.35 * sev * conf
        evt["ivt_capacity"] = 1.35 * conf
        evt["change_class"] = "NEGATIVE"
        evt["event_label"] = "COUNTER_CORRECTION_CLUSTER"
        evt["event_reason"] = "COUNTERS_MOVED_BACKWARD"
        evt["note"] = ",".join(correction_details[:8])
        push(evt)

    if safe_float(freq["signal_strength"]) < -0.15 and safe_float(lpv_rate["signal_strength"]) < -0.15 and min(safe_float(rev_per_lpv["signal_strength"]), safe_float(roi_proxy["signal_strength"])) < -0.15:
        conf = avg_conf_items([freq, lpv_rate, rev_per_lpv, roi_proxy])
        evt = _composite_event_template(cur, batch_uuid, "cx_saturation_funnel_divergence", composite_source_scope, "ivt_funnel", "fatigue", "relationship", "DOWN_GOOD")
        evt["confidence"] = conf
        evt["signal_strength"] = -clip((abs(safe_float(freq["signal_strength"])) + abs(safe_float(lpv_rate["signal_strength"])) + abs(min(safe_float(rev_per_lpv["signal_strength"]), safe_float(roi_proxy["signal_strength"])))) / 3.0, 0.0, 1.0)
        evt["health_component"] = -0.85 * conf
        evt["ivt_component"] = 0.85 * conf
        evt["ivt_capacity"] = 0.90 * conf
        evt["change_class"] = "NEGATIVE"
        evt["event_label"] = "SATURATION_FUNNEL_DIVERGENCE"
        evt["event_reason"] = "FREQUENCY_UP_AND_FUNNEL_DOWN"
        evt["note"] = "fatigue / saturation signature detected"
        push(evt)

    # if safe_float(budget_pacing["signal_strength"]) < -0.18 and min(safe_float(rev_per_lpv["signal_strength"]), safe_float(roi_proxy["signal_strength"])) < -0.15:
    #     conf = avg_conf_items([budget_pacing, rev_per_lpv, roi_proxy])
    #     evt = _composite_event_template(cur, batch_uuid, "cx_budget_pressure_without_value", composite_source_scope, "efficiency", "budget_pacing", "relationship", "DOWN_GOOD")
    #     evt["confidence"] = conf
    #     evt["signal_strength"] = -clip((abs(safe_float(budget_pacing["signal_strength"])) + abs(min(safe_float(rev_per_lpv["signal_strength"]), safe_float(roi_proxy["signal_strength"])))) / 2.0, 0.0, 1.0)
    #     evt["health_component"] = -0.80 * abs(evt["signal_strength"]) * conf
    #     evt["ivt_component"] = 0.35 * abs(evt["signal_strength"]) * conf
    #     evt["ivt_capacity"] = 0.45 * conf
    #     evt["change_class"] = "NEGATIVE"
    #     evt["event_label"] = "BUDGET_PRESSURE_WITHOUT_VALUE"
    #     evt["event_reason"] = "PACING_HOT_AND_VALUE_DOWN"
    #     evt["note"] = "budget pacing moved out of band while unit economics weakened"
    #     push(evt)

    if safe_float(primary_revenue["signal_strength"]) > 0.25 and safe_float(rev_per_lpv["signal_strength"]) > 0.15 and delivery_floor > -0.05:
        conf = avg_conf_items([primary_revenue, rev_per_lpv] + delivery_events)
        evt = _composite_event_template(cur, batch_uuid, "cx_monetization_aligned_growth", composite_source_scope, "revenue", "monetization", "relationship", "UP_GOOD")
        evt["confidence"] = conf
        evt["signal_strength"] = clip((safe_float(primary_revenue["signal_strength"]) + safe_float(rev_per_lpv["signal_strength"]) + max(delivery_floor, 0.0)) / 3.0, 0.0, 1.0)
        evt["health_component"] = 1.20 * evt["signal_strength"] * conf
        evt["ivt_component"] = 0.0
        evt["ivt_capacity"] = 0.35 * conf
        evt["change_class"] = "POSITIVE"
        evt["event_label"] = "MONETIZATION_ALIGNED_GROWTH"
        evt["event_reason"] = "REVENUE_AND_VALUE_GROWING"
        evt["note"] = "revenue, yield, and delivery are aligned"
        push(evt)

    if safe_float(quality_view_metric["signal_strength"]) > 0.20 and safe_float(quality_time_metric["signal_strength"]) > 0.20 and safe_float(click_pressure["signal_strength"]) >= -0.10:
        conf = avg_conf_items([quality_view_metric, quality_time_metric, click_pressure, request_density])
        evt = _composite_event_template(cur, batch_uuid, "cx_quality_recovery", composite_source_scope, "quality", "attention", "relationship", "UP_GOOD")
        evt["confidence"] = conf
        evt["signal_strength"] = clip((safe_float(quality_view_metric["signal_strength"]) + safe_float(quality_time_metric["signal_strength"])) / 2.0, 0.0, 1.0)
        evt["health_component"] = 0.60 * evt["signal_strength"] * conf
        evt["ivt_component"] = 0.0
        evt["ivt_capacity"] = 0.25 * conf
        evt["change_class"] = "POSITIVE"
        evt["event_label"] = "QUALITY_RECOVERY"
        evt["event_reason"] = "ATTENTION_METRICS_RECOVERING"
        evt["note"] = "viewability and active time improved together"
        push(evt)

    return out


def _rolling_streak(records: list[dict], predicate) -> int:
    n = 0
    for rec in records:
        if predicate(rec):
            n += 1
        else:
            break
    return n


def _compute_persistence(cur: pd.Series, recent_status_df: pd.DataFrame) -> dict:
    if recent_status_df.empty:
        return {
            "positive_streak": 0,
            "negative_streak": 0,
            "adjustment_streak": 0,
            "ivt_streak": 0,
            "persistence_score": 0.0,
            "label": "NONE",
        }

    base_key = cur["entity_base_key"]
    same_entity = recent_status_df[recent_status_df["entity_base_key"] == base_key].copy()
    if same_entity.empty:
        return {
            "positive_streak": 0,
            "negative_streak": 0,
            "adjustment_streak": 0,
            "ivt_streak": 0,
            "persistence_score": 0.0,
            "label": "NONE",
        }

    current_ts = pd.to_datetime(cur["run_time"], utc=True, errors="coerce")
    same_entity = same_entity[(same_entity["run_time"] < current_ts) | (same_entity["date"] < cur["date"])]
    same_hour = same_entity[same_entity["run_hour"] == int(cur["run_hour"])]
    recent = same_hour.sort_values(["date", "run_hour", "run_time"], ascending=False).head(4)
    if recent.empty:
        recent = same_entity.sort_values(["date", "run_hour", "run_time"], ascending=False).head(4)

    records = recent.to_dict("records")
    positive_streak = _rolling_streak(records, lambda r: safe_float(r.get("health_score")) >= 15 or str(r.get("final_label", "")) in POSITIVE_ROOT_LABELS)
    negative_streak = _rolling_streak(records, lambda r: safe_float(r.get("health_score")) <= -15 or str(r.get("final_label", "")) in NEGATIVE_ROOT_LABELS)
    adjustment_streak = _rolling_streak(records, lambda r: safe_float(r.get("adjustment_score")) <= -25 or str(r.get("root_cause_label", "")) == "NEG_ADJUSTMENT")
    ivt_streak = _rolling_streak(records, lambda r: safe_float(r.get("ivt_risk_score")) >= 55 or str(r.get("final_label", "")).startswith("RED_FLAG") or str(r.get("root_cause_label", "")) == "NEG_ADJUSTMENT")

    persistence_score = clip((positive_streak * 16.0) - (negative_streak * 16.0) - (adjustment_streak * 12.0) - (ivt_streak * 18.0), -100.0, 100.0)
    if ivt_streak >= 2:
        label = "IVT_STREAK"
    elif adjustment_streak >= 2:
        label = "ADJUSTMENT_STREAK"
    elif negative_streak >= 2:
        label = "NEGATIVE_STREAK"
    elif positive_streak >= 2:
        label = "POSITIVE_STREAK"
    else:
        label = "NONE"

    return {
        "positive_streak": int(positive_streak),
        "negative_streak": int(negative_streak),
        "adjustment_streak": int(adjustment_streak),
        "ivt_streak": int(ivt_streak),
        "persistence_score": float(persistence_score),
        "label": label,
    }


def _group_health_score(events: list[dict], group_name: str) -> float:
    scoped = [e for e in events if e["metric_group"] == group_name and e["change_class"] in {"POSITIVE", "NEGATIVE", "NEUTRAL"}]
    if not scoped:
        return 0.0
    weights = [max(abs(safe_float(e["health_component"])), 1e-9) for e in scoped]
    total = sum(weights)
    return 100.0 * sum(safe_float(e["health_component"]) for e in scoped) / total if total > 0 else 0.0


def _group_risk_score(events: list[dict], group_name: str) -> float:
    scoped = [e for e in events if e["metric_group"] == group_name and safe_float(e.get("ivt_capacity")) > 0]
    if not scoped:
        return 0.0
    numerator = sum(max(0.0, safe_float(e.get("ivt_component"))) for e in scoped)
    denominator = sum(max(safe_float(e.get("ivt_capacity")), 1e-9) for e in scoped)
    return clip(100.0 * numerator / denominator, 0.0, 100.0)


def _root_cause_label(status: dict) -> str:
    if status["join_status"] not in SCORABLE_JOIN_STATUSES and status["negative_signal_count"] == 0 and status["positive_signal_count"] == 0:
        return "DATA_INCOMPLETE"
    if status["ivt_risk_score"] >= 70:
        ivt_groups = {
            "RED_FLAG_CLICK_STRESS": status["ivt_click_stress_score"],
            "RED_FLAG_SERVING_SUPPRESSION": status["ivt_serving_score"],
            "RED_FLAG_ATTENTION_COLLAPSE": status["ivt_attention_score"],
            "RED_FLAG_COUNTER_CORRECTION": status["ivt_counter_score"],
            "RED_FLAG_FUNNEL_DIVERGENCE": status["ivt_funnel_score"],
        }
        label, value = max(ivt_groups.items(), key=lambda x: x[1])
        return label if value >= 45 else "RED_FLAG_IVT"
    if status["adjustment_drop_count"] >= 2 and status["adjustment_score"] <= -35:
        return "NEG_ADJUSTMENT"
    if status["health_score"] >= 18 and status["ivt_risk_score"] < 35 and status["positive_signal_count"] >= status["negative_signal_count"]:
        return "POSITIVE_EXPANSION"
    if status["health_score"] >= 10 and status["ivt_risk_score"] < 45 and status["positive_signal_count"] > status["negative_signal_count"]:
        return "POSITIVE_RECOVERY"
    if status["health_score"] <= -18:
        groups = {
            "TRAFFIC_DROP": status["traffic_score"],
            "SERVING_DROP": status["delivery_score"],
            "YIELD_DROP": status["yield_score"],
            "VIEWABILITY_DROP": status["quality_score"],
            "EFFICIENCY_DROP": status["efficiency_score"],
            "REVENUE_DROP": status["revenue_score"],
        }
        label, value = min(groups.items(), key=lambda x: x[1])
        return label if value <= -8 else "NEGATIVE_MIXED"
    return "STABLE"


def _apply_hysteresis(root_label: str, status: dict, persistence: dict) -> tuple[str, int]:
    final_label = root_label
    hysteresis_applied = 0

    if status["ivt_risk_score"] >= 70 or (status["ivt_risk_score"] >= 55 and persistence["ivt_streak"] >= 2):
        if not final_label.startswith("RED_FLAG"):
            final_label = "RED_FLAG_IVT"
            hysteresis_applied = 1
        return final_label, hysteresis_applied

    if root_label == "POSITIVE_EXPANSION" and persistence["negative_streak"] >= 2 and status["health_score"] < 30:
        return "WATCH_RECOVERY", 1
    if root_label == "POSITIVE_RECOVERY" and persistence["negative_streak"] >= 2 and status["health_score"] < 18:
        return "WATCH_RECOVERY", 1
    if root_label in NEGATIVE_ROOT_LABELS and persistence["positive_streak"] >= 2 and status["health_score"] > -12 and status["ivt_risk_score"] < 55:
        return "WATCH_DECAY", 1
    if root_label == "STABLE" and persistence["negative_streak"] >= 2 and status["health_score"] < 8:
        return "WATCH_NEGATIVE", 1
    if root_label == "STABLE" and persistence["positive_streak"] >= 2 and status["health_score"] > 8 and status["ivt_risk_score"] < 45:
        return "WATCH_POSITIVE", 1
    return final_label, hysteresis_applied

def _clamp_score(x):
    return max(0.0, min(100.0, float(x)))

def _summarize_current_row(cur: pd.Series, events: list[dict], batch_uuid: uuid.UUID, persistence: dict) -> dict:
    positive = [e for e in events if e["change_class"] == "POSITIVE"]
    negative = [e for e in events if e["change_class"] == "NEGATIVE"]
    neutral = [e for e in events if e["change_class"] == "NEUTRAL"]
    skipped = [e for e in events if e["change_class"] == "SKIPPED"]
    composite_positive = [e for e in positive if int(safe_float(e.get("is_composite"))) == 1]
    composite_negative = [e for e in negative if int(safe_float(e.get("is_composite"))) == 1]

    # adjustment_num = sum(safe_float(e["adjustment_component"]) for e in events)
    # adjustment_den = sum(abs(safe_float(e["adjustment_component"])) for e in events if abs(safe_float(e["adjustment_component"])) > 0)
    adjustment_num = sum(
        safe_float(e.get("adjustment_component", 0))
        for e in events
        if e["change_class"] != "SKIPPED"
    )

    adjustment_den = sum(
        safe_float(e.get("adjustment_capacity", 0))
        for e in events
        if e["change_class"] != "SKIPPED"
    )
    confidence = sum(safe_float(e["confidence"]) for e in events if e["change_class"] != "SKIPPED") / max(len([e for e in events if e["change_class"] != "SKIPPED"]), 1)
    family_scores = defaultdict(list)  
    for e in events:
        # family_scores[e["family_key"]].append(e)
        family_key = e.get("family_key") or e.get("rule_key") or "unknown"
        family_scores[family_key].append(e)
    family_health = {}
    for family, evts in family_scores.items():
        num = sum(e["health_component"] for e in evts)
        den = sum(abs(e["health_component"]) for e in evts)
        family_health[family] = num / den if den > 0 else 0
    if not family_health:
        health_score = persistence.get("health_score", 50.0)
    else:
        health_score = 100 * sum(family_health.values()) / len(family_health)
    health_score = 100 * sum(family_health.values()) / max(len(family_health), 1)
    # ⭐ TAMBAHKAN
    health_score = max(0.0, min(100.0, health_score))
    prev_health = persistence.get("health_score", health_score)
    prev_health = persistence.get("health_score", 50.0)
    health_score = 0.7 * prev_health + 0.3 * health_score
    # ⭐ WAJIB ADA
    health_score = max(0.0, min(100.0, health_score))
    health_score = _clamp_score(health_score)
    # health_num = sum(safe_float(e["health_component"]) for e in events)
    # health_den = sum(max(abs(safe_float(e["health_component"])), 0.0) for e in events if e["change_class"] in {"POSITIVE", "NEGATIVE", "NEUTRAL"})
    # health_score = 100.0 * health_num / health_den if health_den > 0 else 0.0
    adjustment_score = (
        100.0 * adjustment_num / adjustment_den
        if adjustment_den > 0
        else 0.0
    ) if adjustment_den > 0 else 0.0
    ivt_num = sum(max(0.0, safe_float(e.get("ivt_component"))) for e in events)
    ivt_den = sum(max(safe_float(e.get("ivt_capacity")), 0.0) for e in events if safe_float(e.get("ivt_capacity")) > 0)
    # print(f"events_data: {events}")
    # for e in events:
    #     print({
    #         "metric": e.get("metric_name"),
    #         "ivt_component": e.get("ivt_component"),
    #         "ivt_capacity": e.get("ivt_capacity"),
    #     })
    ivt_risk_score = clip(100.0 * ivt_num / ivt_den, 0.0, 100.0) if ivt_den > 0 else 0.0
    status = {
        "batch_id": str(batch_uuid),
        "run_time": cur["run_time"].to_pydatetime() if hasattr(cur["run_time"], "to_pydatetime") else cur["run_time"],
        "run_date": cur["run_date"],
        "run_hour": int(cur["run_hour"]),
        "entity_key": cur["entity_key"],
        "site": cur["site"],
        "country_code": cur["country_code"],
        "country_name": cur["country_name"],
        "date": cur["date"],
        "mapped_revenue_source": cur.get("mapped_revenue_source", ""),
        "join_status": cur.get("join_status", ""),
        "source_mode": _get_source_mode(cur),
        "status_scope": "ACTIVE_DATE",
        "spend": safe_float(cur.get("meta_spend")),
        "revenue_value": safe_float(cur.get("revenue_value")),
        "health_score": health_score,
        "adjustment_score": adjustment_score,
        "ivt_risk_score": ivt_risk_score,
        "decision_margin": round(health_score - ivt_risk_score + min(0.0, adjustment_score), 4),
        "confidence": clip(confidence, 0.0, 1.0),
        "positive_signal_count": len(positive),
        "negative_signal_count": len(negative),
        "neutral_signal_count": len(neutral),
        "skipped_signal_count": len(skipped),
        "adjustment_drop_count": sum(1 for e in events if safe_float(e["adjustment_component"]) < 0),
        "composite_positive_count": len(composite_positive),
        "composite_negative_count": len(composite_negative),
        "traffic_score": _group_health_score(events, "traffic"),
        "delivery_score": _group_health_score(events, "delivery"),
        "yield_score": _group_health_score(events, "yield"),
        "quality_score": _group_health_score(events, "quality"),
        "revenue_score": _group_health_score(events, "revenue"),
        "efficiency_score": _group_health_score(events, "efficiency"),
        "engagement_score": _group_health_score(events, "engagement"),
        "control_score": _group_health_score(events, "control"),
        "ivt_click_stress_score": _group_risk_score(events, "ivt_click_stress"),
        "ivt_serving_score": _group_risk_score(events, "ivt_serving"),
        "ivt_attention_score": _group_risk_score(events, "ivt_attention"),
        "ivt_counter_score": _group_risk_score(events, "ivt_counter"),
        "ivt_funnel_score": _group_risk_score(events, "ivt_funnel"),
        "positive_streak": persistence["positive_streak"],
        "negative_streak": persistence["negative_streak"],
        "adjustment_streak": persistence["adjustment_streak"],
        "ivt_streak": persistence["ivt_streak"],
        "persistence_score": persistence["persistence_score"],
        "persistence_label": persistence["label"],
        "hysteresis_applied": 0,
        "top_positive_labels": pick_top_labels(e["event_label"] for e in positive),
        "top_negative_labels": pick_top_labels(e["event_label"] for e in negative),
        "top_positive_headers": pick_top_labels(e["header_name"] for e in positive),
        "top_negative_headers": pick_top_labels(e["header_name"] for e in negative),
        "root_cause_label": "",
        "final_label": "",
        "reason_summary": "",
    }
    print(f"status_data: {status}")
    status["root_cause_label"] = _root_cause_label(status)
    final_label, hysteresis_applied = _apply_hysteresis(status["root_cause_label"], status, persistence)
    status["final_label"] = final_label
    status["hysteresis_applied"] = hysteresis_applied
    status["reason_summary"] = "; ".join([
        f"health={status['health_score']:.2f}",
        f"ivt={status['ivt_risk_score']:.2f}",
        f"adjust={status['adjustment_score']:.2f}",
        f"traffic={status['traffic_score']:.2f}",
        f"delivery={status['delivery_score']:.2f}",
        f"yield={status['yield_score']:.2f}",
        f"quality={status['quality_score']:.2f}",
        f"source_mode={status['source_mode']}",
        f"persistence={status['persistence_score']:.2f}",
        f"streaks=+{status['positive_streak']}/-{status['negative_streak']}/ivt{status['ivt_streak']}",
    ])
    return status

def _json_safe_records(df: pd.DataFrame) -> list[dict]:
    if df is None or df.empty:
        return []
    out = df.copy()
    out = out.astype(object)
    return [
        {k: _json_safe_value(v) for k, v in row.items()}
        for row in out.to_dict("records")
    ]

def _json_safe_value(value):
    if value is None:
        return None

    if isinstance(value, pd.Timestamp):
        if pd.isna(value):
            return None
        return value.isoformat()

    if isinstance(value, (date, datetime)):
        return value.isoformat()

    if isinstance(value, dict):
        return {str(k): _json_safe_value(v) for k, v in value.items()}

    if isinstance(value, (list, tuple, set)):
        return [_json_safe_value(v) for v in value]

    try:
        import numpy as np
        if isinstance(value, np.ndarray):
            return [_json_safe_value(v) for v in value.tolist()]
        if isinstance(value, np.generic):
            value = value.item()
    except Exception:
        pass

    try:
        if pd.isna(value):
            return None
    except Exception:
        pass

    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None

    return value


def score_site_country(
    target_date: date,
    domain: str | None = None,
    batch_id: str | uuid.UUID | None = None,
    lookback_days: int = 35,
    compatibility_mode: bool = False,
    write_results: bool = True,
) -> dict:
    batch_uuid = ensure_uuid(batch_id)
    history_df = _load_join_history(target_date, domain, lookback_days=lookback_days)
    if domain:
        history_df = history_df[history_df["site"] == domain].copy()
    if history_df.empty:
        return {"ok": True, "rows_written": 0, "event_rows_written": 0, "batch_id": str(batch_uuid), "warning": "No join history for requested filter", "statuses": [], "events": []}
    current_df = history_df[history_df["is_current_batch"]].copy()
    if current_df.empty:
        return {"ok": True, "rows_written": 0, "batch_id": str(batch_uuid), "warning": "No current batch rows found in fact_join_hourly"}
    recent_status_df = _load_recent_status_history(target_date, lookback_days=max(7, min(max(lookback_days, 7), 21)))
    if not recent_status_df.empty:
        if domain:
            recent_status_df = recent_status_df[recent_status_df["site"] == domain].copy()
    status_rows: list[dict] = []
    event_rows: list[dict] = []
    history_df = history_df.sort_values(["entity_base_key", "date", "run_hour", "run_time"]).reset_index(drop=True)
    same_hour_groups = {k: g.copy() for k, g in history_df.groupby(["entity_base_key", "run_hour"])}
    all_hour_groups = {k: g.copy() for k, g in history_df.groupby("entity_base_key")}

    for _, cur in current_df.sort_values(["site", "country_code", "run_time"]).iterrows():
        base_key = cur["entity_base_key"]
        same_hour = same_hour_groups.get((base_key, int(cur["run_hour"])), pd.DataFrame())
        same_hour = same_hour[same_hour["date"] < cur["date"]].copy()
        all_hours = all_hour_groups.get(base_key, pd.DataFrame())
        all_hours = all_hours[all_hours["date"] < cur["date"]].copy()
        row_events: list[dict] = []
        for rule in RULES:
            evt = _evaluate_rule(cur, same_hour, all_hours, rule, batch_uuid)
            row_events.append(evt)

        row_events.extend(_evaluate_composite_events(cur, row_events, batch_uuid))
        persistence = _compute_persistence(cur, recent_status_df)
        status = _summarize_current_row(cur, row_events, batch_uuid, persistence)
        status_rows.append(status)
        event_rows.extend(row_events)

    status_df = pd.DataFrame(status_rows)
    event_df = pd.DataFrame(event_rows)

    if write_results:
        if not status_df.empty:
            if compatibility_mode:
                _coerce_insert_df(STATUS_TABLE, status_df, STATUS_COMPAT_COLUMNS)
            else:
                insert_df(STATUS_TABLE, status_df[STATUS_EXTENDED_COLUMNS])
        if not event_df.empty:
            if compatibility_mode:
                _coerce_insert_df(EVENT_TABLE, event_df, EVENT_COMPAT_COLUMNS)
            else:
                insert_df(EVENT_TABLE, event_df[EVENT_EXTENDED_COLUMNS])

    return {
        "ok": True,
        "rows_written": int(len(status_df)),
        "event_rows_written": int(len(event_df)),
        "batch_id": str(batch_uuid),
        "compatibility_mode": bool(compatibility_mode),
        "statuses": _json_safe_records(status_df),
        "events": _json_safe_records(event_df),
    }