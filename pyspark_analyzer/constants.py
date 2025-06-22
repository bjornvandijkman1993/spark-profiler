"""Constants for statistical analysis."""

# Pattern detection regex patterns
PATTERNS = {
    "email": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$",
    "url": r"^https?://",
    "phone": r"^\+?[0-9\s\-\(\)]+$",
    "numeric_string": r"^[0-9]+$",
    "non_ascii": r"[^\x00-\x7F]",
}

# Statistical thresholds
OUTLIER_IQR_MULTIPLIER = 1.5
ZSCORE_THRESHOLD = 3.0

# Sampling and approximation
APPROX_DISTINCT_RSD = 0.05  # Relative standard deviation for approximate count distinct

# Data quality thresholds
QUALITY_OUTLIER_PENALTY_MAX = 0.1  # Max 10% penalty for outliers in quality score
ID_COLUMN_UNIQUENESS_THRESHOLD = 0.95  # ID columns should have > 95% uniqueness

# Performance settings
DEFAULT_TOP_VALUES_LIMIT = 10
