-- Claims Summary Mart
-- Purpose: Aggregated claims metrics for reporting and analysis
-- Owner: Analytics Team
-- Refresh: Daily

CREATE OR REPLACE TABLE marts.mart_claims_summary AS
WITH claims_base AS (
    SELECT
        c.claim_id,
        c.claimant_id,
        c.claim_date,
        c.resolution_date,
        c.status,
        c.category_id,
        c.claim_amount,
        c.approved_amount,
        c.provider_id,
        dt_claim.fiscal_year,
        dt_claim.fiscal_month,
        dt_claim.calendar_month,
        dt_resolution.fiscal_year as resolution_fiscal_year,
        DATEDIFF(day, c.claim_date, c.resolution_date) as days_to_resolve,
        CASE
            WHEN c.status = 'Approved' THEN 1
            WHEN c.status = 'Denied' THEN 0
            ELSE NULL
        END as is_approved
    FROM staging.stg_claims c
    LEFT JOIN marts.dim_date dt_claim
        ON DATE(c.claim_date) = dt_claim.calendar_date
    LEFT JOIN marts.dim_date dt_resolution
        ON DATE(c.resolution_date) = dt_resolution.calendar_date
    WHERE c.claim_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 YEARS)
),

claims_aggregated AS (
    SELECT
        fiscal_year,
        fiscal_month,
        calendar_month,
        category_id,
        COUNT(DISTINCT claim_id) as total_claims,
        COUNT(DISTINCT CASE WHEN status = 'Approved' THEN claim_id END) as approved_claims,
        COUNT(DISTINCT CASE WHEN status = 'Denied' THEN claim_id END) as denied_claims,
        COUNT(DISTINCT CASE WHEN status = 'Pending' THEN claim_id END) as pending_claims,
        SUM(claim_amount) as total_claim_amount,
        SUM(approved_amount) as total_approved_amount,
        AVG(claim_amount) as avg_claim_amount,
        AVG(approved_amount) as avg_approved_amount,
        MIN(claim_amount) as min_claim_amount,
        MAX(claim_amount) as max_claim_amount,
        STDDEV(claim_amount) as stddev_claim_amount,
        APPROX_PERCENTILE_CONT(claim_amount, 0.5) as median_claim_amount,
        APPROX_PERCENTILE_CONT(claim_amount, 0.9) as p90_claim_amount,
        COUNT(DISTINCT claimant_id) as unique_claimants,
        COUNT(DISTINCT provider_id) as unique_providers,
        ROUND(SUM(CASE WHEN is_approved = 1 THEN 1 ELSE 0 END) /
              NULLIF(COUNT(*), 0) * 100, 2) as approval_rate_pct,
        ROUND(AVG(CASE WHEN days_to_resolve > 0 THEN days_to_resolve END), 1) as avg_days_to_resolve
    FROM claims_base
    GROUP BY fiscal_year, fiscal_month, calendar_month, category_id
)

SELECT
    CURRENT_TIMESTAMP() as loaded_at,
    fiscal_year,
    fiscal_month,
    calendar_month,
    category_id,
    total_claims,
    approved_claims,
    denied_claims,
    pending_claims,
    total_claim_amount,
    total_approved_amount,
    avg_claim_amount,
    avg_approved_amount,
    min_claim_amount,
    max_claim_amount,
    stddev_claim_amount,
    median_claim_amount,
    p90_claim_amount,
    unique_claimants,
    unique_providers,
    approval_rate_pct,
    avg_days_to_resolve
FROM claims_aggregated
ORDER BY fiscal_year DESC, fiscal_month DESC, category_id;

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_claims_summary_fiscal
    ON marts.mart_claims_summary(fiscal_year, fiscal_month);

CREATE INDEX IF NOT EXISTS idx_claims_summary_category
    ON marts.mart_claims_summary(category_id);

