-- Create temporary table for CSV import
CREATE TEMP TABLE temp_enriched (
    status_no TEXT,
    api_no TEXT,
    operator_name TEXT,
    lease_name TEXT,
    well_no TEXT,
    district TEXT,
    county TEXT,
    wellbore_profile TEXT,
    filing_purpose TEXT,
    amend TEXT,
    total_depth TEXT,
    current_queue TEXT,
    detail_url TEXT,
    status_date TEXT,
    horizontal_wellbore TEXT,
    field_name TEXT,
    acres TEXT,
    section TEXT,
    block TEXT,
    survey TEXT,
    abstract_no TEXT,
    reservoir_well_count TEXT,
    w1_pdf_url TEXT,
    w1_text_snippet TEXT,
    w1_parse_confidence TEXT,
    w1_parse_status TEXT,
    w1_last_enriched_at TEXT,
    created_at TEXT,
    updated_at TEXT
);

-- Import CSV data
\COPY temp_enriched FROM 'enriched_permits.csv' WITH (FORMAT CSV, HEADER);

-- Upsert data into main table
INSERT INTO permits.permits (
    status_no, api_no, operator_name, lease_name, well_no, district, county,
    wellbore_profile, filing_purpose, amend, total_depth, current_queue,
    detail_url, status_date, horizontal_wellbore, field_name, acres,
    section, block, survey, abstract_no, reservoir_well_count,
    w1_pdf_url, w1_text_snippet, w1_parse_confidence, w1_parse_status,
    w1_last_enriched_at, created_at, updated_at
)
SELECT 
    status_no,
    NULLIF(api_no, ''),
    NULLIF(operator_name, ''),
    NULLIF(lease_name, ''),
    NULLIF(well_no, ''),
    NULLIF(district, '')::INTEGER,
    NULLIF(county, ''),
    NULLIF(wellbore_profile, ''),
    NULLIF(filing_purpose, ''),
    CASE 
        WHEN amend = 't' THEN TRUE 
        WHEN amend = 'f' THEN FALSE 
        ELSE NULL 
    END,
    NULLIF(total_depth, '')::DECIMAL,
    NULLIF(current_queue, ''),
    NULLIF(detail_url, ''),
    NULLIF(status_date, '')::DATE,
    NULLIF(horizontal_wellbore, ''),
    NULLIF(field_name, ''),
    NULLIF(acres, '')::DECIMAL,
    NULLIF(section, ''),
    NULLIF(block, ''),
    NULLIF(survey, ''),
    NULLIF(abstract_no, ''),
    NULLIF(reservoir_well_count, '')::INTEGER,
    NULLIF(w1_pdf_url, ''),
    NULLIF(w1_text_snippet, ''),
    NULLIF(w1_parse_confidence, '')::DECIMAL,
    NULLIF(w1_parse_status, ''),
    NULLIF(w1_last_enriched_at, '')::TIMESTAMP WITH TIME ZONE,
    NULLIF(created_at, '')::TIMESTAMP WITH TIME ZONE,
    NOW()
FROM temp_enriched
ON CONFLICT (status_no) 
DO UPDATE SET
    api_no = EXCLUDED.api_no,
    operator_name = EXCLUDED.operator_name,
    lease_name = EXCLUDED.lease_name,
    well_no = EXCLUDED.well_no,
    district = EXCLUDED.district,
    county = EXCLUDED.county,
    wellbore_profile = EXCLUDED.wellbore_profile,
    filing_purpose = EXCLUDED.filing_purpose,
    amend = EXCLUDED.amend,
    total_depth = EXCLUDED.total_depth,
    current_queue = EXCLUDED.current_queue,
    detail_url = EXCLUDED.detail_url,
    status_date = EXCLUDED.status_date,
    horizontal_wellbore = EXCLUDED.horizontal_wellbore,
    field_name = EXCLUDED.field_name,
    acres = EXCLUDED.acres,
    section = EXCLUDED.section,
    block = EXCLUDED.block,
    survey = EXCLUDED.survey,
    abstract_no = EXCLUDED.abstract_no,
    reservoir_well_count = EXCLUDED.reservoir_well_count,
    w1_pdf_url = EXCLUDED.w1_pdf_url,
    w1_text_snippet = EXCLUDED.w1_text_snippet,
    w1_parse_confidence = EXCLUDED.w1_parse_confidence,
    w1_parse_status = EXCLUDED.w1_parse_status,
    w1_last_enriched_at = EXCLUDED.w1_last_enriched_at,
    updated_at = NOW();

-- Show results
SELECT 
    'Data pushed successfully!' as message,
    COUNT(*) as total_enriched_permits
FROM permits.permits 
WHERE w1_last_enriched_at IS NOT NULL;

-- Show sample of enhanced data
SELECT 
    status_no, lease_name, section, block, survey, abstract_no, 
    acres, field_name, reservoir_well_count
FROM permits.permits 
WHERE section IS NOT NULL 
ORDER BY status_no 
LIMIT 5;
