-- Update permits with enhanced data

UPDATE permits.permits SET 
    section = '16',
    block = '17',
    survey = 'MUSQUIZ',
    abstract_no = 'A-7',
    acres = 767.18,
    field_name = '(exactly as shown in RRC records)',
    reservoir_well_count = 7,
    updated_at = NOW()
WHERE status_no = '906213';

UPDATE permits.permits SET 
    section = '16',
    block = '17',
    survey = 'Perpendicu',
    abstract_no = '47',
    acres = 320.00,
    field_name = '(exactly as shown in RRC records)',
    reservoir_well_count = 3,
    updated_at = NOW()
WHERE status_no = '910669';

UPDATE permits.permits SET 
    section = '16',
    block = '17',
    survey = 'Perpendicu',
    abstract_no = '366',
    acres = 1568.80,
    field_name = '(exactly as shown in RRC records)',
    reservoir_well_count = 3,
    updated_at = NOW()
WHERE status_no = '910670';

UPDATE permits.permits SET 
    section = '16',
    block = '17',
    survey = 'Perpendicu',
    abstract_no = '567',
    acres = 1297.00,
    field_name = '(exactly as shown in RRC records)',
    reservoir_well_count = 4,
    updated_at = NOW()
WHERE status_no = '910671';

UPDATE permits.permits SET 
    section = '16',
    block = '17',
    survey = 'Perpendicu',
    abstract_no = '19',
    acres = 645.00,
    field_name = '(exactly as shown in RRC records)',
    reservoir_well_count = 1,
    updated_at = NOW()
WHERE status_no = '910672';

UPDATE permits.permits SET 
    section = '16',
    block = '17',
    survey = 'Perpendicu',
    abstract_no = '23',
    acres = 645.00,
    field_name = '(exactly as shown in RRC records)',
    reservoir_well_count = 2,
    updated_at = NOW()
WHERE status_no = '910673';

UPDATE permits.permits SET 
    section = '16',
    block = '17',
    survey = 'Perpendicu',
    abstract_no = '26',
    acres = 645.00,
    field_name = '(exactly as shown in RRC records)',
    reservoir_well_count = 5,
    updated_at = NOW()
WHERE status_no = '910677';

UPDATE permits.permits SET 
    section = '15',
    block = '28',
    survey = 'PSL',
    abstract_no = 'A-980',
    acres = 1284.37,
    field_name = 'PHANTOM (WOLFCAMP)',
    reservoir_well_count = 2,
    updated_at = NOW()
WHERE status_no = '910678';

UPDATE permits.permits SET 
    section = '15',
    block = '28',
    survey = 'PSL',
    abstract_no = 'A-980',
    acres = 1284.37,
    field_name = 'PHANTOM (WOLFCAMP)',
    reservoir_well_count = 3,
    updated_at = NOW()
WHERE status_no = '910679';

UPDATE permits.permits SET 
    section = '15',
    block = '28',
    survey = 'PSL',
    abstract_no = 'A-980',
    acres = 1284.37,
    field_name = 'PHANTOM (WOLFCAMP)',
    reservoir_well_count = 4,
    updated_at = NOW()
WHERE status_no = '910681';

-- Show results
SELECT COUNT(*) as updated_permits FROM permits.permits WHERE section IS NOT NULL;
SELECT status_no, section, block, survey, abstract_no, acres, field_name, reservoir_well_count FROM permits.permits WHERE status_no LIKE '9106%' ORDER BY status_no LIMIT 5;