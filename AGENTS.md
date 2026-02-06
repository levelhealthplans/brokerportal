# Level Health Broker Portal – Agent Guide

## Direct Contract / Primary Network Assignment

### 1. Purpose
Assign a Primary Network / Direct Contract to each group based on member ZIP codes from the census file.
The goal is to maximize direct contracts while maintaining fallback coverage.
Assignment must be deterministic and reproducible.

### 2. Inputs
**Census File** (CSV/XLS/XLSX)
Minimum required field:
- `zip` (5-digit residential ZIP per member)

Optional fields:
- county
- state
- member_id
- employee_id
- dependent_flag

All ZIP codes must be normalized to 5 digits.

**Contract Mapping File**
Single source of truth lookup:
- `backend/data/network_mappings.csv`

Example:
```
zip,network
63011,Mercy_MO
45202,H2B_OH
```

### 3. Processing Logic
1. Ingest census file.
2. Extract all member ZIP codes.
3. Normalize ZIP format.
4. Validate ZIPs.
5. Map each ZIP to a network using the mapping file.
6. If ZIP not found → assign DEFAULT_NETWORK.
7. Aggregate results across all members.
8. Calculate coverage by network.
9. Select dominant network if threshold met.
10. Otherwise assign fallback network.

### 4. Assignment Rules
**Member-Level**
```
if zip in mapping:
    network = mapping[zip]
else:
    network = DEFAULT_NETWORK
```

DEFAULT_NETWORK = `Cigna_PPO`

**Group-Level**
```
coverage = members_in_direct_contract / total_members

if coverage >= 0.90:
    primary_network = direct_contract
else:
    primary_network = DEFAULT_NETWORK
```

Threshold is configurable.

### 5. Special Cases
**Mercy System Split**
Never merge Mercy regions:
- Mercy_MO ≠ Mercy_OH ≠ Mercy_AR

**Mixed Coverage**
If two direct contracts each exceed 40%:
- Flag as `MIXED_NETWORK`
- Require manual review
- Do not auto-assign

**Invalid ZIPs**
If ZIP is missing, malformed, or non‑US:
- Log error
- Exclude from coverage calculation
- Flag census as incomplete

### 6. Outputs
**Group-Level Output**
- `primary_network`
- `coverage_percentage`
- `fallback_used`
- `review_required`

**Member-Level Output**
- `zip`
- `assigned_network`
- `matched`

### 7. Determinism Requirements
- No randomness
- No ML inference
- No probabilistic assignment
- Same input → same output

### 8. Update Policy
When adding or changing ZIP mappings:
1. Update mapping file
2. Update tests
3. Re-run validation
4. Commit together

Never update logic without mapping updates.

### 9. Test Cases
Maintain a test suite with known censuses:

```
ZIP   Expected Network
63011 Mercy_MO
45202 H2B_OH
99999 Cigna_PPO
```

Include:
- Single-network groups
- Mixed groups
- Edge ZIPs
- Border counties

### 10. File Locations
- Census parsers: `backend/main.py`
- Mapping files: `backend/data/network_mappings.csv`
- Assignment logic: `backend/main.py`
- Tests: (to be added)

Big care for small business.
