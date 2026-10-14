
## Section 1: Introduction and Scope

### 1.1 Problem and Analytical Objective

This project addresses a fundamental challenge in data engineering: reliable data pipelines do not assume data correctness, therefore, they actively verify data quality through automated validation gates before loading into analytical systems. The integration of Spotify's consumer behavior dataset with Grammy Awards institutional recognition data enables sophisticated analysis of artist commercial performance and award correlation, but only when underpinned by robust data quality assurance.

The analytical value derives from combining two complementary data sources: Spotify provides granular consumer behavior signals including streaming popularity, acoustic characteristics, and track-level engagement metrics, while Grammy Awards data offers independently-validated institutional recognition of artistic achievement. This integration into a Dimensional Data Warehouse supports strategic decision-making across marketing, production, and artist development functions.

However, both source systems introduce quality risks. Spotify data contains potential nulls, out-of-range metrics, and ambiguous artist representations; Grammy data exhibits selection bias and categorical inconsistencies. A pipeline that loads unvalidated data propagates these errors into downstream KPI calculations, corrupting analytical conclusions. This project implements Great Expectations as a pre-load validation framework, establishing automated quality gates that enforce data conformance standards before records enter the Data Warehouse. This approach ensures analytical integrity: every KPI rests on verified, traceable data.

### 1.2 Analytical Requirements

| Analytical Requirement | Required Data & Sources | Expected KPI(s) | Required Level of Detail | Justification |
| :--- | :--- | :--- | :--- | :--- |
| AR1: Catalog Performance Baseline Analysis | Spotify: artists, popularity, track_name; Grammy: artist, winner | Mean and standard deviation of popularity across artist discographies; comparative baseline metrics (Grammy-winning vs. non-winning artist catalogs) | Artist-level aggregation across complete discographic records | Establishes whether Grammy status correlates with elevated baseline popularity across an artist's entire catalog, informing portfolio-level commercial assessment |
| AR2: Acoustic Profile Analysis of Awarded Tracks | Spotify: track_name, duration_ms, danceability, valence, popularity; Grammy: nominee, category | Distribution statistics of acoustic features (danceability, valence) in Grammy-winning vs. non-awarded high-popularity tracks; feature correlation coefficients within award categories | Track-level granularity; award-category-specific acoustic profile segmentation | Identifies whether Grammy-winning tracks exhibit consistent acoustic signatures, enabling predictive modeling of acoustic features associated with award recognition |
| AR3: Industry Validation vs. Acoustic Traits Correlation | Spotify: artists, popularity, danceability, valence; Grammy: artist, winner | Average popularity score grouped by acoustic quartile (low vs. high engagement) and award status. | Artist and track-level aggregation. | Evaluates the commercial impact of institutional validation by comparing popularity across tracks with low intrinsic acoustic engagement (low valence, low danceability), segmented by Grammy-winning status. |

### 1.3 Solution Scope

In Scope:
* Data Integration & ETL Orchestration: Apache Airflow-managed pipeline extracting Spotify CSV and Grammy relational database sources, performing transformations, and loading into the Data Warehouse.
* Data Quality Validation: Great Expectations framework implementing automated quality checks (schema validation, null checks, value range enforcement, referential integrity, distribution analysis).
* Dimensional Data Warehouse: Star schema architecture with conformed dimensions (artists, tracks, award categories) and fact tables (track popularity metrics, Grammy nominations).
* Dimensional Modeling: Dimensional design patterns including slowly changing dimensions (SCD Type 2), surrogate keys, and fact table grain optimization for analytical query patterns.
* Business Intelligence Dashboard: BI Dashboard connected to the Data Warehouse (Tableau, Power BI, Metabase, or equivalent) visualizing all three analytical requirements with interactive filtering and drill-down capability.
* Data Lineage & Traceability: Complete traceability from source system extraction through transformation logic to final KPI calculations and visualizations.

Out of Scope:
* Real-Time Streaming: This project implements batch processing; event streaming architectures and near-real-time data ingestion are excluded.
* Machine Learning & Predictive Modeling: Statistical model development and prediction systems are outside scope; the Data Warehouse is structured to support future ML workflows but does not include model implementation.
* External Data Enrichment: Third-party data sources, social media integration, chart rankings, or supplementary enrichment services are not integrated.
* Advanced Statistical Inference: Causal analysis, econometric modeling, hypothesis testing, and significance testing are excluded; scope limited to descriptive analytics and exploratory analysis.

### 1.4 End-to-End Traceability Matrix

| Requirement | Required Data | Quality Risk | DQ Rule / GX Expectation | Transformation | DW Element | KPI / Visualization |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| AR1: Catalog Performance Baseline Analysis | Spotify artists, popularity, track_name; Grammy artist, winner | (1) artists column contains comma-separated collaborator names requiring disambiguation; (2) Null values in popularity metric (missing engagement data); (3) Artist name inconsistencies across Spotify and Grammy datasets; (4) popularity values outside expected 0-100 range | expect_column_values_to_not_be_null (popularity); expect_column_values_to_be_between (popularity: 0-100); expect_column_values_to_match_regex (artists format validation); expect_table_columns_to_match_ordered_list (schema conformance) | Parse multi-artist collaboration records into individual artist entities (explode operation); standardize artist names via fuzzy matching algorithm; calculate per-artist popularity aggregates (mean, stddev) across full discography | dim_artists (artist_id, artist_name, grammy_winner_flag); fact_track_popularity (artist_id, track_id, popularity_score, catalog_date) | Mean/StdDev popularity comparison; box plot visualization by Grammy winner status; statistical summary table of baseline metrics by artist category |
| AR2: Acoustic Profile Analysis of Awarded Tracks | Spotify track_name, duration_ms, danceability, valence, popularity; Grammy nominee, category | (1) Null or missing acoustic feature values (danceability, valence); (2) danceability and valence outside theoretical range [0-1]; (3) duration_ms values indicating data corruption (negative or >3,600,000ms); (4) Duplicate track records; (5) Grammy category mismatches with Spotify nominees | expect_column_values_to_not_be_null (danceability, valence, duration_ms); expect_column_values_to_be_between (danceability: 0-1, valence: 0-1); expect_column_values_to_between (duration_ms: 1000-3600000); expect_compound_columns_to_be_unique (artist, track_name, duration_ms) | Deduplication by (artist_id, track_name, duration_ms) composite key; inner join tracks to Grammy nominees; calculate acoustic feature percentiles and distributions per award category | dim_tracks (track_id, track_name, danceability, valence, duration_ms); dim_grammy_category (category_id, category_name); fact_grammy_nominations (track_id, category_id, nominee_status, award_status) | Acoustic feature distribution histograms by award category; correlation coefficient matrix (danceability, valence, popularity); violin plots comparing Grammy-winning vs. non-winning track acoustic profiles |
| AR3:  Industry Validation vs. Acoustic Traits Analysis | Spotify: artists, popularity, danceability, valence; Grammy: artist, winner | (1) `winner` column lacks variance (only 'True' values); (2) Missing danceability/valence values. | `expect_column_values_to_not_be_null` (danceability, valence, popularity); `expect_column_distinct_values_to_equal` (winner, 1) | Segment tracks into acoustic quartiles using conditional logic (low/high valence and danceability); join with Grammy dimension; aggregate average popularity by acoustic segment and award status | dim_artists (artist_id, artist_name, grammy_winner_flag); dim_tracks (track_id, track_name, acoustic_quartile); fact_track_performance (track_id, artist_id, popularity_score)) | Bar chart comparing average popularity of low-acoustic tracks (Grammy winners vs. non-winners). |
# Section 2: Data Sources and Architecture

### 2.1 Data Sources

The pipeline integrates two complementary data sources:
1. **Spotify Dataset (CSV):** A flat comma-separated values file containing track-level metadata and acoustic characteristics. Fields include artist names, track identifiers, duration (milliseconds), acoustic features (danceability, valence, energy, acousticness), and Spotify popularity scores (0-100 scale). This dataset represents consumer engagement metrics and intrinsic audio product attributes.
2. **Grammy Awards Dataset (Relational Database):** A MySQL relational database containing institutional award nomination and winner records. Tables include artist names, award categories, nomination years, and winner/nominee status flags. This source simulates a production relational system serving as the system of record for Grammy institutional data.

### 2.2 Source Database Preparation

The Grammy Awards relational source was prepared to comply with strict engineering requirements prohibiting direct CSV consumption by pipeline components.

A Docker Compose service (`mysql-source`) was configured to spin up a MySQL 8.0 container listening on port 3306. A SQL initialization script (`source_setup.sql`) was mounted via the `docker-entrypoint-initdb.d` directory, enabling automatic schema and data initialization upon container startup.

The initialization script executes the following operations:
* Creates the `grammy_source` database and associated schema.
* Executes `LOAD DATA INFILE` statements to populate the `grammy_awards` table directly from a physical CSV file mounted within the container filesystem.

This architecture effectively simulates a live production relational database, allowing the Airflow pipeline to extract Grammy data via standard database connectivity protocols rather than consuming files directly. It establishes proper separation between data sources and pipeline orchestration, supporting future enhancements such as incremental load patterns and transactional consistency.

### 2.3 Source-to-Pipeline Mapping

The pipeline establishes source-specific connections through standardized integrations:
* **Spotify Data Mapping:** Airflow accesses the Spotify CSV file via Pandas filesystem operations targeting the local `data/` directory. Great Expectations utilizes a Pandas data source configured to read directly from CSV path references, enabling in-memory validation of raw records.
* **Grammy Awards Data Mapping:** A SQL-Alchemy connection string (`mysql+mysqlconnector://root:root@localhost:3306/grammy_source`) points Airflow and Great Expectations to the Docker-hosted MySQL container. Database hooks execute parameterized queries and return result sets as in-memory structures for validation and transformation.

Both connections are externalized to Airflow configuration (Connections UI or environment variables), enabling environment-specific routing without code modification.

### 2.4 Pipeline Architecture

The pipeline architecture enforces a strict 5-stage design ensuring data quality validation before integration and warehouse loading. Non-conforming data triggers pipeline halting, preventing corrupted records from reaching analytical systems.
![pipeline architecture](docs/sources/pipelineW2.svg)

# 3. Data Profiling and Quality Design

## Section 3: Data Profiling and Quality Design

### 3.1 Data Profiling Methodology

Data profiling was conducted using Pandas in a Jupyter Notebook (`notebooks/data_profiling.ipynb`) to establish empirical data characteristics without altering raw source files. The profiling analysis evaluated three critical dimensions:
* **Completeness:** Detection of null values, missing records, and gaps in key attributes across both datasets.
* **Validity:** Range validation, domain conformance, and structural correctness against theoretical and business constraints.
* **Uniqueness:** Grain analysis, cardinality assessment, and identification of duplicate records at source level.

Profiling findings directly informed the design of quality rules, validation thresholds, and transformation logic. No assumptions were made about data quality; all rules derive from observed empirical characteristics.

### 3.2 Spotify Profiling Findings

Dataset Scale: 114,000 rows (complete track records across artists and albums).

Completeness Analysis:
* `artists` column: 1 null value (0.0009% incompleteness).
* `track_name` column: 1 null value (0.0009% incompleteness).
* All other columns (`popularity`, `danceability`, `valence`, `duration_ms`, `track_id`): 0 nulls (100% complete).

Validity Analysis:
* `popularity` (0-100 scale): Min 0, Max 100. All values conform to theoretical range. Distribution shows right skew with median ~52.
* `danceability` (0-1 scale): Min 0.0, Max 0.974. All values conform to theoretical [0,1] range.
* `duration_ms` (track length in milliseconds): Min 0 ms, Max 3,661,000 ms. Critical finding: 47 tracks exhibit `duration_ms = 0`, which is physically impossible for audio files. This indicates data corruption or placeholder values in the source system.

Structure Analysis:
* `artists` column uses semicolon (`;`) as collaboration delimiter (e.g., `'Ingrid Michaelson;ZAYN'` represents two-artist collaboration). This is a flat string representation, not a native list or array structure. Parsing requires explicit string splitting before relational join operations.

### 3.3 Grammy Profiling Findings

Dataset Scale: 23,457 rows (nomination and award records).

Completeness Analysis:
* `artist` column: Multiple historical records. These nulls correspond to technical awards, orchestral recordings, and ensemble performances where artist attribution is absent; such records use alternative identifier columns (`nominee`, `workers`).
* `winner` column: 0 nulls (100% complete).
* `category` and `year` columns: 0 nulls (100% complete).

Domain Analysis:
* `winner` column contains exclusively `True` values (no `False` values, no null values). This indicates the source table represents winners only—a pre-filtered dataset lacking negative cases. This creates severe selection bias for comparative analysis (AR1, AR3) requiring Grammy winner vs. non-winner cohorts.

Uniqueness & Grain Analysis:
* Table grain is nomination/award record, not artist. Artist cardinality is significantly lower than row count.
* Example: Artist 'U2' appears 18 times across different award categories, years, and nomination statuses.
* This multiplicity creates risk of Cartesian explosion when joined with Spotify tracks without deduplication.

### 3.4 Cross-Source Profiling

Text Matching Risk Analysis:
When attempting to match Spotify artists to Grammy artist via string equality, high false negative rates emerge due to unclean text representation:
* Spotify raw text: `'LU2VYK;Janethan'` contains the substring `'U2'` embedded within a collaboration string.
* Grammy text: `'U2'` is a clean, isolated artist identifier.
* String matching on raw values fails to recognize U2 as present in the Spotify record.

Root Cause: Spotify artist strings require normalization (lowercase, whitespace trim) and structural decomposition (semicolon-based explosion into individual artist records) before relational join on Grammy artist identifiers.

Impact: Without preprocessing, legitimate U2 track records fail to join with Grammy U2 award records, producing false negatives (U2 tracks incorrectly marked as non-Grammy-winning).

### 3.5 Quality Risks

The identified data quality gaps present concrete risks to analytical integrity:
* **Risk 1: Cartesian Explosion in Fact Table**
  * Grammy data exhibits multiple records per artist (U2: 18 records across different categories/years).
  * A naive inner join or left join without Grammy deduplication multiplies each Spotify track metric across all Grammy nomination records for that artist.
  * Example: 1 Spotify U2 track $\times$ 18 Grammy U2 records = 18 identical fact rows with duplicated popularity metrics, corrupting aggregate KPI calculations.
* **Risk 2: Integration Failure via Collaboration Delimiter**
  * Spotify artists column uses semicolon-delimited strings representing artist collaborations.
  * Join on unprocessed strings fails: `'Artist1;Artist2'` $\neq$ `'Artist1'` at string level.
  * Tracks from collaborative artists are systematically excluded from join, producing incomplete Grammy enrichment and biased AR1/AR3 baselines.
* **Risk 3: Null Key Corruption**
  * masive null values in Grammy artist column propagate to join key; nulls in join keys produce unmatched, orphaned records.
  * Traceability breaks for technical award categories, preventing audit trail reconstruction.
* **Risk 4: Acoustic Profile Corruption**
  * Presence of cues with `duration_ms = 0` corrupt acoustic feature analysis (AR2 requirement).
  * Duration-normalized acoustic metrics become undefined (division by zero).
  * Affected tracks distort acoustic distribution statistics, skewing Grammy-winning track profile conclusions.

### 3.6 Quality Rules, Metrics, Thresholds and Severity

| Rule ID | Dataset/Layer | Attribute(s) | Quality Dimension | Quality Rule | Metric / Threshold | Severity |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| R1 | Spotify Raw | `popularity` | Validity | Value must be between 0 and 100 | Min $\geq$ 0, Max $\leq$ 100 | Critical |
| R2 | Spotify Raw | `duration_ms` | Validity | Track duration must be physically valid | > 1,000 ms | Critical |
| R3 | Spotify Raw | `artists` | Completeness | Artist field must not be null | Null count = 0 | Critical |
| R4 | Grammy Raw | `winner` | Domain | Winner column must contain expected Boolean values | Values in `['True']` | Informational |
| R5 | Prepared | `grammy_winner_flag` | Completeness | Grammy enrichment flag must not be null after join | Null count = 0 | Critical |
| R6 | Prepared | `acoustic_quartile` | Domain | Acoustic engagement classification must be valid | Values in `['High Engagement', 'Medium Engagement', 'Low Engagement']` | Critical |

### 3.7 Great Expectations Validation Design

Profiling-derived quality rules were materialized into Great Expectations validation suites, enabling automated enforcement at pipeline checkpoints:

Validation Suites:
1. `spotify_raw_suite`: Enforces R1, R2, R3 on raw Spotify extracts.
   * `expect_column_values_to_be_between` (`popularity`: 0–100)
   * `expect_column_values_to_be_between` (`duration_ms`: 1000–3661000)
   * `expect_column_values_to_not_be_null` (`artists`)
2. `grammys_raw_suite`: Enforces R4 on raw Grammy extracts.
   * `expect_column_values_to_be_in_set` (`winner`: `['True']`)
3. `prepared_suite`: Enforces R5, R6 on integrated dataset post-transformation.
   * `expect_column_values_to_not_be_null` (`grammy_winner_flag`)
   * `expect_column_values_to_be_in_set` (`acoustic_quartile`: `['High Engagement', 'Medium Engagement', 'Low Engagement']`)

Execution Model:
Each suite executes as a Great Expectations Checkpoint invoked by an Airflow BashOperator task. Checkpoint execution occurs at designated pipeline stages:
* `validate_spotify_raw` task triggers `spotify_raw_suite`.
* `validate_grammys_raw` task triggers `grammys_raw_suite`.
* `validate_prepared` task triggers `prepared_suite` after transformation and join completion.

Circuit-Breaker Enforcement:
Any Critical-severity rule violation (exit code $\neq$ 0) causes immediate Airflow task failure. Airflow's dependency graph automatically halts all downstream tasks (`transform_and_integrate`, `validate_prepared`, `load_dw`), preventing non-conforming data from entering the Data Warehouse. This circuit-breaker mechanism ensures analytical layer integrity by rejecting corrupted records at ingestion boundaries.

## 4. Data Transformation and Integration

### 4.1 Transformation Strategy

Data transformation executes in-memory using Pandas within an Airflow PythonOperator task. Transformations are applied to clean, standardize, and prepare data grain for relational integration without modifying records to artificially satisfy validation rules.

**Spotify Transformation Pipeline:**
The `artists` column undergoes sequential normalization:
1. Null values are removed via `dropna()` to prevent orphaned join keys.
2. String representations (brackets, quotation marks) are stripped using regex pattern matching.
3. Artist strings are split on the semicolon (`;`) delimiter, separating collaborating artists.
4. The Pandas `explode()` function transforms each row into multiple rows (one per artist), with all other track attributes (`popularity`, `danceability`, `duration_ms`, `track_id`) replicated across exploded rows.
5. Artist names are converted to lowercase and stripped of leading/trailing whitespace for case-insensitive matching.

**Grammy Transformation Pipeline:**
The `artist` column undergoes standardization and deduplication:
1. Artist names are converted to lowercase.
2. Whitespace trimming is applied (leading/trailing removal).
3. Strict deduplication is performed: for each unique artist name, a single record is retained; duplicate artist entries across different award years, categories, or nomination statuses are collapsed into one representative row per winning artist.
4. Records with null artist values (technical awards, orchestral ensembles without individual artist attribution) are excluded from the deduplicated set.

### 4.2 Transformation Rules

| Rule and Rationale | Affected Fields | Before / After Behavior | Exception Handling | Analytical Impact |
| :--- | :--- | :--- | :--- | :--- |
| **T1: Explicit Null Dropping**<br>Rationale: Null join keys cause orphaned records and break traceability. | `artists` (Spotify) | Before: Contains NaN.<br>After: Rows removed. | Rows with null artists are dropped via `dropna()` before explosion; nulls are not imputed. | Ensures AR1 and AR3 group-by aggregations do not include records with missing artist attribution, preserving analytical cohort integrity. |
| **T2: Delimiter Explosion**<br>Rationale: Semicolon-delimited artist strings prevent exact matching. | `artists` (Spotify) | Before: 'Ingrid Michaelson;ZAYN' (single row).<br>After: Row 1 'ingrid michaelson', Row 2 'zayn' (two rows). | Single-artist strings (no semicolon) remain as single rows. | Enables artist-level granularity and aggregation for AR1; ensures each artist in a collaboration receives proper Grammy enrichment. |
| **T3: Grammy Deduplication**<br>Rationale: Multiple records per artist multiply Spotify track metrics across duplicate Grammy records. | `artist` (Grammys) | Before: 'U2' appears 18 times across different award categories/years.<br>After: 'U2' appears 1 time (unique winner record). | Artists with null names (technical awards) are filtered out. | Prevents Cartesian explosion in the fact table; ensures each Spotify track for a Grammy-winning artist joins exactly once per artist. |
| **T4: Acoustic Categorization**<br>Rationale: AR3 requires segmented acoustic profiles. | `acoustic_quartile` (Derived) | Before: Raw `danceability` and `valence` floats.<br>After: Categorical label ('High', 'Medium', 'Low'). | Invalid floats default to 'Unknown' category. | Directly supports AR3 requirement to isolate tracks by acoustic engagement. |
| **T5: Defensive String Truncation**  Prevents pipeline failure from anomalous metadata exceeding schema limits (VARCHAR 255) | track_name (Spotify) | Before: Strings > 255 chars. After: Strings right-truncated to 255 chars. | Silently truncates rightmost characters. | None. Track identifiers (track_id) and metrics remain intact; only the display name is shortened. |

### 4.3 Integration Contract

**Integration Keys:**
- **Left Key:** Spotify's normalized `artists` column (post-explosion, lowercase, whitespace-trimmed).
- **Right Key:** Grammy's deduplicated, normalized `artist` column (lowercase, whitespace-trimmed).
- **Key Matching:** Exact string equality on normalized values.

**Cardinality Specification:**
- **Relationship:** Many-to-One.
- **Rationale:** Many Spotify tracks can reference the same artist; each artist maps to at most one unique Grammy winner record (post-deduplication).

**Preprocessing Requirements:**
- Both join keys are converted to lowercase prior to join operation.
- Both join keys are stripped of leading and trailing whitespace.
- Spotify artists are exploded (one artist per row) before join execution.
- Grammy artists are deduplicated (one unique artist record) before join execution.

### 4.4 Matching and Integration Logic

**Join Type:** Left Outer Join (Pandas `merge(how='left')`)

**Join Specification:**
`Spotify_exploded` (left) LEFT JOIN `Grammy_deduplicated` (right) ON `Spotify.artists_clean` = `Grammy.artist_clean`

**Rationale for Left Outer Join:**
- The left table (Spotify) represents complete commercial metadata: every track's popularity and acoustic features must be preserved in the integrated dataset.
- The right table (Grammy) enriches with institutional validation status; the absence of a match indicates a non-winning artist, which is a valid analytical state.
- A left join ensures 100% of Spotify track records remain in the integrated dataset, preventing analytical bias.

### 4.5 Reconciliation and Integration Evidence

**Row Count Preservation:**
The deduplication of Grammy data prior to the join ensures deterministic row cardinality post-integration:
- **Evidence of No Data Loss or Duplication:** The row count remains invariant across the left join. The integrated dataset row count exactly matches the Spotify post-explosion row count. 
- This invariance confirms that no Cartesian explosion occurred. All Spotify records are preserved with their original track-level metrics and attributes.

### 4.6 Exception and Unmatched Record Handling

**Unmatched Record Identification:**
Records for which Spotify exploded artists have no corresponding entry in the deduplicated Grammy winners set retain null values in Grammy enrichment columns after the left join.

**Null Imputation Strategy:**
The `grammy_winner_flag` column is explicitly imputed with the value `False` for all unmatched records.

**Imputation Justification:**
This imputation creates the essential analytical control group:
- **AR1 (Catalog Performance Baseline):** Requires comparison of mean popularity across Grammy-winning vs. non-winning artist catalogs. Non-winning artists (`False` flag) form the statistical baseline.
- **AR3 (Industry Validation vs. Acoustic Traits):** Requires isolation of the Grammy label effect on popularity. The `False` cohort serves as the regression reference group.
- **Semantic Correctness:** An artist absent from the Grammy winners dataset is definitionally not a Grammy winner; `False` is the correct, non-missing value for this attribute.


## Section 5: Data Warehouse and Analytics

### 5.1 Analytical Target

The analytical objective is to establish a reliable, dimensional Data Warehouse enabling cross-analysis of commercial track performance (Spotify) with institutional recognition (Grammy Awards). The warehouse supports evaluation of acoustic and commercial impact by segmenting artists into Grammy-winning and non-winning cohorts, enabling comparative analysis of catalog baseline performance, acoustic profiles of award-winning tracks, and the isolated effect of industry validation on commercial success metrics.

### 5.2 Dimensional Model Design

The warehouse architecture follows dimensional modeling principles, implementing a Star Schema structure optimized for OLAP (Online Analytical Processing) query patterns. The design centralizes dynamic track performance metrics (facts) surrounded by immutable descriptive context (dimensions), enabling efficient aggregation queries across artist and track attributes without requiring complex multi-table joins or denormalization.

The dimensional design separates analytical concerns:
* Facts capture event-level performance metrics.
* Dimensions provide stable, descriptive contextual attributes.
* Keys enforce referential integrity and enable tractable join paths.

This separation reduces query complexity and enables analysts to rapidly explore performance across artist Grammy status, acoustic characteristics, and track-level attributes.

### 5.3 Star Schema

The Star Schema is implemented in MySQL, consisting of one central fact table surrounded by two primary dimension tables:
![Dashboard evidence](docs/sources/star_squemas.svg)
Schema Characteristics:
* **Fact Table:** fact_track_performance (central table with foreign keys to dimensions).
* **Dimension Tables:** dim_artists, dim_tracks (surrounding tables with primary keys).
* **Grain:** One row per unique artist-track performance observation.
* **Optimization:** Natural keys (VARCHAR identifiers) enable direct join operations; denormalization of dimension attributes eliminates the need for dimension-to-dimension joins.

### 5.4 Fact and Dimension Tables

**dim_artists (Artist Dimension)**

| Column | Type | Description |
| :--- | :--- | :--- |
| artist_id | VARCHAR(255) | Primary key; normalized artist identifier (lowercase, whitespace-trimmed). |
| artist_name | VARCHAR(255) | Full artist name from source data. |
| grammy_winner_flag | BOOLEAN | Institutional award status (True/False); imputed to False for non-winning artists. |

*Grain:* One row per unique artist after deduplication. Represents a single artist record with immutable Grammy institutional status.

**dim_tracks (Track Dimension)**

| Column | Type | Description |
| :--- | :--- | :--- |
| track_id | VARCHAR(255) | Primary key; unique track identifier from Spotify. |
| track_name | VARCHAR(255) | Track title; truncated to 255 characters to ensure storage compliance. |
| acoustic_quartile | VARCHAR(50) | Categorical acoustic engagement classification ('High Engagement', 'Medium Engagement', 'Low Engagement', 'Unknown'). |
| danceability | FLOAT | Normalized audio feature (0-1 scale); enables detailed acoustic analysis. |
| valence | FLOAT | Normalized audio feature (0-1 scale); enables detailed acoustic analysis. |
| duration_ms | INT | Track duration in milliseconds; used for acoustic normalization and data quality validation. |

*Grain:* One row per unique track from Spotify dataset. Immutable attributes are stored; no historical versioning.

**fact_track_performance (Performance Fact Table)**

| Column | Type | Description |
| :--- | :--- | :--- |
| artist_id | VARCHAR(255) | Foreign key referencing dim_artists.artist_id. |
| track_id | VARCHAR(255) | Foreign key referencing dim_tracks.track_id. |
| popularity_score | INT | Spotify popularity metric (0-100 scale); primary measure for AR1 and AR3. |

*Grain:* One row per unique artist-track collaboration (after explosion of multi-artist tracks). Represents the commercial performance of a specific artist's involvement in a specific track.

### 5.5 Keys, Relationships and Constraints

Primary Keys:
* dim_artists.artist_id: VARCHAR(255) natural key derived from normalized artist name.
* dim_tracks.track_id: VARCHAR(255) natural key derived from Spotify track identifier.

Foreign Keys:
* fact_track_performance.artist_id REFERENCES dim_artists(artist_id)
* fact_track_performance.track_id REFERENCES dim_tracks(track_id)

Referential Integrity Enforcement:
Foreign key constraints are enabled during normal DW operation, ensuring fact records reference valid dimension records. Constraints are temporarily disabled during loading operations to enable safe bulk deletion and reinsertion.

### 5.6 Data Warehouse Loading

Loading Trigger:
Data Warehouse loading occurs only after the prepared data validation gate (validate_prepared) successfully completes. Non-conforming integrated datasets are rejected before reaching the warehouse, preventing corrupted records from polluting the analytical layer.

Defensive String Truncation:
Before invoking Pandas .to_sql() to load the dim_tracks table, the pipeline applies defensive string truncation to the track_name column to prevent MySQL Error 1406 (Data too long for column):

    Truncate track_name to 255 characters (VARCHAR(255) column limit) df_tracks['track_name'] = df_tracks['track_name'].astype(str).str[:255]

**Idempotent Loading:**
The pipeline implements an idempotent loading strategy using TRUNCATE TABLE combined with if_exists='append', preserving the strict DDL schema (Primary/Foreign Keys) while preventing duplicate entry errors (MySQL Error 1062) during DAG reruns:
```sql
# Disable foreign key checks to permit TRUNCATE
connection.execute("SET FOREIGN_KEY_CHECKS = 0;")
# Clear existing data safely
connection.execute("TRUNCATE TABLE fact_track_performance;")
connection.execute("TRUNCATE TABLE dim_artists;")
connection.execute("TRUNCATE TABLE dim_tracks;")
# Re-enable constraints
connection.execute("SET FOREIGN_KEY_CHECKS = 1;")
# Insert dimensions first (to establish PK values)
df_artists.to_sql('dim_artists', con=mysql_engine, if_exists='append', index=False)
df_tracks.to_sql('dim_tracks', con=mysql_engine, if_exists='append', index=False)
# Insert facts (referential integrity enforced)
df_facts.to_sql('fact_track_performance', con=mysql_engine, if_exists='append', index=False)
```
Load Sequence:
Dimension tables are loaded before the fact table. This sequence ensures referential integrity constraints are satisfied during the final fact table load.

### 5.7 KPI Definitions and Analytical Queries

**KPI 1: Catalog Performance Baseline**
```sql
SELECT 
    a.grammy_winner_flag, 
    AVG(f.popularity_score) AS avg_catalog_popularity,
    STDDEV(f.popularity_score) AS std_catalog_popularity,
    COUNT(f.track_id) AS total_tracks
FROM fact_track_performance f
JOIN dim_artists a ON f.artist_id = a.artist_id
GROUP BY a.grammy_winner_flag;
```
Measures the correlation between Grammy-winning status and baseline catalog popularity. Groups all tracks by artist Grammy status, computing mean and standard deviation of popularity within each cohort. Supports hypothesis that Grammy winners enjoy elevated baseline popularity across their entire discography (halo effect).

**KPI 2: Acoustic Profile of Award-Winning Tracks** 
```sql
SELECT 
    a.grammy_winner_flag,
    AVG(t.danceability) AS avg_danceability,
    AVG(t.valence) AS avg_valence,
    AVG(t.duration_ms) / 1000 AS avg_duration_seconds
FROM fact_track_performance f
JOIN dim_artists a ON f.artist_id = a.artist_id
JOIN dim_tracks t ON f.track_id = t.track_id
GROUP BY a.grammy_winner_flag;
```
Identifies the statistical acoustic signature of Grammy-winning artists' tracks. Compares mean danceability, valence, and duration between Grammy-winning and non-winning artist cohorts. Enables identification of consistent acoustic patterns associated with award recognition.

**KPI 3: Popularity by Acoustic Quartile and Grammy Status**
```sql
SELECT 
    t.acoustic_quartile,
    a.grammy_winner_flag,
    AVG(f.popularity_score) AS avg_popularity,
    COUNT(f.track_id) AS track_count
FROM fact_track_performance f
JOIN dim_tracks t ON f.track_id = t.track_id
JOIN dim_artists a ON f.artist_id = a.artist_id
GROUP BY t.acoustic_quartile, a.grammy_winner_flag
ORDER BY t.acoustic_quartile, a.grammy_winner_flag;
```
Isolates the impact of Grammy status on popularity, controlling for acoustic product quality. Cross-tabulates acoustic engagement level (High/Medium/Low) with Grammy winner status, measuring average popularity within each cohort combination. Enables quantification of the "Grammy label" effect independent of intrinsic track audio characteristics.

### 5.8 Dashboard and Analytical Outputs
![Dashboard evidence](docs/sources/dashboard_evidence.png)
#### 5.8.1 AR1: Catalog Performance Baseline Findings

Key Metric: Average popularity of Grammy-winning vs. non-winning artists on Spotify.

Observed Results:
* **Grammy Winners:** ~31 average popularity score.
* **Non-Winners:** ~32 average popularity score.
* **Difference:** Statistically insignificant (< 1 point).

Interpretation:
Contrary to the initial hypothesis that Grammy status establishes a elevated commercial baseline across an artist's entire catalog, the empirical data reveals that Grammy-winning and non-winning artists exhibit virtually identical baseline popularity on Spotify. This indicates that:
1. Grammy award status is **not** a predictive factor for baseline popularity on modern digital streaming platforms.
2. The expected "Halo Effect" does not materialize as a distinct visibility advantage on Spotify.
3. Other factors—such as catalog recency, recommendation algorithms, and editorial playlist placement—likely dictate popularity metrics over historical institutional recognition.

Data Limitation: Both cohorts are pre-filtered to artists present on Spotify, excluding obscure artists who never achieved digital distribution. The comparison is strictly between digital-native winning artists and digital-native non-winning artists, rather than winners versus individuals completely absent from streaming platforms.

#### 5.9.2 AR2: Acoustic Profile Analysis Findings

Key Metrics: Danceability, Valence, and Duration (in milliseconds).

Observed Results:

| Metric | Grammy Winners | Non-Winners |
| :--- | :--- | :--- |
| **Danceability** | ~0.62 | ~0.59 |
| **Valence** | ~0.49 | ~0.48 |
| **Duration (ms)** | ~213K | ~198K |

Interpretation:
The acoustic profiles of winning and non-winning artists are statistically indistinguishable. Both groups exhibit:
1. **Moderate Danceability (0.59–0.62):** Tracks positioned in a balanced, non-extreme dance range.
2. **Slightly Melancholic Valence (0.48–0.49):** Compositions maintaining a neutral-to-negative emotional tone.
3. **Standard Track Duration (198,000–213,000 ms):** Consistent with standard commercial production norms (3 to 3.5 minutes).

Critical Finding: The absence of acoustic divergence indicates that Grammy Award selection criteria do not correlate with specific audio attributes. Institutional bodies recognize artists for broader achievements (commercial reach, artistic merit, critical acclaim) that operate independently from intrinsic audio track properties.

Implication for AR2: While the original analytical scope sought to identify an optimal acoustic signature for award-winning tracks, empirical results demonstrate that no universal acoustic blueprint exists. The diversity of genres, styles, and sonic approaches among winners reflects the multi-dimensional nature of award evaluations.

#### 5.9.3 AR3: Moral Luck Bias Findings (Primary Analytical Result)

Original Hypothesis: Tracks with low intrinsic acoustic engagement (low valence combined with low danceability) achieve disproportionately high popularity solely due to their association with Grammy-winning artists (the "moral luck" effect).

Observed Results:

| Acoustic Quartile | Grammy Winners | Non-Winners | Difference |
| :--- | :--- | :--- | :--- |
| **High Engagement** | ~20 | ~20 | $\approx$ 0 |
| **Medium Engagement** | ~20 | ~20 | $\approx$ 0 |
| **Low Engagement** | ~20 | ~20 | $\approx$ 0 |

Heatmap Visual: Uniform metric values across all matrix cells, reflecting identical popularity scores (~20).

Interpretation — Hypothesis Rejection:
1. **Absence of Moral Luck Bias:** Commercial popularity is not elevated by the Grammy label alone. Tracks with lower acoustic engagement ratings (Low Engagement) maintain the same popularity average (~20) regardless of the artist's Grammy status.
2. **Confounding Variables and Reverse Causality:** The initial assumption posed a direct causal path from Grammy status to popularity. The data suggests alternative mechanisms:
   * *Alternative Hypothesis A:* Grammy-winning artists are evaluated on holistic commercial, artistic, or cultural impact rather than specific acoustic dimensions.
   * *Alternative Hypothesis B:* Spotify platform popularity is dictated primarily by real-time engagement mechanics (algorithmic playlists, contemporary user trends) that operate independently of historical institutional awards.
3. **Theoretical Implication:** Empirical results refute the assumption that external institutional validation alters consumer listening behavior at scale. Instead, popularity metrics operate meritocratically within the platform, independent of external accolades.

#### Integrated Analytical Summary

| Requirement | Original Hypothesis | Empirical Result | Analytical Interpretation |
| :--- | :--- | :--- | :--- |
| **AR1** | Grammy status creates an elevated catalog baseline. | Rejected (~31 vs. ~32) | Grammy status is not predictive of baseline catalog popularity. |
| **AR2** | Grammy winners share an optimal acoustic profile. | Rejected (0.62 vs. 0.59 danceability) | Grammy selection criteria are orthogonal to specific audio features. |
| **AR3** | Grammy status artificially inflates track popularity. | Rejected (identical ~20 across all quartiles) | Platform engagement operates independently of external institutional labels. |

#### Architectural and Analytical Conclusions

The integrated analysis demonstrates that Spotify popularity metrics function independently from historical Grammy Award status. Neither institutional recognition elevates overall catalog baselines nor inflates listener engagement for tracks with lower acoustic appeal. These findings confirm that modern digital distribution systems—driven by algorithmic recommendations and editorial playlisting—follow operational dynamics distinct from traditional institutional validation. While a Grammy Award confers critical acclaim and industry prestige, it does not guarantee elevated commercial performance on streaming platforms, demonstrating the boundary between historical credentials and digital-era consumer engagement.


### 5.9 Requirement-to-Analytics Traceability

Every KPI and analytical output displayed in the BI dashboard follows a complete, auditable lineage from raw data extraction through dimensional warehouse loading:
1. Raw Extraction: Spotify CSV and Grammy database query produce raw datasets.
2. Raw Validation: validate_spotify_raw and validate_grammys_raw Great Expectations suites enforce schema, completeness, and validity rules (R1–R4).
3. Transformation & Integration: Null dropping, delimiter explosion, deduplication, and left join produce integrated dataset.
4. Prepared Validation: validate_prepared Great Expectations suite enforces business logic and KPI-readiness rules (R5–R6).
5. Idempotent Loading: Validated integrated data is loaded into MySQL dimensional schema with defensive string truncation.
6. BI Dashboard Consumption: BI tool queries only the loaded dimensional tables, ensuring all visualizations reflect validated data.

This complete lineage enables traceability of any KPI metric back to source-level validation rules and transformation logic, supporting audit requirements and enabling root cause analysis of analytical anomalies.

## Section 6: Orchestration and Reliability — Apache Airflow

### 6.1 Airflow Environment

The pipeline is orchestrated using Apache Airflow v2.8.1 deployed in a standalone Dockerized container environment. The Airflow deployment manages task execution, dependency resolution, failure recovery, and observability across the entire ELT workflow.

Environment Configuration:
* **Deployment Model:** Standalone Dockerized Apache Airflow v2.8.1.
* **Scheduler:** Single-node scheduler executing scheduled DAG runs on a 24-hour interval.
* **Executor:** `SequentialExecutor` (single-threaded task execution within Airflow container).
* **Storage:** SQLite backend for metadata and DAG state persistence.
* **External Connectivity:** Database connections to `mysql-source` (Grammy Awards relational source) and `mysql-dw` (Data Warehouse) via network-accessible container endpoints.

Runtime Dependencies:
Critical libraries are explicitly provisioned within the Airflow container to support pipeline operations:
* `pandas` (data manipulation and transformation).
* `great_expectations` (data quality validation framework and checkpoint execution).
* `sqlalchemy` (relational database connectivity and extraction).
* `mysql-connector-python` (MySQL database driver).

These dependencies are installed directly into the running container via `pip` to ensure all Python operators and `BashOperator` subprocess calls have access to required modules.

### 6.2 DAG Design

DAG Identification:
* DAG ID: `reliable_music_pipeline`
* Schedule Interval: `@daily` (24-hour execution cadence)
* Catchup Policy: `catchup = False` (prevents historical backfill execution; each run processes only current incremental data)

Architectural Approach:
The pipeline implements a modular task architecture separating orchestration from execution logic. The DAG itself functions as a thin orchestration layer, delegating data operations to external Python modules:
* `src/extract.py`: Handles all source data extraction (Spotify CSV files, Grammy MySQL relational queries).
* `src/transform.py`: Executes data cleaning, standardization, integration logic, and dimensional modeling.
* `src/load.py`: Implements idempotent warehouse loading with defensive truncation.

This modular design decouples DAG definition from business logic, enabling:
* **Testability:** Each module can be unit tested independently.
* **Maintainability:** Logic changes do not require DAG code modifications.
* **Reusability:** Modules can be invoked from multiple DAGs or batch processes.

Task Operator Strategy:
* **PythonOperator:** Used for data extraction, transformation, and loading tasks (`extract_spotify`, `extract_grammys`, `transform_and_integrate`, `load_to_dw`).
* **BashOperator:** Used for Great Expectations checkpoint execution (`validate_spotify_raw`, `validate_grammys_raw`, `validate_prepared`).

### 6.3 Task Dependencies

The pipeline enforces a strict execution graph ensuring data quality validation before integration and loading:

extract_spotify >> validate_spotify_raw --┐
                                           ├──> transform_and_integrate >> validate_prepared >> load_to_dw
extract_grammys >> validate_grammys_raw --┘

Execution Sequence:
* **Phase 1: Parallel Extraction**
  * `extract_spotify` (`PythonOperator`): Reads Spotify CSV from local filesystem via `src/extract.py`, materializing raw track records in memory.
  * `extract_grammys` (`PythonOperator`): Establishes SQLAlchemy connection to `mysql-source` container, executes parameterized query extracting Grammy nomination/winner records directly from relational database.
  * Both extraction tasks execute concurrently, independent of each other. Extraction logic is delegated to `src/extract.py`, enabling separation of extraction concerns from DAG definition.
* **Phase 2: Parallel Raw Validation**
  * `validate_spotify_raw` (`BashOperator`): Invokes `test_checkpoint.py` Great Expectations checkpoint, enforcing schema conformance, null checks, and range validation on raw Spotify data (R1, R2, R3).
  * `validate_grammys_raw` (`BashOperator`): Invokes `test_checkpoint_grammys.py` Great Expectations checkpoint, enforcing domain validation on raw Grammy data (R4).
  * Both validation tasks execute concurrently. Non-zero exit codes trigger task failure and downstream circuit-breaking.
* **Phase 3: Convergence and Transformation**
  * `transform_and_integrate` (`PythonOperator`): Triggered only after both raw validation tasks complete successfully. Delegates to `src/transform.py`:
    * Loads cleaned Spotify and Grammy datasets from intermediate storage.
    * Applies left outer join on normalized `artist_clean` key.
    * Imputes Grammy status flags (`False` for unmatched artists).
    * Engineers acoustic quartile metrics for AR3 analysis.
    * Materializes integrated DataFrame.
* **Phase 4: Prepared Validation**
  * `validate_prepared` (`BashOperator`): Invokes `test_checkpoint_prepared.py` Great Expectations checkpoint, enforcing business logic rules on integrated dataset (R5, R6).
* **Phase 5: Warehouse Load**
  * `load_to_dw` (`PythonOperator`): Triggered only after prepared validation succeeds. Delegates to `src/load.py`:
    * Applies idempotent truncation strategy.
    * Loads dimension tables (`dim_artists`, `dim_tracks`).
    * Loads fact table (`fact_track_performance`).
    * Enforces referential integrity constraints.

This linear-then-parallel-then-linear topology optimizes execution time while maintaining strict quality enforcement at each stage.

### 6.4 Raw Validation Gates

Validation Execution Model:
Raw data validation occurs immediately after source extraction, before any transformation logic. Great Expectations checkpoints are invoked via `BashOperator` tasks, executing as subprocess commands within the Airflow container.

Spotify Raw Validation (`validate_spotify_raw`):
Invokes `test_checkpoint.py` Great Expectations checkpoint suite, enforcing rules R1–R3:
* `expect_column_values_to_be_between` (`popularity`: 0–100): Ensures popularity scores conform to theoretical [0, 100] range.
* `expect_column_values_to_be_between` (`duration_ms`: 1000–3661000): Rejects tracks with zero or anomalously short duration (< 1000 ms indicates data corruption).
* `expect_column_values_to_not_be_null` (`artists`): Enforces that artist attribution is present before explosion.

Grammy Raw Validation (`validate_grammys_raw`):
Invokes `test_checkpoint_grammys.py` Great Expectations checkpoint suite, enforcing rule R4:
* `expect_column_values_to_be_in_set` (`winner`: `['True']`): Documents that Grammy source contains only winner records (selection bias; no negative cases).

Failure Behavior:
Non-zero exit codes from checkpoint scripts trigger immediate `BashOperator` task failure. Airflow's dependency graph automatically halts all downstream tasks, preventing non-validated data from entering transformation logic.

### 6.5 Prepared Data Validation

Before warehouse loading, the integrated dataset passes through a final Great Expectations validation gate (`validate_prepared`). This gate enforces business logic rules and KPI-readiness validation.

Validation Execution:
The `validate_prepared` task invokes `test_checkpoint_prepared.py` via `BashOperator`, executing as a subprocess within the Airflow container.

Enforced Expectations:
* **Grammy Winner Flag Validation:**
  * `expect_column_values_to_not_be_null` on the `grammy_winner_flag` column (no NULL values permitted post-imputation).
  * `expect_column_values_to_be_of_type` enforcing strict Boolean type (`True`/`False` only; rejects numeric 0/1 or string representations).
* **Acoustic Quartile Validation:**
  * `expect_column_values_to_be_in_set` enforcing the `acoustic_quartile` categorical domain strictly to: `['High Engagement', 'Medium Engagement', 'Low Engagement']`.
  * This engineered metric (derived for AR3 analysis) segments tracks by combined danceability and valence percentiles; the validation ensures no spurious or out-of-domain values reach the warehouse.

Failure Behavior:
Records failing either expectation trigger pipeline halting. Detailed validation reports are logged to facilitate root cause analysis before resumption. Non-zero exit codes prevent `load_to_dw` from executing.

### 6.6 Failure and Retry Policy

Retry Policy:
Default arguments enforce strict failure recovery:
* Max Retries: 1 (single automatic retry upon transient task failure)
* Retry Delay: 300 seconds (5 minutes; allows temporary network/database connection issues to resolve)
* Retry Scope: Transient failures only (connection timeouts, temporary service unavailability, ephemeral database locks)

Retry Trigger Examples:
* Temporary MySQL connection timeout to `mysql-source` during extraction.
* Ephemeral network latency causing Great Expectations checkpoint timeout.
* Temporary unavailability of Airflow's SQLite metadata backend.

Non-Retryable Failures:
Persistent errors fail immediately after the single retry, enabling rapid incident detection:
* Schema violations detected by raw validation gates (data quality violations).
* Missing data sources (CSV files not found, Grammy database unreachable after retry).
* Authentication failures (incorrect database credentials).
* Referential integrity violations (non-existent foreign keys in fact table load).

This conservative retry policy balances operational resilience with fast failure feedback, preventing prolonged retry loops that mask systemic issues.

Circuit-Breaker Effect:
When a task fails permanently (after retries), Airflow's dependency graph automatically marks all downstream tasks as skipped or `upstream_failed`, preventing corrupted data from propagating to the Data Warehouse. The pipeline halts in a known-good state, preserving analytical integrity.

### 6.7 Logging and Observability

Task-Level Logging:
Each Airflow task generates detailed logs accessible via:
* Airflow UI task logs view.
* Container STDOUT (via `docker logs workshop_2-airflow-webserver-1`).
* Log aggregation tools (if configured).

Python Operator Logging:
Extraction, transformation, and load operations (via `src/extract.py`, `src/transform.py`, `src/load.py`) emit structured logs to STDOUT:
* Data source connectivity: `"Connecting to mysql-source..."`
* Record counts: `"Extracted 114,000 Spotify tracks"`
* Transformation milestones: `"Exploded artists column: 115,842 rows"`
* Load completion: `"Loaded 89,740 dim_artists records"`

Great Expectations Checkpoint Logging:
`BashOperator` tasks capture Great Expectations checkpoint output:
* Validation results summary (passed/failed expectations).
* Detailed failure reports identifying specific rows violating quality rules.
* Exit codes (0 = all expectations passed; 1 = violations detected).

Failure Diagnostics:
When tasks fail, logs capture:
* Stack traces (Python exceptions).
* Database error messages (MySQL connection errors, constraint violations).
* Great Expectations validation failure details (specific rows, expected vs. actual values).

### 6.8 Repeatability and Safe Rerun Strategy

Idempotent Loading Design:
The pipeline implements idempotent warehouse loading to support safe, repeatable DAG reruns without manual data cleanup. This addresses the critical issue of MySQL Error 1062 (Duplicate Entry on primary key) that can occur when Airflow's automatic retry mechanism reruns failed load tasks.

Safe Rerun Mechanism:
The `load_to_dw` task (delegated to `src/load.py`) implements defensive truncation before bulk insert:
```python
import pandas as pd
from sqlalchemy import create_engine, text
def  load_data_to_dw():
print("Iniciando carga al Data Warehouse Dimensional...")
df = pd.read_csv('data/integrated_data_prepared.csv')
engine = create_engine('mysql+mysqlconnector://root:root@mysql-dw:3306/music_dw')
with engine.begin() as conn:
conn.execute(text("SET FOREIGN_KEY_CHECKS = 0;"))
conn.execute(text("TRUNCATE TABLE fact_track_performance;"))
conn.execute(text("TRUNCATE TABLE dim_tracks;"))
conn.execute(text("TRUNCATE TABLE dim_artists;"))
conn.execute(text("SET FOREIGN_KEY_CHECKS = 1;"))
dim_artists = df[['artist_id', 'artist_name', 'grammy_winner_flag']].drop_duplicates()
dim_artists.to_sql('dim_artists', con=engine, if_exists='append', index=False)
dim_tracks = df[['track_id', 'track_name', 'acoustic_quartile', 'danceability', 'valence', 'duration_ms']].drop_duplicates()
dim_tracks['track_name'] = dim_tracks['track_name'].astype(str).str[:255]
dim_tracks.to_sql('dim_tracks', con=engine, if_exists='append', index=False)
fact_performance = df[['track_id', 'artist_id', 'popularity_score']]
fact_performance.to_sql('fact_track_performance', con=engine, if_exists='append', index=False)
print("Carga dimensional completada exitosamente.")
 ```
 Why Idempotence is Critical:
* **Scenario 1:** The `load_to_dw` task inserts 100,000 fact records. Network interruption occurs after 50,000 records are loaded. Airflow's automatic retry mechanism reruns the task.
  * *Without idempotence:* TRUNCATE fails because foreign key checks are still enabled; subsequent insert generates duplicate key errors for the first 50,000 records.
  * *With idempotence:* TRUNCATE clears all prior records; INSERT proceeds without duplicate key violations.
* **Scenario 2:** A DAG is manually triggered (e.g., `airflow dags test`) to reprocess a specific date's data.
  * *Without idempotence:* Duplicate records accumulate in the warehouse, corrupting aggregate KPI calculations.
  * *With idempotence:* Warehouse is cleared before reload; final state is identical to a single execution.

Defensive String Truncation:
Prior to `.to_sql()` invocation, the load script applies defensive truncation to the `track_name` column to prevent MySQL Error 1406 (Data too long for column):
```python 
df_tracks['track_name'] = df_tracks['track_name'].astype(str).str[:255]
```
This ensures anomalously long track names (> 255 characters) conform to the `VARCHAR(255)` column constraint, preventing load failures mid-insertion.

Testing Safe Reruns:
The DAG's rerun safety was validated by intentionally triggering failures during load and verifying successful recovery:
```bash 
# Manually trigger DAG airflow dags test 
reliable_music_pipeline 2026-10-10 
# Simulate failure (manually kill load_to_dw task midway) 
# Automatic retry executes 
TRUNCATE + INSERT without duplicate key errors
```
Result: Warehouse reaches consistent final state without manual intervention, confirming idempotent loading strategy effectiveness.


# 7. Execution and Reliability Evidence

### 7.1 Reliability Evidence Register

Every reliability mechanism, quality gate, and fault-tolerance policy implemented in the pipeline is supported by machine-readable logs, Airflow execution states, and relational Data Warehouse audit checks:

| Evidence ID | Run / Task Scope | Artifact / Image Path | What It Proves | Related Policy / Rule |
| :--- | :--- | :--- | :--- | :--- |
| EV-01 | Full DAG Run | `docs\evidence\All_success.png` | Nominal end-to-end execution across all 5 architecture stages | Nominal Pipeline Flow |
| EV-02 | `validate_spotify_raw` | `docs\evidence\critical_failure.png` | Circuit-breaker halting downstream tasks upon Critical quality failure | R1 (`popularity` range check) |
| EV-03 | `load_dw` / MySQL | `docs\evidence\safe_rerun.png` | Idempotent safe-rerun via table truncation and row-count reconciliation | Repeatability & Safe Rerun Policy |

### 7.2 Successful Execution Evidence

Under nominal execution conditions with valid source inputs, the pipeline completes all 5 architectural phases sequentially without task failures.

![Successful Execution](docs/evidence/All_success.png)

*   **DAG Run State:** `success`
*   **Total Run Duration:** 00:02:49
*   **Execution Behavior:** Parallel extraction tasks (`extract_spotify`, `extract_grammys`) and raw validation gates (`validate_spotify_raw`, `validate_grammys_raw`) completed successfully. The transformation task `transform_and_integrate` executed the `explode` and Left Join operations, passed the `validate_prepared` gate, and completed the Data Warehouse load via `load_dw`.

### 7.3 Controlled Critical Failure Evidence

To verify the circuit-breaker mechanism, an intentional data quality violation was injected into the raw Spotify dataset by introducing an out-of-range popularity value (`popularity = 999`), violating Rule **R1** (`expect_column_values_to_be_between`).

![Controlled Critical Failure](docs/evidence/critical_failure.png)

*   **Failed Task:** `validate_spotify_raw` marked as `failed` with exit code `1`.
*   **Log Details:** Great Expectations raised a validation error reporting `partial_unexpected_list: [999]` for the `popularity` column.
*   **Circuit-Breaker Behavior:** Airflow automatically halted all downstream tasks. Tasks `transform_and_integrate`, `validate_prepared`, and `load_dw` were blocked (`0 downstream tasks scheduled`), ensuring corrupted data was isolated at the ingestion boundary and never reached the Data Warehouse.

### 7.4 Selective Retry Evidence

The pipeline implements a differentiated failure recovery policy configured in `default_args`:
*   `retries = 1` with `retry_delay = 300s` is applied strictly to transient operational failures (e.g., temporary database connection timeouts or container initialization delays).
*   Data quality validation violations evaluated by Great Expectations exit deterministically with non-zero status codes, causing an immediate task halt without triggering unnecessary retry loops on unrecoverable data errors.

### 7.5 Safe Rerun Evidence

To guarantee safe reruns without creating duplicate records or polluting historical metrics, the Data Warehouse loading process implements an **idempotent replace strategy** (truncate-and-load) enforced via foreign key constraints management.

![Safe Rerun Evidence](docs/evidence/safe_rerun.png)

*   **Executed DDL Operations:**
    ```sql
    SET FOREIGN_KEY_CHECKS = 0;
    TRUNCATE TABLE fact_track_performance;
    TRUNCATE TABLE dim_tracks;
    TRUNCATE TABLE dim_artists;
    SET FOREIGN_KEY_CHECKS = 1;
    ```
*   **Row-Count Reconciliation:** Re-executing the complete DAG against the target MySQL instance produces an exact, consistent count of **158,292 records** in `fact_track_performance`, verifying that repeated runs yield identical, uncorrupted target state.

### 7.6 Evidence Interpretation

The empirical evidence confirms that the pipeline fulfills all core reliability requirements:
1. Valid data flows seamlessly from source ingestion through transformation to the analytical Data Warehouse.
2. Injected data corruption triggers an immediate circuit-breaker stop, isolating bad records at the raw validation layer.
3. Re-execution guarantees strict idempotency, preserving Data Warehouse integrity across repeated DAG runs.

  

# 8. Reproducibility

### 8.1 Repository Structure

The repository enforces separation of concerns between orchestration, ETL transformation logic, data quality checkpoints, relational schemas, and analytical outputs:

```t
workshop-2/
├── dags/
│   └── reliable_music_pipeline.py
├── data/
│   ├── grammy_extracted.csv
│   ├── grammy.csv
│   ├── integrated_data_prepared.csv
│   └── spotify.csv
├── docs/
│   ├── dashboard/
│   ├── evidence/
│   └── sources/
├── gx/
│   ├── checkpoints/
│   ├── expectations/
│   ├── plugins/
│   ├── profilers/
│   ├── uncommitted/
│   ├── .gitignore
│   └── great_expectations.yml
├── notebooks/
│   └── data_profiling.ipynb
├── scripts/
│   ├── setup_gx.py
│   ├── setup_gx_grammys.py
│   ├── setup_gx_prepared.py
│   ├── test_checkpoint.py
│   ├── test_checkpoint_grammys.py
│   └── test_checkpoint_prepared.py
├── sql/
│   ├── dw_schema.sql
│   └── source_setup.sql
├── src/
│   ├── extract.py
│   ├── load.py
│   └── transform.py
├── .env
├── .gitignore
├── docker-compose.yaml
├── requirements.txt
└── run_transform.py

```
### 8.2 Environment and Dependencies

-   **Execution Runtime:** Python 3.8+

-   **Orchestration Engine:** Apache Airflow v2.8.1 (Standalone in Docker)

-   **Database Engine:** MySQL 8.0 (Containerized)
    
-   **Core Libraries:** `pandas`, `great_expectations`, `mysql-connector-python`, `sqlalchemy`
 
-   **Analytics & BI:** Tableau Desktop

Dependencies are defined in `requirements.txt` and hot-injected into the Airflow container runtime during environment startup to support external checkpoint execution.

### 8.3 Configuration and Environment Variables

System credentials and container port mappings are managed via environment variables. Copy `.env.example` to `.env` before starting containers:


``` sql
# Database Configuration
MYSQL_HOST=localhost
MYSQL_PORT=3007
MYSQL_ROOT_PASSWORD=root
MYSQL_USER=airflow
MYSQL_PASSWORD=airflow

# Databases
SOURCE_DB=grammy_source
TARGET_DB=music_dw

# Airflow Configuration
AIRFLOW_UI_PORT=8080

```
### 8.4 Setup Instructions

1.  **Clone the Repository:**
    ```
    git clone <repository_url>
    cd workshop-2
    
    ```    
2.  **Environment Variables:**

    ```
    cp .env.example .env
    
    ```
3.  **Launch Containerized Services:**

    ```
    docker-compose up -d
    
    ```
4.  **Provision Airflow Runtime Dependencies:**
    ```
    docker exec -u airflow -it workshop_2-airflow-webserver-1 pip install pandas great_expectations mysql-connector-python sqlalchemy
    
    ```

### 8.5 Source Database Initialization

The transactional source database (`grammy_source`) is automatically provisioned upon MySQL container startup. The initialization script mounted from `sql/source_setup.sql` creates the target database, sets up table schemas, and executes a `LOAD DATA INFILE` command importing `data/grammy.csv` into the `grammy_source` relational database.

### 8.6 Great Expectations Setup

Validation suites and checkpoints are configured under the `gx/` directory structure. To re-initialize or verify the Great Expectations Data Context and checkpoints manually, execute:

```
python scripts/setup_gx.py
python scripts/setup_gx_grammys.py
python scripts/setup_gx_prepared.py

```
To test individual validation gates directly from the CLI:
```
python scripts/test_checkpoint.py
python scripts/test_checkpoint_grammys.py
python scripts/test_checkpoint_prepared.py

```
### 8.7 Airflow Execution

1.  Access the Airflow Web UI at `http://localhost:8080`.
    
2.  Enable and trigger the DAG `reliable_music_pipeline`.

3.  To run a CLI test execution for a specific batch date:
    ```
    docker exec -u airflow -it workshop_2-airflow-webserver-1 airflow dags test reliable_music_pipeline 2026-10-10
    
    ```
### 8.8 Data Warehouse Initialization
The analytical target database (`music_dw`) is initialized by running the DDL script against the containerized MySQL instance on port `3007`:
Bash
```
mysql -h 127.0.0.1 -P 3007 -u root -p music_dw < sql/dw_schema.sql
```
This constructs the star schema, establishing `dim_artists`, `dim_tracks`, and `fact_track_performance` with proper foreign key constraints.
### 8.9 Dashboard Connection
The Tableau workbook located in `docs/dashboard/` connects directly to the Data Warehouse:
-   **Connection Type:** MySQL Native / ODBC Connector
-   **Server Host:** `localhost` (Port `3007`)
-   **Database:** `music_dw`   
-   **Data Model:** Star Schema (`fact_track_performance` joined to `dim_artists` and `dim_tracks`)
Direct CSV connections are strictly prohibited; Tableau queries live Data Warehouse tables to populate all KPIs and visual artifacts.
### 8.10 Assumptions and Limitations
1.  **Batch Ingestion Scope:** The pipeline is designed strictly for daily batch processing. Streaming or near-real-time ingestion is out of scope.
2.  **String Normalization Boundary:** Artist matching relies on case-insensitive exact string equality post-split (`split(';')`). Advanced fuzzy string matching or NLP-based entity resolution is omitted to preserve deterministic join behavior.
3.  **Local Virtualization:** Relational source and target databases run locally in Docker containers mapped to host port `3007`. Production deployment would require remote cloud database endpoints (e.g., AWS RDS).
