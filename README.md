# Supply Chain Optimizer

An AI-powered supply chain risk and disruption analysis tool built on Databricks. Uses a multi-agent architecture to answer natural-language questions about supplier risk, part availability, shipment disruptions, and BOM dependencies — routing intelligently between SQL (Delta Lake) and graph (Neo4j AuraDB) depending on the question type.

---

## Architecture

![Architecture](docs/architecture.png)

### Data Flow

```
Synthetic Data (Faker + Spark)
        ↓
/Volumes/supplychain/supply_chain_raw/landing/
        ↓
Lakeflow Spark Declarative Pipeline (Serverless)
        ↓
supplychain.supply_chain_medallion
  Bronze (6 streaming tables)  →  Silver (6 streaming tables)  →  Gold (5 materialized views)
        ↓                                                               ↓
  Raw ingestion                                              Agent Layer reads here
  Auto Loader from volume                                    + Neo4j subgraph projection
```

### Agent Layer

```
User Question
      ↓
  Router Agent (Claude Opus 4.6)
      ├── route_to_sql  →  SQL Agent  →  spark.sql() on gold tables  →  Answer
      └── route_to_graph → Graph Agent → project subgraph → Neo4j Cypher → Answer
                                               ↓
                                     Delta answer_cache (TTL 24h)
```

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Data platform | Databricks (Serverless) |
| Storage | Delta Lake / Unity Catalog |
| Pipeline | Lakeflow Spark Declarative Pipelines (SQL) |
| Agent framework | Claude Opus 4.6 (Anthropic SDK) — tool use + adaptive thinking |
| Graph database | Neo4j AuraDB |
| Orchestration | Databricks Asset Bundles (DAB) |
| Data generation | Spark + Faker + Pandas UDFs |

---

## Project Structure

```
supply-chain-optimizer/
├── README.md
├── databricks.yml                          # Databricks Asset Bundle config
├── requirements.txt                        # anthropic, neo4j, databricks-sdk
├── .env                                    # Credentials (gitignored)
│
├── resources/
│   └── supply_chain_pipeline.pipeline.yml  # SDP pipeline resource definition
│
├── data_gen/
│   ├── generate_supply_chain_data.py       # Local Spark data generator
│   └── generate_supply_chain_data_notebook.py  # Databricks notebook version
│
├── src/supply_chain_pipeline/transformations/
│   ├── bronze_suppliers.sql                # Auto Loader → bronze streaming tables
│   ├── bronze_parts.sql
│   ├── bronze_facilities.sql
│   ├── bronze_bom.sql
│   ├── bronze_purchase_orders.sql
│   ├── bronze_shipments.sql
│   ├── silver_suppliers.sql                # Typed + DQ constraints
│   ├── silver_parts.sql
│   ├── silver_facilities.sql
│   ├── silver_bom.sql
│   ├── silver_purchase_orders.sql
│   ├── silver_shipments.sql
│   ├── gold_supplier_risk.sql              # Composite risk score 0-100
│   ├── gold_part_availability.sql          # Stock status per part/facility
│   ├── gold_active_purchase_orders.sql     # Open/delayed POs with aging
│   ├── gold_shipment_pipeline.sql          # In-transit shipments + disruption severity
│   └── gold_bom_explosion.sql              # 2-level BOM with rolled-up cost
│
├── agents/
│   ├── config.py                           # All env vars and table names
│   ├── cache.py                            # Delta answer cache (local Python)
│   ├── prompts.py                          # System prompts + tool schemas
│   ├── router.py                           # Claude router agent (local Python)
│   ├── sql_agent.py                        # SQL agent (local Python)
│   ├── graph_agent.py                      # Graph agent (local Python)
│   └── supply_chain_agent_notebook.py      # All-in-one Databricks notebook
│
├── neo4j_graph/
│   ├── connector.py                        # Neo4j driver + subgraph projectors
│   └── queries.py                          # Pre-built Cypher query library
│
└── main.py                                 # Local CLI entry point
```

---

## Data Model

### Synthetic Domain Entities

| Entity | Count | Key Fields |
|--------|-------|-----------|
| Suppliers | 200 | `SUP-XXXXX`, tier (Tier-1/2/3), reliability_score, country |
| Parts | 500 | `PRT-XXXXX`, category (Raw Material / Sub-Assembly / Component), is_critical |
| Facilities | 50 | `FAC-XXXXX`, facility_type (Manufacturing / Assembly / Warehouse), region |
| BOM | ~2,000 | `BOM-XXXXX`, parent→child part relationships |
| Purchase Orders | 8,000 | `PO-XXXXXXX`, status (Open / In-Transit / Received / Delayed / Cancelled) |
| Shipments | ~12,000 | `SHP-XXXXXXXX`, carrier (5 carriers, Pareto dist), delay_days |

### Gold Tables

| Table | Description |
|-------|-------------|
| `gold_supplier_risk` | Composite risk score (reliability 40% + PO problem rate 35% + shipment issues 25%), risk_tier |
| `gold_part_availability` | Ordered vs received qty per part/facility, stock_status |
| `gold_active_purchase_orders` | Open/delayed POs with aging buckets and exposure_score |
| `gold_shipment_pipeline` | In-transit/delayed shipments with disruption_severity and route_key |
| `gold_bom_explosion` | 2-level BOM flattening with cumulative_quantity and rolled_up_cost_usd |

### Neo4j Graph Schema

```
(:Supplier)-[:SUPPLIES {po_count, avg_delay_days}]->(:Part)
(:Part)-[:REQUIRES {quantity, depth}]->(:Part)
(:Facility)-[:SHIPS_TO {carrier, route_key}]->(:Facility)
(:Shipment)-[:DEPARTS_FROM]->(:Facility)
(:Shipment)-[:ARRIVES_AT]->(:Facility)
```

---

## Setup

### Prerequisites

- Databricks workspace (serverless enabled)
- Neo4j AuraDB instance
- Anthropic API key (Default workspace)

### 1. Store Secrets

```bash
databricks secrets create-scope supply_chain
databricks secrets put-secret supply_chain anthropic_api_key --string-value sk-ant-...
databricks secrets put-secret supply_chain neo4j_password    --string-value <password>
```

### 2. Generate Synthetic Data

Open `data_gen/generate_supply_chain_data_notebook.py` in Databricks and run all cells.  
Data lands at: `/Volumes/supplychain/supply_chain_raw/landing/`

### 3. Deploy and Run the Medallion Pipeline

```bash
databricks bundle deploy
databricks bundle run supply_chain_pipeline
```

### 4. Run the Agent Notebook

Open `agents/supply_chain_agent_notebook` in your Databricks workspace:
- **Run All** once to initialize
- Update the **Question** widget and run the last cell for each query

### 5. Deploy the Databricks App

#### 5a. Grant the app service principal access to the secret scope

Before deploying, ensure the `supply_chain` secret scope exists with the required secrets:

```bash
databricks secrets create-scope supply_chain
databricks secrets put-secret supply_chain anthropic_api_key --string-value sk-ant-...
databricks secrets put-secret supply_chain neo4j_password    --string-value <password>
```

#### 5b. Upload and deploy

```bash
databricks workspace import-dir app /Workspace/Users/<your-email>/supply-chain-optimizer --overwrite
databricks apps deploy supply-chain-optimizer \
  --source-code-path /Workspace/Users/<your-email>/supply-chain-optimizer
```

#### 5c. Grant the app service principal permissions

After the first deploy, retrieve the app's service principal client ID:

```bash
databricks apps get supply-chain-optimizer -o json | python3 -c \
  "import sys,json; d=json.load(sys.stdin); print(d['service_principal_client_id'])"
```

Then grant it access to secrets and Unity Catalog:

```bash
SP=<service_principal_client_id>

# Secret scope
databricks secrets put-acl supply_chain "$SP" READ

# Unity Catalog
databricks grants update catalog supplychain \
  --json "{\"changes\": [{\"principal\": \"$SP\", \"add\": [\"USE CATALOG\"]}]}"
databricks grants update schema supplychain.supply_chain_medallion \
  --json "{\"changes\": [{\"principal\": \"$SP\", \"add\": [\"USE SCHEMA\", \"SELECT\"]}]}"
databricks grants update table supplychain.supply_chain_medallion.answer_cache \
  --json "{\"changes\": [{\"principal\": \"$SP\", \"add\": [\"MODIFY\"]}]}"
```

#### 5d. Re-deploying after code changes

```bash
databricks workspace import-dir app /Workspace/Users/<your-email>/supply-chain-optimizer --overwrite
databricks apps deploy supply-chain-optimizer \
  --source-code-path /Workspace/Users/<your-email>/supply-chain-optimizer
```

#### App

![Databricks App](docs/databricks_app.png)

---

## Agent Routing Logic

| Question Type | Route | Example |
|--------------|-------|---------|
| Risk scores, rankings | SQL | "Which suppliers have Critical risk?" |
| Stock status, availability | SQL | "What is the stock status for critical parts?" |
| PO aging, exposure | SQL | "Show delayed POs over 30 days old" |
| Shipment delays | SQL | "Which shipments have High disruption severity?" |
| BOM cost rollup | SQL | "What are the top 10 most expensive assemblies?" |
| Impact if X fails | **Graph** | "What parts are at risk if SUP-00094 fails?" |
| Dependency chains | **Graph** | "Which assemblies depend on this component?" |
| Single points of failure | **Graph** | "Which critical parts have only one supplier?" |
| Cascading disruptions | **Graph** | "What happens if all China suppliers are disrupted?" |

---

## Progressive Graph Projection

The Neo4j graph is built lazily — subgraphs are projected from Delta gold tables on first use and persist across questions:

| Subgraph | Relationships Created | Source Tables |
|----------|----------------------|---------------|
| `supplier_risk` | `(:Supplier)-[:SUPPLIES]->(:Part)` | gold_supplier_risk, gold_part_availability, gold_active_purchase_orders |
| `bom_dependency` | `(:Part)-[:REQUIRES]->(:Part)` | gold_bom_explosion |
| `shipment_route` | `(:Facility)-[:SHIPS_TO]->(:Facility)` | gold_shipment_pipeline |
| `full_network` | All of the above | All gold tables |

Projection is skipped automatically if the subgraph already exists in Neo4j (checked via a live count query).

---

## Answer Cache

All agent responses are cached in `supplychain.supply_chain_medallion.answer_cache` (Delta table).

- **Key**: SHA-256 hash of the normalized question text
- **TTL**: 24 hours (configurable via `CACHE_TTL_HOURS`)
- **Hit tracking**: `hit_count` incremented on every cache read
- **Invalidation**: Automatic on TTL expiry; corrupt entries self-heal on read

---

## Example Questions

```
Which suppliers have Critical risk scores?
Show me all delayed purchase orders over 30 days old
What are the top 10 most expensive BOM assemblies?
Which shipments have High or Critical disruption severity?
What is the stock status for critical parts?

What parts are at risk if our highest-risk supplier fails?
Which critical parts have only a single supplier?
What assemblies would be affected if Tier-1 suppliers from China are disrupted?
Show me the most depended-upon components in the BOM network
Which carrier routes have the most disrupted shipments?
```
