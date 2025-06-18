#!/bin/bash

# 1. Create and activate virtualenv
#rm -rf .venv
#python3 -m venv .venv
source .venv/bin/activate

# 2. Install dependencies
pip install --upgrade pip
pip install -r requirements.txt

# 3. Setup dbt profile
mkdir -p ~/.dbt
cat > ~/.dbt/profiles.yml <<EOL
hm_analytics:
  outputs:
    dev:
      type: duckdb
      path: db/hm.duckdb
      threads: 1
    prod:
      type: duckdb
      path: db/hm.duckdb
      threads: 4
  target: dev
EOL

# 4. load sample data to duckdb
rm db/hm.duckdb
python3 load_data_to_duckdb.py

## 5. Run dbt
dbt run --project-dir hm_analytics

# 6. Launch Streamlit
streamlit run visualize_data.py

echo "Setup complete!"
echo "run: source .venv/bin/activate"