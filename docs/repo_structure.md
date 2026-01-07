# Repository Structure (Engine Core)

This tree lists only the files involved in running the trading engine so it can be copy-pasted into ChatGPT Pro or similar tools for codebase analysis.

```
.
├── main.py
├── requirements.txt
├── run_stack.sh
├── Readme.md
├── .env.example
├── config
│   ├── app.yml
│   └── fees.yml
├── prometheus
│   └── prometheus.yml
├── docs
│   ├── engine_flow.svg
│   ├── repo_structure.md  (this file)
│   └── setup.md
├── brokerage
│   └── upstox_client.py
├── market
│   ├── __init__.py
│   └── instrument_cache.py
├── persistence
│   ├── __init__.py
│   ├── db.py
│   ├── schema_migrations
│   │   ├── 001_base_schema.sql
│   │   ├── 003_add_costs_metrics.sql
│   │   ├── 004_positions_unique.sql
│   │   ├── 005_exit_plans.sql
│   │   └── 006_add_lot_size_columns.sql
│   └── store.py
├── strategy
│   ├── advanced_buy.py
│   ├── momentum.py
│   ├── opening_range_breakout.py
│   └── scalping_buy.py
└── engine
    ├── __init__.py
    ├── alerts.py
    ├── broker.py
    ├── charges.py
    ├── config.py
    ├── config_sanity.py
    ├── data.py
    ├── events.py
    ├── exit.py
    ├── fees.py
    ├── instruments.py
    ├── logging_utils.py
    ├── metrics.py
    ├── oms.py
    ├── pnl.py
    ├── recovery.py
    ├── replay.py
    ├── risk.py
    ├── smoke_test.py
    └── time_machine.py
```

## UI & Dashboards

```
streamlit_app.py
grafana/
└── dashboard_buy.json
```

Copy the snippet above into ChatGPT Pro to give it an accurate view of the engine’s active modules without including unused or legacy files.
