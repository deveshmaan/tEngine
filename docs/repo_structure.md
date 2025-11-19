# Repository Structure (Engine Core)

This tree lists only the files involved in running the trading engine so it can be copy-pasted into ChatGPT Pro or similar tools for codebase analysis.

```
.
├── main.py
├── engine_config.yaml
├── requirements.txt
├── run_stack.sh
├── Readme.md
├── config
│   ├── app.yml
│   └── fees.yml
├── prometheus
│   └── prometheus.yml
├── docs
│   ├── engine_flow.svg
│   └── repo_structure.md  (this file)
├── brokerage
│   └── upstox_client.py
├── market
│   ├── __init__.py
│   └── instrument_cache.py
├── persistence
│   ├── __init__.py
│   ├── db.py
│   └── store.py
└── engine
    ├── __init__.py
    ├── alerts.py
    ├── broker.py
    ├── config.py
    ├── data.py
    ├── events.py
    ├── fees.py
    ├── instruments.py
    ├── logging_utils.py
    ├── metrics.py
    ├── oms.py
    ├── pnl.py
    ├── recovery.py
    ├── replay.py
    ├── risk.py
    └── time_machine.py
```

## UI & Dashboards

```
streamlit_app.py
grafana/
└── dashboard.json
```

Copy the snippet above into ChatGPT Pro to give it an accurate view of the engine’s active modules without including unused or legacy files.
