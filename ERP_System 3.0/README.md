# ERP_System 3.0

Package-first ERP/MRP backend.

Run commands:

```powershell
python -m erp_system.cli.etl
python -m erp_system.cli.llm_cli
python -m erp_system.cli.update_pod_site
```

Notes:
- no flat top-level script shims
- no allocation module
- webpage stays separate and should import this package
