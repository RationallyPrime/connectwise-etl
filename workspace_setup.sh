#!/bin/bash
# Setup unified-etl as a uv workspace

# Create workspace structure
mkdir -p packages/unified-etl-core/src/unified_etl_core
mkdir -p packages/unified-etl-connectwise/src/unified_etl_connectwise  
mkdir -p packages/unified-etl-businesscentral/src/unified_etl_businesscentral

# Create root workspace pyproject.toml
cat > pyproject.toml.workspace << 'EOF'
[tool.uv]
workspace = ["packages/*"]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"
EOF

# Create core package pyproject.toml
cat > packages/unified-etl-core/pyproject.toml << 'EOF'
[project]
name = "unified-etl-core"
version = "1.0.0"
description = "Core ETL framework with medallion architecture"
authors = [{name = "Hákon Freyr Gunnarsson", email = "hakonf@wise.is"}]
requires-python = ">=3.11"
dependencies = [
    "pydantic>=2.11.4",
    "sparkdantic",
    "requests>=2.28.0",
    "tenacity>=8.0.0",
    "pyyaml>=6.0.0",
]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"
EOF

echo "Workspace structure created! Next steps:"
echo "1. Move core modules to packages/unified-etl-core/src/unified_etl_core/"
echo "2. Move ConnectWise-specific code to packages/unified-etl-connectwise/"
echo "3. Run 'uv sync' to set up the workspace"