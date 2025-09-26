# MCP Tree-sitter Server Setup for PSA Unified

The MCP Tree-sitter server is now configured and ready to use with Claude Code for advanced code analysis of the PSA Unified project.

## What It Provides

- **AST-based code analysis**: Deep understanding of code structure
- **Symbol extraction**: Find classes, functions, and variables
- **Cross-file search**: Search patterns across the entire codebase
- **Dependency analysis**: Understand relationships between components
- **Multi-language support**: Python, SQL, and more

## How to Use

### Option 1: Run the MCP Inspector (Development Mode)

```bash
./scripts/run_tree_sitter_server.sh
```

This opens a web interface where you can test the server's capabilities.

### Option 2: Use with Claude Code

The server is automatically available when you use Claude Code. You can ask questions like:

- "Find all classes that inherit from SparkModel"
- "Show me all functions handling Tímapottur logic"
- "Analyze the dependency graph between core and ConnectWise packages"
- "Find all fact table creators in the codebase"
- "Show me the complexity of the transforms modules"

### Option 3: Programmatic Access

Use the test script to explore capabilities:

```bash
python scripts/test_tree_sitter_mcp.py
```

## Example Queries for PSA Unified

1. **Find all Pydantic models**:
   "Show me all classes that inherit from SparkModel or BaseModel"

2. **Analyze fact table patterns**:
   "Find all functions that create fact tables in the gold layer"

3. **Track Tímapottur handling**:
   "Show me all code that handles Tímapottur agreements"

4. **Understand dimension creation**:
   "Analyze how dimensions are generated from enum columns"

5. **Find API integration points**:
   "Show me all ConnectWise API client methods"

## Benefits for This Project

- Quickly navigate the large codebase across multiple packages
- Understand complex inheritance hierarchies (SparkModel → Pydantic models)
- Track business logic implementations (Icelandic agreement types)
- Analyze ETL pipeline flows from Bronze → Silver → Gold
- Find all usages of specific patterns or functions