#!/bin/bash
# Script to run MCP Tree-sitter server for the PSA Unified project

echo "Starting MCP Tree-sitter server for PSA Unified project..."
echo "This will analyze the codebase and provide advanced code understanding capabilities."
echo ""

# Run the MCP server with the inspector for development
mcp dev mcp_server_tree_sitter.server:mcp

# Alternative: Run without inspector
# mcp run mcp_server_tree_sitter.server:mcp