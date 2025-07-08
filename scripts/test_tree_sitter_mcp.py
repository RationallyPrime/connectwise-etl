#!/usr/bin/env python3
"""
Test script for MCP Tree-sitter server
This demonstrates how to use the tree-sitter server to analyze the PSA Unified codebase
"""

import asyncio
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client


async def main():
    """Test the MCP Tree-sitter server functionality"""
    
    # Create connection to the server
    server_params = StdioServerParameters(
        command="python",
        args=["-m", "mcp_server_tree_sitter.server"]
    )
    
    async with stdio_client(server_params) as (read, write):
        async with ClientSession(read, write) as session:
            # Initialize the session
            await session.initialize()
            
            print("Connected to MCP Tree-sitter server!")
            print(f"Available tools: {[tool.name for tool in session.tools]}")
            print("\nTesting tree-sitter capabilities on PSA Unified project...\n")
            
            # Example 1: Register the project
            result = await session.call_tool(
                "register_project",
                arguments={
                    "name": "psa-unified",
                    "path": "/home/hakonf/psa-unified-clean"
                }
            )
            print(f"Project registered: {result}")
            
            # Example 2: Find all Python classes that inherit from SparkModel
            result = await session.call_tool(
                "search_project",
                arguments={
                    "project_name": "psa-unified",
                    "query": "class that extends SparkModel",
                    "language": "python"
                }
            )
            print(f"\nClasses inheriting from SparkModel: {result}")
            
            # Example 3: Extract symbols from a specific file
            result = await session.call_tool(
                "extract_symbols",
                arguments={
                    "project_name": "psa-unified",
                    "path": "packages/unified-etl-core/src/unified_etl_core/dimensions.py"
                }
            )
            print(f"\nSymbols in dimensions.py: {result}")
            
            # Example 4: Find Tímapottur references
            result = await session.call_tool(
                "search_project",
                arguments={
                    "project_name": "psa-unified",
                    "query": "Tímapottur",
                    "language": "python"
                }
            )
            print(f"\nTímapottur references: {result}")


if __name__ == "__main__":
    asyncio.run(main())