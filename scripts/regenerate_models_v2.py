#!/usr/bin/env python3
"""Unified model generator with auto-detection and pluggable formats."""
import logging
from pathlib import Path

import click
from unified_etl_core.generators.registry import GeneratorRegistry

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@click.command()
@click.argument('source', type=click.Path(exists=True, path_type=Path))
@click.argument('output', type=click.Path(path_type=Path))
@click.option('--format', '-f', help='Input format (auto-detected if not specified)')
@click.option('--config', '-c',
              default='../configs/generation.toml',
              type=click.Path(exists=True, path_type=Path),
              help='Generation config file')
@click.option('--validate/--no-validate', default=True, help='Validate output models')
@click.option('--debug', is_flag=True, help='Enable debug logging')
def generate(source: Path, output: Path, format: str, config: Path, validate: bool, debug: bool):
    """Generate Pydantic models from various schema formats.
    
    Examples:
        
        # Auto-detect format
        python regenerate_models_v2.py PSA_OpenAPI_schema.json models.py
        
        # Explicit format
        python regenerate_models_v2.py schema.json models.py --format openapi
        
        # With custom config
        python regenerate_models_v2.py data.json models.py --config my-config.toml
    """
    if debug:
        logging.getLogger().setLevel(logging.DEBUG)

    try:
        # Auto-detect format if not specified
        if not format:
            format = GeneratorRegistry.detect_format(source)
            click.echo(f"🔍 Detected format: {format}")
        else:
            click.echo(f"📋 Using specified format: {format}")

        # Create generator
        generator = GeneratorRegistry.create(format, config)

        # Generate models
        click.echo(f"🚀 Generating models from {source} → {output}")
        generator.generate(source, output, validate=validate)

        click.echo("✅ Generation complete!")

    except Exception as e:
        click.echo(f"❌ Error: {e}", err=True)
        if debug:
            logger.exception("Full error trace:")
        raise click.Abort()


@click.command()
def list_formats():
    """List all available formats."""
    click.echo("Available formats:")
    for fmt in GeneratorRegistry.list_formats():
        click.echo(f"  - {fmt}")


@click.group()
def cli():
    """Unified model generator for ETL framework."""
    pass


# Add commands to group
cli.add_command(generate)
cli.add_command(list_formats)


if __name__ == '__main__':
    cli()
