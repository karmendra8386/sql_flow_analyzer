import click
from rich.console import Console
from rich.panel import Panel
from pathlib import Path

from .sql_parser import SQLParser
from .visualizer import MermaidVisualizer

console = Console()

@click.group()
def cli():
    """SQL Flow Analyzer - Generate data flow diagrams from SQL queries."""
    pass

@cli.command()
@click.option('--sql-file', required=True, help='Path to SQL file containing ETL queries')
@click.option('--output', default='etl_flow', help='Output file name (without extension)')
@click.option('--output-dir', default='etl_flow_diag', help='Output directory for generated diagrams')
def analyze(sql_file: str, output: str, output_dir: str):
    """Analyze SQL queries and generate a data flow diagram."""
    try:
        # Read SQL file
        sql_path = Path(sql_file)
        if not sql_path.exists():
            raise click.BadParameter(f"SQL file not found: {sql_file}")
            
        console.print(Panel.fit("🔍 Reading and parsing SQL queries...", title="Step 1"))
        with open(sql_path, 'r') as f:
            sql_content = f.read()
            
        # Parse SQL queries
        parser = SQLParser()
        relations = parser.parse_queries(sql_content)
        
        if not relations:
            console.print("[yellow]Warning: No table relations found in the SQL file.[/yellow]")
            return
            
        console.print(Panel.fit("📊 Generating data flow diagram...", title="Step 2"))
        
        # Create output directory if it doesn't exist
        output_dir_path = Path(output_dir)
        output_dir_path.mkdir(parents=True, exist_ok=True)
        
        # Generate output filename using SQL filename as prefix
        sql_filename = sql_path.stem  # Get filename without extension
        output_path = output_dir_path / f"{sql_filename}_{output}"
        
        # Generate diagram
        visualizer = MermaidVisualizer()
        visualizer.generate_diagram(relations, output_path)
        
        console.print(Panel.fit(
            f"✅ Data flow diagram generated successfully!\nSaved to: {output_path}.html",
            title="Complete"
        ))
        
    except Exception as e:
        console.print(f"[red]Error: {str(e)}[/red]")
        raise click.Abort()

if __name__ == '__main__':
    cli() 