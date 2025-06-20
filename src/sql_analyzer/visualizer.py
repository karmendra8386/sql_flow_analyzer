from pathlib import Path
from typing import List, Dict
from .sql_parser import TableRelation, Column

class MermaidVisualizer:
    def __init__(self):
        self.nodes: Dict[str, List[Column]] = {}
        self.edges: List[tuple] = []
        self.table_types: Dict[str, str] = {}
        
    def _determine_table_type(self, table_name: str) -> str:
        """Determine the type of table based on its name."""
        name = table_name.lower()
        if 'source' in name:
            return 'source'
        elif 'staging' in name:
            return 'staging'
        elif 'fact' in name:
            return 'fact'
        elif 'dim_' in name:
            return 'dimension'
        elif 'mart' in name:
            return 'mart'
        elif 'metrics' in name:
            return 'metrics'
        elif 'warehouse' in name:
            return 'warehouse'
        else:
            return 'transform'
            
    def _create_node_id(self, table_name: str) -> str:
        """Create a valid Mermaid node ID from table name."""
        return table_name.replace('.', '_').replace('-', '_')
            
    def _create_node_def(self, table_name: str, columns: List[Column] = None) -> str:
        """Create Mermaid node definition with styling."""
        node_id = self._create_node_id(table_name)
        table_type = self._determine_table_type(table_name)
        self.table_types[node_id] = table_type
        
        # Create label with columns if available
        if columns:
            # Group columns by transformation type
            transformed = []
            regular = []
            for col in columns:
                if col.transformation:
                    transformed.append(f"• {col.name} ({col.transformation})")
                else:
                    regular.append(f"• {col.name}")
            
            column_text = ""
            if regular:
                column_text += "<br/>" + "<br/>".join(regular)
            if transformed:
                if regular:
                    column_text += "<br/>---"
                column_text += "<br/>" + "<br/>".join(transformed)
        else:
            column_text = ''
            
        return f'{node_id}["{table_name}{column_text}"]'
            
    def _create_edge_def(self, source: str, target: str, operation: str) -> str:
        """Create Mermaid edge definition with styling."""
        source_id = self._create_node_id(source)
        target_id = self._create_node_id(target)
        
        # Define arrow style based on operation
        if operation == 'EXTRACT':
            style = 'stroke:#1f77b4,stroke-width:2px'
        elif operation == 'TRANSFORM':
            style = 'stroke:#2ca02c,stroke-width:2px'
        elif operation == 'LOAD':
            style = 'stroke:#d62728,stroke-width:2px'
        elif operation == 'MERGE':
            style = 'stroke:#9467bd,stroke-width:2px'
        else:
            style = 'stroke:#7f7f7f,stroke-width:1px'
            
        return f'{source_id} -->|{operation}| {target_id}'
        
    def generate_diagram(self, relations: List[TableRelation], output_path: str):
        """Generate the ETL data flow diagram in Mermaid format."""
        mermaid_code = ["graph LR;", "  %% Node Styles"]
        
        # Define node styles
        mermaid_code.extend([
            "  classDef source fill:#f5f5f5,stroke:#1f77b4,stroke-width:2px",
            "  classDef staging fill:#e3f2fd,stroke:#2196f3,stroke-width:2px",
            "  classDef transform fill:#f1f8e9,stroke:#4caf50,stroke-width:2px",
            "  classDef fact fill:#fff3e0,stroke:#ff9800,stroke-width:2px",
            "  classDef dimension fill:#f3e5f5,stroke:#9c27b0,stroke-width:2px",
            "  classDef mart fill:#fce4ec,stroke:#e91e63,stroke-width:2px",
            "  classDef metrics fill:#e8eaf6,stroke:#3f51b5,stroke-width:2px",
            "  classDef warehouse fill:#fbe9e7,stroke:#ff5722,stroke-width:2px",
            "  classDef audit fill:#efebe9,stroke:#795548,stroke-width:2px",
            "  classDef procedure fill:#e0f2f1,stroke:#009688,stroke-width:2px",
            "  classDef other fill:#fff,stroke:#333,stroke-width:1px",
            ""
        ])
        
        # First pass: create all nodes
        nodes_seen = set()
        for relation in relations:
            if relation.source not in nodes_seen:
                mermaid_code.append(f"  {self._create_node_def(relation.source)}")
                nodes_seen.add(relation.source)
            
            if relation.target not in nodes_seen:
                mermaid_code.append(f"  {self._create_node_def(relation.target, relation.columns)}")
                nodes_seen.add(relation.target)
        
        mermaid_code.append("")
        
        # Second pass: create all edges
        for relation in relations:
            mermaid_code.append(f"  {self._create_edge_def(relation.source, relation.target, relation.operation)}")
            
        # Third pass: apply styles
        mermaid_code.append("")
        for node, type_ in self.table_types.items():
            mermaid_code.append(f"  class {node} {type_}")
            
        # Generate HTML with embedded Mermaid
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>ETL Flow Diagram</title>
            <script src="https://cdn.jsdelivr.net/npm/mermaid/dist/mermaid.min.js"></script>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                .mermaid {{ text-align: center; }}
                .legend {{ margin-top: 20px; padding: 10px; border: 1px solid #ccc; }}
                .legend h3 {{ margin-top: 0; }}
                .legend-item {{ margin: 5px 0; }}
                .legend-color {{ display: inline-block; width: 20px; height: 3px; margin-right: 10px; vertical-align: middle; }}
            </style>
        </head>
        <body>
            <div class="mermaid">
            {chr(10).join(mermaid_code)}
            </div>
            
            <div class="legend">
                <h3>Legend</h3>
                <div class="legend-item">
                    <span class="legend-color" style="background: #1f77b4;"></span>
                    EXTRACT: Data extraction from source systems
                </div>
                <div class="legend-item">
                    <span class="legend-color" style="background: #2ca02c;"></span>
                    TRANSFORM: Data transformation and cleaning
                </div>
                <div class="legend-item">
                    <span class="legend-color" style="background: #d62728;"></span>
                    LOAD: Loading data into target tables
                </div>
                <div class="legend-item">
                    <span class="legend-color" style="background: #9467bd;"></span>
                    MERGE: Slowly changing dimension updates
                </div>
            </div>
            
            <script>
                mermaid.initialize({{
                    startOnLoad: true,
                    theme: 'default',
                    flowchart: {{
                        useMaxWidth: true,
                        htmlLabels: true,
                        curve: 'basis'
                    }}
                }});
            </script>
        </body>
        </html>
        """
        
        # Write HTML file
        output_file = Path(f"{output_path}.html")
        # Ensure parent directory exists
        output_file.parent.mkdir(parents=True, exist_ok=True)
        output_file.write_text(html_content, encoding='utf-8') 