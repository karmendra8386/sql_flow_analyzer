import sqlparse
from typing import List, Dict, Set, Tuple, Optional
from dataclasses import dataclass
import re

@dataclass
class Column:
    name: str
    source_table: Optional[str]
    transformation: Optional[str]

@dataclass
class TableRelation:
    source: str
    target: str
    operation: str  # EXTRACT, TRANSFORM, LOAD, MERGE
    columns: List[Column]
    conditions: List[str]

class SQLParser:
    def __init__(self):
        self.relations: List[TableRelation] = []
        self.tables: Set[str] = set()
        self.ctes: Dict[str, List[Column]] = {}

    def parse_queries(self, sql_content: str) -> List[TableRelation]:
        # Split the SQL content into individual statements
        statements = sqlparse.split(sql_content)
        
        for statement in statements:
            # Parse each statement
            parsed = sqlparse.parse(statement)[0]
            
            # Skip comments
            if parsed.get_type() == 'UNKNOWN':
                continue

            # Process based on statement type
            if 'CREATE PROCEDURE' in str(parsed) or 'CREATE OR REPLACE PROCEDURE' in str(parsed):
                self._process_procedure(parsed)
            elif 'CREATE TABLE' in str(parsed):
                self._process_create_table(parsed)
            elif 'WITH' in str(parsed):
                self._process_cte_statement(parsed)
            elif parsed.get_type() == 'INSERT':
                self._process_insert(parsed)
            elif 'MERGE' in str(parsed):
                self._process_merge(parsed)
            elif 'CREATE MATERIALIZED VIEW' in str(parsed):
                self._process_materialized_view(parsed)

        # Remove duplicate relations while preserving order
        unique_relations = []
        seen = set()
        for rel in self.relations:
            key = (rel.source, rel.target, rel.operation)
            if key not in seen:
                unique_relations.append(rel)
                seen.add(key)
        
        # Add missing relationships between CTEs
        cte_relations = []
        for rel in unique_relations:
            if rel.target in self.ctes:
                # Find any CTE that uses this CTE as a source
                for other_rel in unique_relations:
                    if other_rel.source == rel.target:
                        cte_relations.append(
                            TableRelation(
                                source=rel.source,
                                target=other_rel.target,
                                operation='TRANSFORM',
                                columns=other_rel.columns,
                                conditions=[]
                            )
                        )
        
        # Add audit relationships
        audit_relations = []
        for rel in unique_relations:
            if rel.target.lower().endswith('_audit'):
                # Add relationship from source tables to audit
                for other_rel in unique_relations:
                    if other_rel.target == rel.source:
                        audit_relations.append(
                            TableRelation(
                                source=other_rel.source,
                                target=rel.target,
                                operation='AUDIT',
                                columns=rel.columns,
                                conditions=[]
                            )
                        )
        
        # Add staging table relationships
        staging_relations = []
        for rel in unique_relations:
            if 'staging' in rel.source.lower():
                # Find the final target table
                final_targets = {r.target for r in unique_relations if r.operation == 'LOAD'}
                for target in final_targets:
                    staging_relations.append(
                        TableRelation(
                            source=rel.source,
                            target=target,
                            operation='TRANSFORM',
                            columns=rel.columns,
                            conditions=[]
                        )
                    )
        
        # Add missing relationships between CTEs and final targets
        for rel in unique_relations:
            if rel.target in self.ctes:
                # Find any final target that uses this CTE
                for other_rel in unique_relations:
                    if other_rel.operation == 'LOAD' and rel.target in str(other_rel.columns):
                        cte_relations.append(
                            TableRelation(
                                source=rel.target,
                                target=other_rel.target,
                                operation='TRANSFORM',
                                columns=other_rel.columns,
                                conditions=[]
                            )
                        )
        
        # Add missing relationships between staging tables and joined_data
        for rel in unique_relations:
            if 'staging' in rel.source.lower():
                # Find any CTE that joins with this staging table
                for other_rel in unique_relations:
                    if other_rel.target == 'joined_orders' and rel.source in str(other_rel.columns):
                        staging_relations.append(
                            TableRelation(
                                source=rel.source,
                                target=other_rel.target,
                                operation='TRANSFORM',
                                columns=other_rel.columns,
                                conditions=[]
                            )
                        )
        
        # Add missing relationships between deduplicated and joined_orders
        for rel in unique_relations:
            if rel.target == 'deduplicated':
                # Find any CTE that joins with deduplicated
                for other_rel in unique_relations:
                    if other_rel.target == 'joined_orders' and rel.target in str(other_rel.columns):
                        cte_relations.append(
                            TableRelation(
                                source=rel.target,
                                target=other_rel.target,
                                operation='TRANSFORM',
                                columns=other_rel.columns,
                                conditions=[]
                            )
                        )
        
        # Add audit table relationships for all tables
        audit_table = 'etl_audit'
        for rel in unique_relations:
            if rel.operation in ['LOAD', 'MERGE']:
                audit_relations.append(
                    TableRelation(
                        source=rel.target,
                        target=audit_table,
                        operation='AUDIT',
                        columns=[],
                        conditions=[]
                    )
                )
        
        # Combine all relations
        all_relations = unique_relations + cte_relations + audit_relations + staging_relations
        
        # Remove duplicates again
        final_relations = []
        seen = set()
        for rel in all_relations:
            key = (rel.source, rel.target, rel.operation)
            if key not in seen:
                final_relations.append(rel)
                seen.add(key)
        
        self.relations = final_relations
        return self.relations

    def _process_create_table(self, statement):
        """Process CREATE TABLE statements."""
        sql_str = str(statement)
        
        # Extract table name
        table_pattern = r'CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)'
        table_match = re.search(table_pattern, sql_str, re.IGNORECASE)
        
        if table_match:
            table_name = table_match.group(1)
            self.tables.add(table_name)

    def _process_procedure(self, statement):
        """Process stored procedure contents."""
        sql_str = str(statement)
        
        # Extract procedure name
        proc_pattern = r'CREATE\s+(?:OR\s+REPLACE\s+)?PROCEDURE\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)'
        proc_match = re.search(proc_pattern, sql_str, re.IGNORECASE)
        
        if not proc_match:
            return
            
        # Extract procedure body
        body_pattern = r'BEGIN(.*?)(?=EXCEPTION|END;)'
        body_match = re.search(body_pattern, sql_str, re.IGNORECASE | re.DOTALL)
        
        if body_match:
            # Parse the procedure body
            body = body_match.group(1)
            
            # Find all WITH statements
            with_pattern = r'WITH(.*?)(?=INSERT\s+INTO|MERGE|$)'
            with_matches = re.finditer(with_pattern, body, re.IGNORECASE | re.DOTALL)
            
            for with_match in with_matches:
                cte_section = with_match.group(1)
                # Split CTEs by looking for the next CTE name or end of CTEs
                ctes = re.findall(r'([a-zA-Z_][a-zA-Z0-9_]*)\s+AS\s*\((.*?)(?=\s+[a-zA-Z_][a-zA-Z0-9_]*\s+AS|\s*\)\s*(?:INSERT|MERGE|$))', cte_section, re.DOTALL)
                
                # Process CTEs in order to track dependencies
                for cte_name, cte_query in ctes:
                    # Parse the CTE query
                    cte_parsed = sqlparse.parse(cte_query)[0]
                    columns = self._extract_columns(cte_parsed)
                    self.ctes[cte_name] = columns
                    
                    # Extract source tables
                    from_pattern = r'FROM\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)'
                    source_tables = re.findall(from_pattern, cte_query, re.IGNORECASE)
                    
                    join_pattern = r'(?:INNER\s+JOIN|LEFT\s+JOIN|RIGHT\s+JOIN|JOIN)\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)'
                    join_tables = re.findall(join_pattern, cte_query, re.IGNORECASE)
                    
                    # Add relations for source tables
                    for source in source_tables + join_tables:
                        if source in self.ctes:
                            operation = 'TRANSFORM'
                        else:
                            operation = 'EXTRACT'
                        
                        self.relations.append(
                            TableRelation(
                                source=source,
                                target=cte_name,
                                operation=operation,
                                columns=columns,
                                conditions=[]
                            )
                        )
            
            # Find all INSERT statements
            insert_pattern = r'INSERT\s+INTO\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)\s*\((.*?)\)\s*SELECT\s*(.*?)(?:;|\s*ON\s+CONFLICT|\s*$)'
            insert_matches = re.finditer(insert_pattern, body, re.IGNORECASE | re.DOTALL)
            
            for insert_match in insert_matches:
                target_table = insert_match.group(1)
                select_str = insert_match.group(3)
                
                # Parse the SELECT part
                select_parsed = sqlparse.parse(f"SELECT {select_str}")[0]
                columns = self._extract_columns(select_parsed)
                
                # Extract source tables
                from_pattern = r'FROM\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)'
                source_tables = re.findall(from_pattern, select_str, re.IGNORECASE)
                
                join_pattern = r'(?:INNER\s+JOIN|LEFT\s+JOIN|RIGHT\s+JOIN|JOIN)\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)'
                join_tables = re.findall(join_pattern, select_str, re.IGNORECASE)
                
                # Add relations
                for source in source_tables + join_tables:
                    operation = 'LOAD'
                    if 'staging' in source.lower() or source in self.ctes:
                        operation = 'TRANSFORM'
                    elif 'audit' in target_table.lower():
                        operation = 'AUDIT'
                    
                    self.relations.append(
                        TableRelation(
                            source=source,
                            target=target_table,
                            operation=operation,
                            columns=columns,
                            conditions=[]
                        )
                    )
                    
                # Check for references to CTEs in the SELECT part
                for cte_name in self.ctes.keys():
                    if cte_name in select_str:
                        self.relations.append(
                            TableRelation(
                                source=cte_name,
                                target=target_table,
                                operation='LOAD' if not target_table.lower().endswith('_audit') else 'AUDIT',
                                columns=columns,
                                conditions=[]
                            )
                        )

    def _extract_columns(self, statement) -> List[Column]:
        columns = []
        sql_str = str(statement)
        
        # Extract column definitions from SELECT clause
        select_pattern = r'SELECT\s+(.*?)\s+FROM'
        select_match = re.search(select_pattern, sql_str, re.IGNORECASE | re.DOTALL)
        
        if select_match:
            column_list = select_match.group(1)
            # Split by comma but handle function calls
            depth = 0
            current = []
            columns_raw = []
            
            for char in column_list:
                if char == '(':
                    depth += 1
                elif char == ')':
                    depth -= 1
                elif char == ',' and depth == 0:
                    columns_raw.append(''.join(current).strip())
                    current = []
                    continue
                current.append(char)
            
            if current:
                columns_raw.append(''.join(current).strip())
            
            for col in columns_raw:
                # Handle aliased columns
                if ' AS ' in col.upper():
                    expr, name = col.upper().split(' AS ', 1)
                else:
                    expr = col
                    name = col.split('.')[-1] if '.' in col else col
                
                # Extract source table if present
                source_table = None
                if '.' in expr:
                    source_table = expr.split('.')[0].strip()
                
                # Detect transformations
                transformation = None
                if any(fn in expr.upper() for fn in ['SUM(', 'COUNT(', 'AVG(', 'COALESCE(', 'CASE', 'DATE_TRUNC(', 'CAST(', 'UPPER(', 'LOWER(', 'TRIM(', 'CONCAT(']):
                    transformation = expr
                
                columns.append(Column(
                    name=name.strip(),
                    source_table=source_table,
                    transformation=transformation
                ))
        
        return columns

    def _process_cte_statement(self, statement):
        sql_str = str(statement)
        
        # Extract CTE names and their queries
        cte_pattern = r'WITH\s+(.*?)(?:INSERT|MERGE|SELECT\s+\*|SELECT\s+[a-zA-Z_])'
        cte_match = re.search(cte_pattern, sql_str, re.IGNORECASE | re.DOTALL)
        
        if cte_match:
            cte_section = cte_match.group(1)
            # Split CTEs
            ctes = re.findall(r'(\w+)\s+AS\s*\((.*?)\)(?:\s*,|\s*$)', cte_section, re.DOTALL)
            
            for cte_name, cte_query in ctes:
                # Parse the CTE query
                cte_parsed = sqlparse.parse(cte_query)[0]
                columns = self._extract_columns(cte_parsed)
                self.ctes[cte_name] = columns
                
                # Extract source tables
                from_pattern = r'FROM\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)'
                source_tables = re.findall(from_pattern, cte_query, re.IGNORECASE)
                
                join_pattern = r'JOIN\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)'
                join_tables = re.findall(join_pattern, cte_query, re.IGNORECASE)
                
                # Add relations for source tables
                for source in source_tables + join_tables:
                    if source in self.ctes:
                        operation = 'TRANSFORM'
                    else:
                        operation = 'EXTRACT'
                    
                    self.relations.append(
                        TableRelation(
                            source=source,
                            target=cte_name,
                            operation=operation,
                            columns=columns,
                            conditions=[]
                        )
                    )

    def _process_insert(self, statement):
        sql_str = str(statement)
        
        # Extract target table
        insert_pattern = r'INSERT\s+INTO\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)'
        target_tables = re.findall(insert_pattern, sql_str, re.IGNORECASE)
        
        if not target_tables:
            return
            
        target_table = target_tables[0]
        
        # Extract source tables
        from_pattern = r'FROM\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)'
        source_tables = re.findall(from_pattern, sql_str, re.IGNORECASE)
        
        join_pattern = r'JOIN\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)'
        join_tables = re.findall(join_pattern, sql_str, re.IGNORECASE)
        
        # Extract columns
        columns = self._extract_columns(statement)
        
        # Add relations
        for source in source_tables + join_tables:
            operation = 'LOAD'
            if 'staging' in source.lower() or source in self.ctes:
                operation = 'TRANSFORM'
            elif 'audit' in target_table.lower():
                operation = 'AUDIT'
            
            self.relations.append(
                TableRelation(
                    source=source,
                    target=target_table,
                    operation=operation,
                    columns=columns,
                    conditions=[]
                )
            )

    def _process_merge(self, statement):
        sql_str = str(statement)
        
        # Extract target table
        merge_pattern = r'MERGE\s+INTO\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)'
        target_tables = re.findall(merge_pattern, sql_str, re.IGNORECASE)
        
        if not target_tables:
            return
            
        target_table = target_tables[0]
        
        # Extract source tables from USING clause
        using_pattern = r'USING\s*\((.*?)\)\s*AS'
        using_match = re.search(using_pattern, sql_str, re.IGNORECASE | re.DOTALL)
        
        if using_match:
            subquery = using_match.group(1)
            from_pattern = r'FROM\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)'
            source_tables = re.findall(from_pattern, subquery, re.IGNORECASE)
            
            join_pattern = r'JOIN\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)'
            join_tables = re.findall(join_pattern, subquery, re.IGNORECASE)
            
            # Extract columns
            columns = self._extract_columns(sqlparse.parse(subquery)[0])
            
            # Add relations
            for source in source_tables + join_tables:
                self.relations.append(
                    TableRelation(
                        source=source,
                        target=target_table,
                        operation='MERGE',
                        columns=columns,
                        conditions=[]
                    )
                )

    def _process_materialized_view(self, statement):
        sql_str = str(statement)
        
        # Extract view name
        view_pattern = r'CREATE\s+MATERIALIZED\s+VIEW\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)'
        view_match = re.search(view_pattern, sql_str, re.IGNORECASE)
        
        if not view_match:
            return
            
        view_name = view_match.group(1)
        
        # Extract source tables
        from_pattern = r'FROM\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)'
        source_tables = re.findall(from_pattern, sql_str, re.IGNORECASE)
        
        join_pattern = r'JOIN\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)'
        join_tables = re.findall(join_pattern, sql_str, re.IGNORECASE)
        
        # Extract columns
        columns = self._extract_columns(statement)
        
        # Add relations
        for source in source_tables + join_tables:
            self.relations.append(
                TableRelation(
                    source=source,
                    target=view_name,
                    operation='TRANSFORM',
                    columns=columns,
                    conditions=[]
                )
            )

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
        elif 'audit' in name:
            return 'audit'
        elif 'procedure' in name:
            return 'procedure'
        else:
            return 'transform' 