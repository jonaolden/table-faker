import yaml
from os import path, makedirs
from . import config
from .llm_client import LLMClient
from .semantic_model_metrics import generate_model_metrics
from typing import Dict, List, Any, Optional
import re

def generate_semantic_view(config_source, target_file_path=None, llm_config_path=None):
    """
    Generate a semantic view YAML file from a tablefaker config.
    
    Args:
        config_source: Path to tablefaker config file or dict
        target_file_path: Optional path to write YAML (default: same dir as config)
        llm_config_path: Optional path to llm.config file (default: looks for llm.config in current dir)
    
    Returns:
        Path to generated semantic view YAML file
    """
    
    # Initialize LLM client
    llm_client = LLMClient(llm_config_path)
    
    # Load config
    if isinstance(config_source, str):
        conf = config.Config(config_source)
    elif isinstance(config_source, dict):
        conf = config.Config(config_source)
    else:
        raise Exception("Unsupported config source for semantic view generation")
    
    tables = conf.config.get("tables", [])
    
    # Build table metadata
    table_metadata = _extract_table_metadata(tables)
    
    # Build relationships from foreign keys
    relationships = _extract_relationships(tables)
    
    # Generate semantic model structure
    semantic_model = _build_semantic_model(table_metadata, relationships, llm_client)
    
    # Determine output path
    if isinstance(config_source, str):
        base = path.splitext(path.basename(config_source))[0]
        default_name = f"{base}_semantic_view.yml"
        src_dir = path.dirname(config_source) or "."
    else:
        default_name = "semantic_view.yml"
        src_dir = "."
    
    if target_file_path in (None, "", "."):
        out_path = path.join(src_dir, default_name)
    else:
        out_path = path.join(target_file_path, default_name) if path.isdir(target_file_path) else target_file_path
    
    # Ensure parent directory exists
    makedirs(path.dirname(out_path) or ".", exist_ok=True)
    
    # Write YAML (increase indentation for logical table properties)
    with open(out_path, "w", encoding="utf-8") as outf:
        # Use 2-space indentation to match existing domain semantic view formatting.
        yaml.safe_dump(semantic_model, outf, sort_keys=False, default_flow_style=False, allow_unicode=True, indent=2)
    
    # Attempt to generate and inject model-level metrics into the written semantic view.
    # This is best-effort: failure to generate metrics should not prevent semantic view creation.
    try:
        # generate_model_metrics will overwrite the semantic view file by default to include metrics
        generate_model_metrics(out_path, llm_config_path=llm_client._config_path if hasattr(llm_client, "_config_path") else None)
    except Exception as e:
        # Avoid crashing the semantic view generation if metrics generation fails.
        print(f"Warning: failed to generate model metrics: {e}")
    
    return out_path


def _extract_table_metadata(tables: List[Dict]) -> Dict[str, Dict]:
    """Extract metadata for each table including columns and their types."""
    metadata = {}
    
    for table in tables:
        table_name = table.get("table_name")
        columns = table.get("columns", [])
        
        table_info = {
            "table_name": table_name,
            "row_count": table.get("row_count", 10),
            "primary_keys": [],
            "columns": []
        }
        
        for col in columns:
            col_name = col.get("column_name")
            col_type = col.get("type", "string")
            data_expr = str(col.get("data", ""))
            is_pk = col.get("is_primary_key", False)
            
            col_info = {
                "column_name": col_name,
                "type": col_type,
                "data_expression": data_expr,
                "is_primary_key": is_pk,
                "is_foreign_key": "foreign_key(" in data_expr,
                "references_column": "copy_from_fk(" in data_expr or any(c in data_expr for c in [col_name] if col_name != col.get("column_name")),
                "null_percentage": col.get("null_percentage", 0)
            }
            
            # Extract foreign key reference if present
            if col_info["is_foreign_key"]:
                fk_match = re.search(r'foreign_key\(["\'](\w+)["\']\s*,\s*["\'](\w+)["\']\)', data_expr)
                if fk_match:
                    col_info["fk_table"] = fk_match.group(1)
                    col_info["fk_column"] = fk_match.group(2)
            
            table_info["columns"].append(col_info)
            
            if is_pk:
                table_info["primary_keys"].append(col_name)
        
        metadata[table_name] = table_info
    
    return metadata


def _extract_relationships(tables: List[Dict]) -> List[Dict]:
    """Extract relationships based on foreign_key() calls.

    Return relationships in the exact target format with relationship_columns:
      - name: LEFT_TO_RIGHT (uppercased)
        left_table: LEFT (uppercased)
        right_table: RIGHT (uppercased)
        relationship_columns:
          - left_column: LEFT_COL (uppercased)
            right_column: RIGHT_COL (uppercased)
        relationship_type: many_to_one
        join_type: left_outer
    """
    relationships: List[Dict] = []
    seen = set()

    # Build PK map (use original casing from config but normalize when emitting)
    table_pks = {}
    for table in tables:
        table_name = table.get("table_name")
        for col in table.get("columns", []):
            if col.get("is_primary_key"):
                table_pks[table_name] = col.get("column_name")
                break

    # Extract FK relationships
    for table in tables:
        left_table = table.get("table_name")
        for col in table.get("columns", []):
            col_name = col.get("column_name")
            cmd = str(col.get("data", ""))

            if "foreign_key(" in cmd:
                fk_match = re.search(r'foreign_key\(["\'](\w+)["\']\s*,\s*["\'](\w+)["\']\)', cmd)
                if not fk_match:
                    continue
                right_table = fk_match.group(1)
                right_col = fk_match.group(2)

                # Only add if right_col is the PK of right_table
                if right_table in table_pks and table_pks[right_table] == right_col:
                    # Normalize keys for uniqueness check
                    rel_key = (left_table.upper(), right_table.upper(), col_name.upper(), right_col.upper())
                    if rel_key in seen:
                        continue
                    seen.add(rel_key)

                    rel_entry = {
                        "name": f"{left_table}_TO_{right_table}".upper(),
                        "left_table": left_table.upper(),
                        "right_table": right_table.upper(),
                        "relationship_columns": [
                            {
                                "left_column": col_name.upper(),
                                "right_column": right_col.upper()
                            }
                        ],
                        "relationship_type": "many_to_one",
                        "join_type": "left_outer"
                    }
                    relationships.append(rel_entry)

    return relationships


def _classify_column(col_info: Dict, table_name: str, llm_client: LLMClient) -> str:
    """
    Classify a column as dimension, time_dimension, or fact.
    
    Rules (matching target format):
    - IDs (primary or foreign keys) -> dimension (never facts)
    - Date/datetime types -> time_dimension
    - Boolean and categorical strings -> dimension
    - Numeric measurements (amounts, counts, rates) -> fact with access_modifier
    """
    col_name = col_info["column_name"]
    col_type = col_info.get("type", "string")
    data_expr = col_info.get("data_expression", "")
    is_pk = col_info.get("is_primary_key", False)
    is_fk = col_info.get("is_foreign_key", False)
    
    # IDs are ALWAYS dimensions (primary keys or foreign keys)
    if is_pk or is_fk or "_id" in col_name.lower():
        return "dimension"
    
    # Date/time columns
    if col_type in ["date", "datetime", "timestamp", "time"]:
        return "time_dimension"
    
    # Check column name patterns for dates
    date_patterns = ["date", "time", "created", "updated", "modified", "datetime", "timestamp", "_at", "_on"]
    if any(pattern in col_name.lower() for pattern in date_patterns):
        return "time_dimension"
    
    # Boolean types are dimensions
    if col_type in ["boolean", "bool"]:
        return "dimension"
    
    # Numeric types - check if they're measurements/facts or dimensional attributes
    if col_type in ["int32", "int64", "float", "double", "decimal", "number"]:
        # Fact patterns: measurements, amounts, counts, rates
        fact_patterns = [
            "amount", "total", "sum", "price", "cost", "rate", "salary",
            "revenue", "profit", "tax", "fee", "charge", "payment", "balance",
            "quantity", "points", "score", "rating", "capacity", "length", "nights",
            "adults", "children", "subtotal", "discount", "weight", "height",
            "count", "days", "reservations"
        ]
        
        # Dimension patterns: identifiers, codes, levels
        dimension_patterns = ["number", "floor", "level", "year", "month", "day", "postcode", "zip"]
        
        if any(pattern in col_name.lower() for pattern in fact_patterns):
            return "fact"
        elif any(pattern in col_name.lower() for pattern in dimension_patterns):
            return "dimension"
        else:
            # Default numeric to fact (measurements)
            return "fact"
    
    # String types are dimensions
    if col_type in ["string", "text", "varchar", "char"]:
        return "dimension"
    
    # Default to dimension
    return "dimension"


def _infer_data_type(col_type: str, col_name: str = "") -> str:
    """Map tablefaker types to Snowflake SQL types with precision."""
    # Check for specific patterns to determine precision
    if col_type in ["int32", "int64", "number"]:
        # IDs and counts are NUMBER(38,0)
        if any(p in col_name.lower() for p in ["_id", "count", "nights", "adults", "children", "capacity", "floor", "number", "days", "reservations"]):
            return "NUMBER(38,0)"
        # Monetary values are NUMBER(38,2)
        elif any(p in col_name.lower() for p in ["amount", "total", "price", "rate", "revenue", "cost", "salary", "tax", "subtotal", "payment"]):
            return "NUMBER(38,2)"
        else:
            return "NUMBER(38,0)"
    elif col_type in ["float", "double", "decimal"]:
        # Float types with precision
        if any(p in col_name.lower() for p in ["rating"]):
            return "NUMBER(38,1)"
        elif any(p in col_name.lower() for p in ["amount", "total", "price", "rate", "revenue", "cost", "salary", "tax", "subtotal", "payment"]):
            return "NUMBER(38,2)"
        else:
            return "NUMBER(38,2)"
    
    type_mapping = {
        "string": "VARCHAR(16777216)",
        "text": "VARCHAR(16777216)",
        "varchar": "VARCHAR(16777216)",
        "char": "VARCHAR(16777216)",
        "boolean": "BOOLEAN",
        "bool": "BOOLEAN",
        "date": "DATE",
        "datetime": "DATE",
        "timestamp": "DATE",
        "time": "TIME",
    }
    return type_mapping.get(col_type, "VARCHAR(16777216)")


def _generate_description_with_llm(table_name: str, col_name: str, col_type: str,
                                    data_expr: str, classification: str,
                                    llm_client: LLMClient) -> str:
    """Generate a description using LLM if available and enabled.
    For testing, when LLM is disabled or fails, return the literal "none" so callers can
    validate YAML schema without depending on LLM output."""
    if not llm_client.is_enabled():
        return "none"
    
    try:
        description = llm_client.generate_column_description(
            table_name, col_name, col_type, data_expr, classification
        )
        return description
    except Exception as e:
        print(f"Warning: LLM call failed for {table_name}.{col_name}: {str(e)}")
        return "none"


def _build_semantic_model(table_metadata: Dict, relationships: List[Dict],
                          llm_client: LLMClient) -> Dict:
    """Build the complete semantic model structure matching target format."""
    
    # Generate model name from first table - uppercase
    first_table = list(table_metadata.keys())[0] if table_metadata else "model"
    model_name = f"{first_table.upper()}_SEMANTIC_VIEW"
    
    # Generate model description with LLM (return "none" when disabled or on failure for testing)
    if llm_client.is_enabled():
        try:
            model_description = llm_client.generate_model_description(list(table_metadata.keys()))
        except Exception as e:
            print(f"Warning: LLM call failed for model description: {str(e)}")
            model_description = "none"
    else:
        model_description = "none"
    
    semantic_model = {
        "name": model_name,
        "description": model_description,
        "tables": []
    }
    
    # Include relationships section when available so downstream tools (metrics gen)
    # can leverage model relationships for cross-table metrics and correct placement.
    if relationships:
        # Ensure relationships is a list of mappings in the expected shape.
        semantic_model["relationships"] = relationships
    
    # Build logical tables
    for table_name, table_info in table_metadata.items():
        # Generate table description with LLM (return "none" when disabled or on failure for testing)
        if llm_client.is_enabled():
            try:
                col_names = [col["column_name"] for col in table_info["columns"]]
                table_desc = llm_client.generate_table_description(table_name, col_names)
            except Exception as e:
                print(f"Warning: LLM call failed for table {table_name}: {str(e)}")
                table_desc = "none"
        else:
            table_desc = "none"
        
        logical_table = {
            "name": table_name.upper(),
            "description": table_desc,
            "base_table": {
                "database": "<database>",
                "schema": "<schema>",
                "table": table_name.upper()
            }
        }
        
        # Include primary key information from source config if present
        if table_info.get("primary_keys"):
            logical_table["primary_key"] = {
                "columns": [pk.upper() for pk in table_info["primary_keys"]]
            }
        
        # Classify and organize columns
        dimensions = []
        time_dimensions = []
        facts = []
        
        for col_info in table_info["columns"]:
            classification = _classify_column(col_info, table_name, llm_client)
            col_name = col_info["column_name"]
            col_type = col_info.get("type", "string")
            data_expr = col_info.get("data_expression", "")
            
            # Generate description
            description = _generate_description_with_llm(
                table_name, col_name, col_type, data_expr,
                classification, llm_client
            )
            
            col_def = {
                "name": col_name.upper(),
                "description": description,
                "expr": col_name.upper() if classification == "dimension" or classification == "time_dimension" else col_name,
                "data_type": _infer_data_type(col_type, col_name)
            }
            
            # Add access_modifier for facts only
            if classification == "fact":
                col_def["access_modifier"] = "public_access"
            
            # Don't add unique flag (not in target format)
            
            # Add to appropriate list
            if classification == "dimension":
                dimensions.append(col_def)
            elif classification == "time_dimension":
                time_dimensions.append(col_def)
            elif classification == "fact":
                facts.append(col_def)
        
        # Add column sections to logical table (only if non-empty)
        if dimensions:
            logical_table["dimensions"] = dimensions
        
        if time_dimensions:
            logical_table["time_dimensions"] = time_dimensions
        
        if facts:
            logical_table["facts"] = facts
        
        semantic_model["tables"].append(logical_table)
    
    # Don't add relationships section (not in target format)
    
    return semantic_model