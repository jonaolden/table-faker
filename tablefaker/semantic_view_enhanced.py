"""
Enhanced semantic view generator with caching, synonyms, metrics, and split output.
"""
import yaml
from os import path, makedirs
from . import config
from .llm_client import LLMClient
from typing import Dict, List, Any, Optional, Tuple
import re


class DescriptionCache:
    """Cache for column descriptions to reuse across tables."""
    
    def __init__(self):
        self.cache = {}  # (column_name, col_type, classification) -> (description, synonyms)
    
    def get(self, col_name: str, col_type: str, classification: str) -> Optional[Tuple[str, List[str]]]:
        """Get cached description and synonyms for a column."""
        key = (col_name.lower(), col_type, classification)
        return self.cache.get(key)
    
    def set(self, col_name: str, col_type: str, classification: str, description: str, synonyms: List[str]):
        """Cache description and synonyms for a column."""
        key = (col_name.lower(), col_type, classification)
        self.cache[key] = (description, synonyms)


def generate_semantic_view_enhanced(config_source, target_file_path=None, llm_config_path=None):
    """
    Generate enhanced semantic view YAML files from a tablefaker config.
    Creates separate files for base tables and model-level components.
    
    Args:
        config_source: Path to tablefaker config file or dict
        target_file_path: Optional path to write YAML (default: same dir as config)
        llm_config_path: Optional path to llm.config file
    
    Returns:
        Dict with paths to generated files
    """
    
    # Initialize LLM client and description cache
    llm_client = LLMClient(llm_config_path)
    desc_cache = DescriptionCache()
    
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
    
    # Determine output directory
    if isinstance(config_source, str):
        base = path.splitext(path.basename(config_source))[0]
        src_dir = path.dirname(config_source) or "."
    else:
        base = "semantic_model"
        src_dir = "."
    
    if target_file_path in (None, "", "."):
        out_dir = src_dir
    else:
        out_dir = target_file_path if path.isdir(target_file_path) else path.dirname(target_file_path)
    
    makedirs(out_dir, exist_ok=True)
    
    # Generate output files
    output_files = {}
    
    # 1. Generate base table YAML files
    for table_name, table_info in table_metadata.items():
        table_yaml = _build_base_table_yaml(table_name, table_info, llm_client, desc_cache)
        table_file = path.join(out_dir, f"{table_name}_base_table.yml")
        
        with open(table_file, "w", encoding="utf-8") as f:
            yaml.safe_dump(table_yaml, f, sort_keys=False, default_flow_style=False, allow_unicode=True)
        
        output_files[f"{table_name}_base_table"] = table_file
    
    # 2. Generate relationships YAML
    if relationships:
        rel_yaml = {"relationships": relationships}
        rel_file = path.join(out_dir, f"{base}_relationships.yml")
        
        with open(rel_file, "w", encoding="utf-8") as f:
            yaml.safe_dump(rel_yaml, f, sort_keys=False, default_flow_style=False, allow_unicode=True)
        
        output_files["relationships"] = rel_file
    
    # 3. Generate model-level metrics YAML (derived metrics)
    model_metrics = _generate_model_metrics(table_metadata, llm_client)
    if model_metrics:
        metrics_yaml = {"metrics": model_metrics}
        metrics_file = path.join(out_dir, f"{base}_model_metrics.yml")
        
        with open(metrics_file, "w", encoding="utf-8") as f:
            yaml.safe_dump(metrics_yaml, f, sort_keys=False, default_flow_style=False, allow_unicode=True)
        
        output_files["model_metrics"] = metrics_file
    
    # 4. Generate complete semantic model (combines all parts)
    complete_model = _build_complete_semantic_model(
        base, table_metadata, relationships, model_metrics, llm_client
    )
    complete_file = path.join(out_dir, f"{base}_semantic_view.yml")
    
    with open(complete_file, "w", encoding="utf-8") as f:
        yaml.safe_dump(complete_model, f, sort_keys=False, default_flow_style=False, allow_unicode=True)
    
    output_files["complete_model"] = complete_file
    
    return output_files


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
                "null_percentage": col.get("null_percentage", 0)
            }
            
            # Extract foreign key reference if present
            if col_info["is_foreign_key"]:
                fk_match = re.search(r'foreign_key\(["\'](\w+)["\']\s*,\s*["\'](\w+)["\']\)', data_expr)
                if fk_match:
                    col_info["fk_table"] = fk_match.group(1)
                    col_info["fk_column"] = fk_match.group(2)
            
            # Check if this column uses aggregation (potential metric)
            col_info["is_aggregation"] = any(agg in data_expr.upper() for agg in ["SUM(", "AVG(", "COUNT(", "MAX(", "MIN("])
            
            # Check if this column is a filter condition
            col_info["is_filter"] = any(op in data_expr for op in [" IN (", " = ", " > ", " < ", " BETWEEN "])
            
            table_info["columns"].append(col_info)
            
            if is_pk:
                table_info["primary_keys"].append(col_name)
        
        metadata[table_name] = table_info
    
    return metadata


def _extract_relationships(tables: List[Dict]) -> List[Dict]:
    """Extract relationships based on foreign_key() calls."""
    relationships = []
    seen = set()
    
    # Build PK map
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
                if fk_match:
                    right_table = fk_match.group(1)
                    right_col = fk_match.group(2)
                    
                    # Only add if right_col is the PK of right_table
                    if right_table in table_pks and table_pks[right_table] == right_col:
                        rel_key = (left_table, right_table, col_name, right_col)
                        if rel_key not in seen:
                            seen.add(rel_key)
                            relationships.append({
                                "name": f"{left_table}_to_{right_table}",
                                "left_table": left_table,
                                "right_table": right_table,
                                "relationship_columns": [{
                                    "left_column": col_name,
                                    "right_column": right_col
                                }],
                                "join_type": "left_outer",
                                "relationship_type": "many_to_one"
                            })
    
    return relationships


def _classify_column(col_info: Dict, table_name: str) -> str:
    """Classify a column as dimension, time_dimension, or fact."""
    col_name = col_info["column_name"]
    col_type = col_info.get("type", "string")
    
    # Primary keys are always dimensions
    if col_info.get("is_primary_key"):
        return "dimension"
    
    # Foreign keys are always dimensions
    if col_info.get("is_foreign_key"):
        return "dimension"
    
    # Date/time columns
    if col_type in ["date", "datetime", "timestamp", "time"]:
        return "time_dimension"
    
    # Check column name patterns for dates
    date_patterns = ["date", "time", "created", "updated", "modified", "datetime", "timestamp", "_at", "_on"]
    if any(pattern in col_name.lower() for pattern in date_patterns):
        return "time_dimension"
    
    # Numeric types - distinguish between facts and dimensions
    if col_type in ["int32", "int64", "float", "double", "decimal", "number"]:
        fact_patterns = [
            "amount", "total", "sum", "count", "price", "cost", "rate", "salary", 
            "revenue", "profit", "tax", "fee", "charge", "payment", "balance",
            "quantity", "points", "score", "rating", "capacity", "length", "nights",
            "adults", "children", "subtotal", "discount", "weight", "height"
        ]
        
        dimension_patterns = ["_id", "number", "floor", "level", "type", "status", "year", "month", "day"]
        
        if any(pattern in col_name.lower() for pattern in fact_patterns):
            return "fact"
        elif any(pattern in col_name.lower() for pattern in dimension_patterns):
            return "dimension"
        
        if "id" in col_name.lower() or "number" in col_name.lower():
            return "dimension"
        else:
            return "fact"
    
    return "dimension"


def _infer_data_type(col_type: str) -> str:
    """Map tablefaker types to Snowflake SQL types."""
    type_mapping = {
        "string": "VARCHAR",
        "text": "VARCHAR",
        "int32": "NUMBER",
        "int64": "NUMBER",
        "float": "FLOAT",
        "double": "FLOAT",
        "decimal": "DECIMAL",
        "boolean": "BOOLEAN",
        "bool": "BOOLEAN",
        "date": "DATE",
        "datetime": "TIMESTAMP",
        "timestamp": "TIMESTAMP",
        "time": "TIME",
    }
    return type_mapping.get(col_type, "VARCHAR")


def _generate_column_metadata(col_info: Dict, table_name: str, classification: str,
                               llm_client: LLMClient, desc_cache: DescriptionCache) -> Dict:
    """Generate complete metadata for a column including description and synonyms."""
    col_name = col_info["column_name"]
    col_type = col_info.get("type", "string")
    data_expr = col_info.get("data_expression", "")
    
    # Check cache first
    cached = desc_cache.get(col_name, col_type, classification)
    if cached:
        description, synonyms = cached
    else:
        # Generate description
        if llm_client.is_enabled():
            try:
                description = llm_client.generate_column_description(
                    table_name, col_name, col_type, data_expr, classification
                )
                # Generate synonyms
                synonyms = llm_client.generate_synonyms(col_name, description, count=2)
            except Exception as e:
                print(f"Warning: LLM call failed for {table_name}.{col_name}: {str(e)}")
                description = f"The {col_name} column in {table_name}"
                synonyms = []
        else:
            description = f"The {col_name} column in {table_name}"
            synonyms = []
        
        # Cache for reuse
        desc_cache.set(col_name, col_type, classification, description, synonyms)
    
    col_def = {
        "name": col_name,
        "description": description,
        "expr": col_name,
        "data_type": _infer_data_type(col_type)
    }
    
    if synonyms:
        col_def["synonyms"] = synonyms
    
    if col_info.get("is_primary_key"):
        col_def["unique"] = True
    
    return col_def


def _build_base_table_yaml(table_name: str, table_info: Dict, llm_client: LLMClient, 
                            desc_cache: DescriptionCache) -> Dict:
    """Build base table YAML with dimensions, time_dimensions, facts, metrics, and filters."""
    
    # Generate table description
    if llm_client.is_enabled():
        try:
            col_names = [col["column_name"] for col in table_info["columns"]]
            table_desc = llm_client.generate_table_description(table_name, col_names)
        except Exception as e:
            print(f"Warning: LLM call failed for table {table_name}: {str(e)}")
            table_desc = f"Logical table for {table_name}"
    else:
        table_desc = f"Logical table for {table_name}"
    
    base_table_yaml = {
        "name": table_name,
        "description": table_desc,
        "base_table": {
            "database": "<database>",
            "schema": "<schema>",
            "table": table_name
        }
    }
    
    # Add primary key if exists
    if table_info["primary_keys"]:
        base_table_yaml["primary_key"] = {
            "columns": table_info["primary_keys"]
        }
    
    # Classify and organize columns
    dimensions = []
    time_dimensions = []
    facts = []
    metrics = []
    filters = []
    
    for col_info in table_info["columns"]:
        classification = _classify_column(col_info, table_name)
        
        # Check if this is a metric (aggregation)
        if col_info.get("is_aggregation"):
            metric_def = _generate_column_metadata(col_info, table_name, "metric", llm_client, desc_cache)
            metric_def["expr"] = col_info["data_expression"]  # Use actual expression
            metrics.append(metric_def)
            continue
        
        # Check if this is a filter
        if col_info.get("is_filter") and not col_info.get("is_primary_key"):
            filter_def = _generate_column_metadata(col_info, table_name, "filter", llm_client, desc_cache)
            filter_def["expr"] = col_info["data_expression"]
            filters.append(filter_def)
            continue
        
        # Regular columns
        col_def = _generate_column_metadata(col_info, table_name, classification, llm_client, desc_cache)
        
        if classification == "dimension":
            dimensions.append(col_def)
        elif classification == "time_dimension":
            time_dimensions.append(col_def)
        elif classification == "fact":
            facts.append(col_def)
    
    if dimensions:
        base_table_yaml["dimensions"] = dimensions
    if time_dimensions:
        base_table_yaml["time_dimensions"] = time_dimensions
    if facts:
        base_table_yaml["facts"] = facts
    if metrics:
        base_table_yaml["metrics"] = metrics
    if filters:
        base_table_yaml["filters"] = filters
    
    return base_table_yaml


def _generate_model_metrics(table_metadata: Dict, llm_client: LLMClient) -> List[Dict]:
    """Generate model-level derived metrics that combine data from multiple tables."""
    # For now, return empty list - this would require more sophisticated analysis
    # of cross-table calculations that could be done at model level
    return []


def _build_complete_semantic_model(base_name: str, table_metadata: Dict, relationships: List[Dict],
                                    model_metrics: List[Dict], llm_client: LLMClient) -> Dict:
    """Build the complete semantic model combining all components."""
    
    model_name = f"{base_name}_semantic_model"
    
    # Generate model description
    if llm_client.is_enabled():
        try:
            model_description = llm_client.generate_model_description(list(table_metadata.keys()))
        except Exception as e:
            print(f"Warning: LLM call failed for model description: {str(e)}")
            model_description = f"Semantic model containing {len(table_metadata)} tables"
    else:
        model_description = f"Semantic model containing {len(table_metadata)} tables"
    
    complete_model = {
        "name": model_name,
        "description": model_description,
        "tables": []
    }
    
    # Add note about base table files
    complete_model["comments"] = (
        "This is the complete semantic model. "
        "Individual base table YAMLs are available as separate files for modular management."
    )
    
    # We don't rebuild the full tables here - just reference that they exist
    # In practice, you would load them from the base_table files
    complete_model["tables"] = [{"name": name, "note": f"See {name}_base_table.yml for details"} 
                                 for name in table_metadata.keys()]
    
    if relationships:
        complete_model["relationships"] = relationships
    
    if model_metrics:
        complete_model["metrics"] = model_metrics
    
    return complete_model