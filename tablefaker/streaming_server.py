#!/usr/bin/env python3
"""
Streaming Data Generator Server

Continuously generates and appends fake data to Delta/Parquet tables based on 
per-table cadence settings defined in YAML configuration.

Features:
- Per-table update policies (append, disabled, replace)
- Configurable rows-per-minute cadence
- FK and copy_from_fk consistency via in-memory caches
- Atomic Delta/Parquet appends
- Graceful shutdown on KeyboardInterrupt
- Automatic cache loading from existing data on startup

Requirements:
- pip install deltalake pandas pyarrow

Usage:
    python streaming_server.py --config streaming_config.yaml --output ./delta_tables
"""

import argparse
import time
import signal
import sys
import threading
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional

import pandas as pd
import yaml

# Import tablefaker components
sys.path.insert(0, str(Path(__file__).parent.parent))
from tablefaker.tablefaker import TableFaker
from tablefaker.config import Config
from tablefaker import util

try:
    import deltalake
    from deltalake import write_deltalake, DeltaTable
except ImportError:
    print("Error: deltalake package not installed. Run: pip install deltalake")
    sys.exit(1)


class CycleBarrier:
    """
    Synchronization barrier for coordinating streaming cycle completion.
    When all threads reach the barrier, one thread is designated to run postprocess.
    """
    
    def __init__(self, num_threads):
        """
        Args:
            num_threads: Number of streaming threads to wait for
        """
        self.num_threads = num_threads
        self.counter = 0
        self.cycle_number = 0
        self.lock = threading.Lock()
        self.condition = threading.Condition(self.lock)
        self.timeout_seconds = 60  # Prevent deadlock
        
        util.log(f"CycleBarrier initialized for {num_threads} threads",
                util.FOREGROUND_COLOR.CYAN)
    
    def wait(self) -> bool:
        """
        Wait at barrier until all threads arrive.
        
        Returns:
            True if this thread should trigger postprocess execution
            False otherwise
        """
        with self.condition:
            self.counter += 1
            
            if self.counter == self.num_threads:
                # Last thread to arrive - trigger postprocess
                self.cycle_number += 1
                util.log(f"Cycle {self.cycle_number} completed by all threads",
                        util.FOREGROUND_COLOR.GREEN)
                self.counter = 0
                self.condition.notify_all()
                return True  # This thread runs postprocess
            else:
                # Wait for other threads
                self.condition.wait(timeout=self.timeout_seconds)
                
                # Check if we timed out
                if self.counter > 0:
                    util.log(f"CycleBarrier timeout - possible thread failure",
                            util.FOREGROUND_COLOR.YELLOW)
                    self.counter = 0  # Reset to prevent deadlock
                
                return False


class StreamingTableGenerator:
    """Manages streaming data generation for a single table."""
    
    def __init__(self, table_config: dict, table_faker: TableFaker, 
                 configurator: Config, output_path: Path):
        self.table_name = table_config['table_name']
        self.table_config = table_config
        self.table_faker = table_faker
        self.configurator = configurator
        self.output_path = output_path / self.table_name
        
        # Parse cadence settings
        cadence = table_config.get('cadence', {})
        self.rows_per_minute = cadence.get('rows_per_minute', 60)
        self.enabled = cadence.get('enabled', True)
        self.update_policy = table_config.get('update_policy', 'append')
        
        # Calculate interval between batches (generate every 10 seconds)
        self.tick_interval = 10  # seconds
        self.rows_per_tick = max(1, int(self.rows_per_minute * self.tick_interval / 60))
        
        # Track current row offset
        self.current_row_id = table_config.get('start_row_id', 1)
        
        # Thread control
        self.running = False
        self.thread = None
        
        # Cycle coordination
        self.cycle_barrier = None  # Set by StreamingServer
        self.server = None  # Reference to StreamingServer for postprocess callback
        
        util.log(f"[{self.table_name}] Configured: {self.rows_per_minute} rows/min, "
                f"{self.rows_per_tick} rows per {self.tick_interval}s tick",
                util.FOREGROUND_COLOR.CYAN)
    
    def load_existing_data(self):
        """Load existing Delta table data into TableFaker caches."""
        if not self.output_path.exists():
            util.log(f"[{self.table_name}] No existing data, starting fresh", 
                    util.FOREGROUND_COLOR.YELLOW)
            return
        
        try:
            # Read existing Delta table
            dt = DeltaTable(str(self.output_path))
            df = dt.to_pandas()
            
            if len(df) == 0:
                util.log(f"[{self.table_name}] Empty table, starting fresh", 
                        util.FOREGROUND_COLOR.YELLOW)
                return
            
            util.log(f"[{self.table_name}] Loading {len(df)} existing rows into cache", 
                    util.FOREGROUND_COLOR.CYAN)
            
            # Find primary key columns
            pk_cols = [c['column_name'] for c in self.table_config['columns'] 
                      if c.get('is_primary_key')]
            
            if not pk_cols:
                util.log(f"[{self.table_name}] No primary keys, skipping cache", 
                        util.FOREGROUND_COLOR.YELLOW)
                return
            
            # Populate primary_key_cache
            if self.table_name not in self.table_faker.primary_key_cache:
                self.table_faker.primary_key_cache[self.table_name] = {}
            
            for pk_col in pk_cols:
                if pk_col in df.columns:
                    self.table_faker.primary_key_cache[self.table_name][pk_col] = \
                        df[pk_col].tolist()
            
            # Populate parent_rows cache for copy_from_fk
            if pk_cols:
                self.table_faker.parent_rows.setdefault(self.table_name, {})
                for _, row in df.iterrows():
                    row_dict = row.to_dict()
                    for pk_col in pk_cols:
                        self.table_faker.parent_rows[self.table_name][row_dict[pk_col]] = row_dict
            
            # Update current_row_id to continue from max + 1
            max_id = df[pk_cols[0]].max()
            self.current_row_id = max_id + 1
            
            util.log(f"[{self.table_name}] Cache loaded, next row_id: {self.current_row_id}", 
                    util.FOREGROUND_COLOR.GREEN)
            
        except Exception as e:
            util.log(f"[{self.table_name}] Error loading existing data: {e}", 
                    util.FOREGROUND_COLOR.RED)
    
    def generate_and_append_batch(self):
        """Generate a batch of rows and append to Delta table."""
        try:
            # Create a modified table config for this batch
            batch_config = self.table_config.copy()
            batch_config['row_count'] = self.rows_per_tick
            batch_config['start_row_id'] = self.current_row_id
            
            util.log(f"[{self.table_name}] Generating {self.rows_per_tick} rows "
                    f"starting at row_id {self.current_row_id}",
                    util.FOREGROUND_COLOR.CYAN)
            
            # Generate rows using TableFaker
            df = self.table_faker.generate_table(
                batch_config,
                self.configurator,
                internal_start_row_id=0,
                internal_row_count=self.rows_per_tick
            )
            
            # Append to Delta table
            if not self.output_path.exists():
                # Create new Delta table
                write_deltalake(str(self.output_path), df, mode="overwrite")
                util.log(f"[{self.table_name}] Created new Delta table at {self.output_path}", 
                        util.FOREGROUND_COLOR.GREEN)
            else:
                # Append to existing Delta table
                write_deltalake(str(self.output_path), df, mode="append")
                util.log(f"[{self.table_name}] Appended {len(df)} rows to Delta table", 
                        util.FOREGROUND_COLOR.GREEN)
            
            # Update row_id counter
            self.current_row_id += self.rows_per_tick
            
        except Exception as e:
            util.log(f"[{self.table_name}] Error generating/appending batch: {e}", 
                    util.FOREGROUND_COLOR.RED)
            import traceback
            traceback.print_exc()
    
    def run_loop(self):
        """Main loop that generates batches at the configured interval."""
        util.log(f"[{self.table_name}] Starting generation loop",
                util.FOREGROUND_COLOR.GREEN)
        
        while self.running:
            try:
                start_time = time.time()
                
                # Generate and append batch
                self.generate_and_append_batch()
                
                # Wait at cycle barrier if coordinating
                if self.cycle_barrier is not None:
                    should_trigger_postprocess = self.cycle_barrier.wait()
                    
                    if should_trigger_postprocess and self.server is not None:
                        # This thread won the race - trigger postprocess
                        util.log(f"[{self.table_name}] Triggering postprocess execution",
                                util.FOREGROUND_COLOR.CYAN)
                        self.server.execute_postprocess_tables()
                
                # Sleep for remaining interval time
                elapsed = time.time() - start_time
                sleep_time = max(0, self.tick_interval - elapsed)
                
                if sleep_time > 0:
                    time.sleep(sleep_time)
                    
            except Exception as e:
                util.log(f"[{self.table_name}] Error in streaming loop: {e}",
                        util.FOREGROUND_COLOR.RED)
                # Continue running despite errors
        
        util.log(f"[{self.table_name}] Thread stopped",
                util.FOREGROUND_COLOR.YELLOW)
    
    def start(self):
        """Start the generation thread."""
        if self.update_policy == 'disabled':
            util.log(f"[{self.table_name}] Update policy is 'disabled', skipping", 
                    util.FOREGROUND_COLOR.YELLOW)
            return
        
        if not self.enabled:
            util.log(f"[{self.table_name}] Cadence not enabled, skipping", 
                    util.FOREGROUND_COLOR.YELLOW)
            return
        
        self.running = True
        self.thread = threading.Thread(target=self.run_loop, daemon=True)
        self.thread.start()
        util.log(f"[{self.table_name}] Generation thread started", 
                util.FOREGROUND_COLOR.GREEN)
    
    def stop(self):
        """Stop the generation thread."""
        if self.thread and self.running:
            util.log(f"[{self.table_name}] Stopping generation thread", 
                    util.FOREGROUND_COLOR.YELLOW)
            self.running = False
            self.thread.join(timeout=5)


class StreamingServer:
    """Manages multiple streaming table generators."""
    
    def __init__(self, config_path: Path, output_path: Path):
        self.config_path = config_path
        self.output_path = output_path
        self.output_path.mkdir(parents=True, exist_ok=True)
        
        # Initialize TableFaker instance (shared across all tables)
        self.table_faker = TableFaker()
        
        # Load configuration
        self.configurator = Config(str(config_path))
        
        # Apply seed if configured
        seed = self.configurator.config.get('config', {}).get('seed')
        if seed is not None:
            self.table_faker._apply_seed(seed)
            util.log(f"Applied seed: {seed}", util.FOREGROUND_COLOR.GREEN)
        
        # Create generators mapped by table name
        self.generators_by_name = {}
        tables = self.configurator.config.get('tables', [])
        for table_config in tables:
            gen = StreamingTableGenerator(
                table_config,
                self.table_faker,
                self.configurator,
                self.output_path
            )
            self.generators_by_name[table_config.get('table_name')] = gen

        # Order generators deterministically based on FK dependencies (parents before children)
        # actual ordering logic is implemented as class methods outside __init__
        self.generators = self._order_generators()
        
        # Track postprocess tables
        self.postprocess_tables = [g for g in self.generators
                                   if g.update_policy == 'postprocess']
        
        if self.postprocess_tables:
            util.log(f"Found {len(self.postprocess_tables)} postprocess tables",
                    util.FOREGROUND_COLOR.CYAN)
        
        # Cycle barrier for coordinating streaming cycles
        self.cycle_barrier = None

        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        self.running = False
    def _extract_dependencies(self):
        """Return dict table_name -> set(parent_table_names) parsed from foreign_key and copy_from_fk calls."""
        tables = self.configurator.config.get('tables', [])
        deps = {}
        for t in tables:
            name = t.get('table_name')
            deps.setdefault(name, set())
            for col in t.get('columns', []):
                cmd = str(col.get('data', ''))
                # Check for foreign_key() dependencies
                if 'foreign_key(' in cmd:
                    try:
                        idx = cmd.find('foreign_key(')
                        end_idx = cmd.find(')', idx)
                        if end_idx != -1:
                            args_str = cmd[idx + len('foreign_key('):end_idx]
                            parsed = eval(f"({args_str})")
                            if len(parsed) >= 1:
                                parent = parsed[0]
                                deps[name].add(parent)
                    except Exception:
                        # ignore parse errors here
                        pass
                # ALSO check for copy_from_fk() dependencies
                if 'copy_from_fk(' in cmd:
                    try:
                        idx = cmd.find('copy_from_fk(')
                        end_idx = cmd.find(')', idx)
                        if end_idx != -1:
                            args_str = cmd[idx + len('copy_from_fk('):end_idx]
                            parsed = eval(f"({args_str})")
                            if len(parsed) >= 2:
                                parent = parsed[1]  # second arg is the parent table name
                                deps[name].add(parent)
                    except Exception:
                        # ignore parse errors here
                        pass
        return deps

    def _topological_sort(self, deps):
        """
        Kahn's algorithm adapted: deps maps node -> set(parents).
        Produce ordered list of generator objects such that parents come before children.
        """
        # Build set of all nodes
        nodes = set(deps.keys()) | {p for parents in deps.values() for p in parents}
        # Build children map and in-degree
        children = {n: set() for n in nodes}
        indeg = {n: 0 for n in nodes}
        for node, parents in deps.items():
            for p in parents:
                children.setdefault(p, set()).add(node)
                indeg[node] = indeg.get(node, 0) + 1
        # Start with nodes that have zero in-degree (no parents)
        queue = [n for n in nodes if indeg.get(n, 0) == 0]
        order = []
        while queue:
            n = queue.pop(0)
            if n in self.generators_by_name:
                order.append(self.generators_by_name[n])
            for c in children.get(n, set()):
                indeg[c] -= 1
                if indeg[c] == 0:
                    queue.append(c)
        configured_count = len(self.generators_by_name)
        if len(order) != configured_count:
            missing = [name for name in self.generators_by_name.keys() if name not in [g.table_name for g in order]]
            raise Exception(f"Circular or missing dependencies detected among tables; unresolved: {missing}")
        return order

    def _order_generators(self):
        deps = self._extract_dependencies()
        try:
            ordered = self._topological_sort(deps)
            util.log(f"Tables will start in dependency order: {[g.table_name for g in ordered]}", util.FOREGROUND_COLOR.CYAN)
            return ordered
        except Exception as e:
            util.log(f"Dependency ordering failed: {e}", util.FOREGROUND_COLOR.RED)
            util.log("Falling back to config order", util.FOREGROUND_COLOR.YELLOW)
            # fallback: preserve config order for tables present
            return [self.generators_by_name[name] for name in [t.get('table_name') for t in self.configurator.config.get('tables', [])] if name in self.generators_by_name]

    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        util.log("\nReceived shutdown signal, stopping gracefully...", 
                util.FOREGROUND_COLOR.YELLOW)
        self.stop()
    
    def start(self):
        """Start all table generators."""
        util.log(f"Starting streaming server with {len(self.generators)} tables",
                util.FOREGROUND_COLOR.GREEN)
        
        # 1. Load existing data into caches first
        for generator in self.generators:
            generator.load_existing_data()
        
        # 2. Generate initial data for ALL tables with update_policy='disabled'
        deps = self._extract_dependencies()
        for generator in self.generators:
            if generator.update_policy == 'disabled':
                # Check dependencies (existing logic)
                table_deps = deps.get(generator.table_name, set())
                missing_deps = [dep for dep in table_deps
                               if dep not in self.table_faker.primary_key_cache]
                
                if missing_deps:
                    util.log(f"[{generator.table_name}] Skipping - depends on {missing_deps}",
                            util.FOREGROUND_COLOR.YELLOW)
                    continue
                
                # Generate full table
                util.log(f"[{generator.table_name}] Generating static data (update_policy=disabled)",
                        util.FOREGROUND_COLOR.CYAN)
                original_row_count = generator.table_config.get('row_count', 100)
                original_rows_per_tick = generator.rows_per_tick
                generator.rows_per_tick = original_row_count
                generator.generate_and_append_batch()
                generator.rows_per_tick = original_rows_per_tick
        
        # 3. Create cycle barrier for streaming tables (if any)
        streaming_gens = [g for g in self.generators
                         if g.update_policy == 'append' and g.enabled]
        
        if len(streaming_gens) > 0:
            self.cycle_barrier = CycleBarrier(len(streaming_gens))
            
            # Assign barrier and server reference to each streaming generator
            for gen in streaming_gens:
                gen.cycle_barrier = self.cycle_barrier
                gen.server = self
        
        # 4. Generate initial batch for streaming parent tables
        for generator in streaming_gens:
            table_deps = deps.get(generator.table_name, set())
            if not table_deps and generator.table_name not in self.table_faker.primary_key_cache:
                util.log(f"[{generator.table_name}] Generating initial batch",
                        util.FOREGROUND_COLOR.CYAN)
                generator.generate_and_append_batch()
        
        # 5. Start all generator threads (append tables only)
        for generator in self.generators:
            if generator.update_policy == 'append' and generator.enabled:
                generator.start()
        
        self.running = True
        
        util.log("Server running. Postprocess tables will execute after first cycle.",
                util.FOREGROUND_COLOR.GREEN)
        
        # Main thread keeps running
        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            pass
        
        self.stop()
    
    def execute_postprocess_tables(self):
        """
        Execute all postprocess tables in dependency order.
        Called by streaming threads after cycle completion.
        """
        if not self.postprocess_tables:
            return
        
        util.log(f"Executing {len(self.postprocess_tables)} postprocess tables",
                util.FOREGROUND_COLOR.CYAN)
        
        deps = self._extract_dependencies()
        
        for generator in self.postprocess_tables:
            try:
                table_name = generator.table_name
                
                # Check if all dependencies are met
                table_deps = deps.get(table_name, set())
                missing_deps = [dep for dep in table_deps
                               if dep not in self.table_faker.primary_key_cache]
                
                if missing_deps:
                    util.log(f"[{table_name}] Skipping postprocess - missing deps: {missing_deps}",
                            util.FOREGROUND_COLOR.YELLOW)
                    continue
                
                # Determine postprocess mode (replace or append)
                postprocess_mode = generator.table_config.get('postprocess_mode', 'replace')
                
                if postprocess_mode == 'replace':
                    # Delete existing data before regenerating
                    if generator.output_path.exists():
                        util.log(f"[{table_name}] Replacing existing data (postprocess_mode=replace)",
                                util.FOREGROUND_COLOR.CYAN)
                        # Delete the Delta table directory
                        import shutil
                        shutil.rmtree(generator.output_path)
                        generator.output_path.mkdir(parents=True, exist_ok=True)
                        
                        # Clear from caches
                        if table_name in self.table_faker.primary_key_cache:
                            del self.table_faker.primary_key_cache[table_name]
                        if table_name in self.table_faker.parent_rows:
                            del self.table_faker.parent_rows[table_name]
                        
                        # Reset row_id
                        generator.current_row_id = generator.table_config.get('start_row_id', 1)
                
                # Generate full table
                util.log(f"[{table_name}] Generating postprocess table (mode={postprocess_mode})",
                        util.FOREGROUND_COLOR.CYAN)
                
                original_rows_per_tick = generator.rows_per_tick
                row_count_config = generator.table_config.get('row_count', 100)
                
                # Evaluate row_count if it's a string expression
                if isinstance(row_count_config, str):
                    # Create variables context for evaluation
                    import sys
                    from pathlib import Path
                    import importlib
                    
                    # Add config directory to path
                    config_dir = Path(self.config_path).parent
                    if str(config_dir) not in sys.path:
                        sys.path.insert(0, str(config_dir))
                    
                    variables = {}
                    # get_table should return a list of row dicts for the table
                    def get_table_func(table_name):
                        """Return all rows for a table as a list of dicts."""
                        table_dict = self.table_faker.parent_rows.get(table_name, {})
                        # Convert dict of {pk_value: row_dict} to list of row_dicts
                        return list(table_dict.values()) if table_dict else []
                    variables['get_table'] = get_table_func
                    
                    # Import any modules referenced in the expression
                    # Look for module names before dots (e.g., "hotel_custom_calendar_utils.room_month_len")
                    import re
                    module_names = re.findall(r'([a-zA-Z_][a-zA-Z0-9_]*)\s*\.', row_count_config)
                    for module_name in set(module_names):
                        try:
                            module = importlib.import_module(module_name)
                            variables[module_name] = module
                        except ImportError:
                            pass
                    
                    try:
                        row_count = eval(row_count_config, variables)
                        util.log(f"[{table_name}] Evaluated row_count expression: {row_count_config} => {row_count}",
                                util.FOREGROUND_COLOR.CYAN)
                    except Exception as e:
                        util.log(f"[{table_name}] Error evaluating row_count expression '{row_count_config}': {e}",
                                util.FOREGROUND_COLOR.RED)
                        row_count = 100  # fallback
                else:
                    row_count = row_count_config
                
                # Ensure row_count is an integer
                if not isinstance(row_count, int):
                    util.log(f"[{table_name}] row_count is not an integer ({type(row_count)}), converting",
                            util.FOREGROUND_COLOR.YELLOW)
                    row_count = int(row_count)
                
                generator.rows_per_tick = row_count
                
                generator.generate_and_append_batch()
                
                generator.rows_per_tick = original_rows_per_tick
                
                # Reset row_id after generation for replace mode
                if postprocess_mode == 'replace':
                    generator.current_row_id = generator.table_config.get('start_row_id', 1)
                
                util.log(f"[{table_name}] Postprocess complete ({row_count} rows)",
                        util.FOREGROUND_COLOR.GREEN)
                
            except Exception as e:
                util.log(f"[{generator.table_name}] Postprocess failed: {e}",
                        util.FOREGROUND_COLOR.RED)
                import traceback
                util.log(traceback.format_exc(), util.FOREGROUND_COLOR.RED)
    
    def stop(self):
        """Stop all table generators."""
        if not self.running:
            return
        
        self.running = False
        
        util.log("Stopping all generators...", util.FOREGROUND_COLOR.YELLOW)
        for generator in self.generators:
            generator.stop()
        
        util.log("Server stopped", util.FOREGROUND_COLOR.GREEN)


def main():
    parser = argparse.ArgumentParser(
        description="Streaming fake data generator with Delta/Parquet output"
    )
    parser.add_argument(
        '--config',
        required=True,
        help='Path to YAML configuration file'
    )
    parser.add_argument(
        '--output',
        default='./delta_tables',
        help='Output directory for Delta tables (default: ./delta_tables)'
    )
    
    args = parser.parse_args()
    
    config_path = Path(args.config)
    if not config_path.exists():
        print(f"Error: Configuration file not found: {config_path}")
        sys.exit(1)
    
    output_path = Path(args.output)
    
    util.log(f"Configuration: {config_path}", util.FOREGROUND_COLOR.CYAN)
    util.log(f"Output directory: {output_path}", util.FOREGROUND_COLOR.CYAN)
    
    server = StreamingServer(config_path, output_path)
    server.start()


if __name__ == '__main__':
    main()