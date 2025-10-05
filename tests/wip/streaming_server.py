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
            start_time = time.time()
            
            self.generate_and_append_batch()
            
            # Sleep for remaining interval time
            elapsed = time.time() - start_time
            sleep_time = max(0, self.tick_interval - elapsed)
            
            if sleep_time > 0:
                time.sleep(sleep_time)
    
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
        
        # Create generators for each table
        self.generators: List[StreamingTableGenerator] = []
        
        # Process tables in dependency order (parents before children)
        tables = self.configurator.config['tables']
        
        for table_config in tables:
            generator = StreamingTableGenerator(
                table_config,
                self.table_faker,
                self.configurator,
                self.output_path
            )
            self.generators.append(generator)
        
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        self.running = False
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        util.log("\nReceived shutdown signal, stopping gracefully...", 
                util.FOREGROUND_COLOR.YELLOW)
        self.stop()
    
    def start(self):
        """Start all table generators."""
        util.log(f"Starting streaming server with {len(self.generators)} tables", 
                util.FOREGROUND_COLOR.GREEN)
        
        # Load existing data into caches first
        for generator in self.generators:
            generator.load_existing_data()
        
        # Start all generators
        for generator in self.generators:
            generator.start()
        
        self.running = True
        
        # Main thread keeps running
        util.log("Server is running. Press Ctrl+C to stop.", 
                util.FOREGROUND_COLOR.GREEN)
        
        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            pass
        
        self.stop()
    
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