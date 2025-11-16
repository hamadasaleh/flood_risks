#!/usr/bin/env python3
"""
Standalone script to monitor CaMa-Flood simulation progress from log file.

Usage:
    python monitor_progress.py <log_file> <start_year> <end_year>
    
Example:
    python monitor_progress.py cama_flood/out/my_test_simulation/log_CaMa.txt 1980 1981
"""

import sys
import time
from datetime import datetime
from pathlib import Path

from src.cama_flood_api.progress import CaMaFloodProgressTracker


def main():
    """Main entry point for progress monitor script."""
    if len(sys.argv) < 4:
        print(__doc__)
        print("\nError: Missing required arguments")
        print(f"Usage: {sys.argv[0]} <log_file> <start_year> <end_year>")
        sys.exit(1)
    
    log_file_path = Path(sys.argv[1])
    start_year = int(sys.argv[2])
    end_year = int(sys.argv[3])
    
    # Validate log file exists
    if not log_file_path.exists():
        print(f"Error: Log file not found: {log_file_path}")
        print("Waiting for log file to be created...")
        # Wait for file to appear (with timeout)
        timeout = 60  # 60 seconds
        start_wait = time.time()
        while not log_file_path.exists():
            if time.time() - start_wait > timeout:
                print(f"Timeout: Log file not created within {timeout} seconds")
                sys.exit(1)
            time.sleep(1)
        print(f"Log file found: {log_file_path}")
    
    # Create date range
    start_date = datetime(start_year, 1, 1, 0, 0)
    end_date = datetime(end_year + 1, 1, 1, 0, 0)  # End of end_year
    
    print(f"Monitoring progress:")
    print(f"  Log file: {log_file_path}")
    print(f"  Start date: {start_date.strftime('%Y-%m-%d %H:%M')}")
    print(f"  End date: {end_date.strftime('%Y-%m-%d %H:%M')}")
    print()
    
    # Create and start progress tracker
    tracker = CaMaFloodProgressTracker(
        log_file=log_file_path,
        start_date=start_date,
        end_date=end_date,
        enabled=True
    )
    
    if not tracker.enabled:
        print("Warning: tqdm not available. Progress bar disabled.")
        print("Install tqdm with: pip install tqdm")
        sys.exit(1)
    
    try:
        tracker.start()
        print("Progress monitoring started. Press Ctrl+C to stop.\n")
        
        # Keep running until simulation completes or interrupted
        while True:
            # Check if simulation is complete
            if tracker.current_date and tracker.current_date >= end_date:
                print("\nSimulation appears to be complete!")
                break
            
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\n\nMonitoring stopped by user.")
    finally:
        tracker.stop()
        print("Progress monitor stopped.")


if __name__ == "__main__":
    main()

