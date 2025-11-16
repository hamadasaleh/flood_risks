"""
Progress bar tracking for CaMa-Flood simulations by parsing log files.
"""
import re
import time
from datetime import datetime, timedelta
from pathlib import Path
from threading import Event, Thread
from typing import Optional, Tuple

try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False


class CaMaFloodProgressTracker:
    """
    Tracks simulation progress by parsing the CaMa-Flood log file.
    
    Monitors the log file in a background thread and updates a progress bar
    based on timestamps found in the log.
    """
    
    # Regex pattern to match progress lines: "CMF::DRV_ADVANCE END: KSTEP, time (end of Tstep):        1536    19800305           0"
    PROGRESS_PATTERN = re.compile(
        r'CMF::DRV_ADVANCE END:.*?(\d{8})\s+(\d{4})'
    )
    
    def __init__(
        self,
        log_file: Path,
        start_date: datetime,
        end_date: datetime,
        enabled: bool = True
    ):
        """
        Initialize progress tracker.
        
        Args:
            log_file: Path to log_CaMa.txt file
            start_date: Simulation start date
            end_date: Simulation end date
            enabled: Whether to show progress bar (default: True)
        """
        self.log_file = log_file
        self.start_date = start_date
        self.end_date = end_date
        self.enabled = enabled and TQDM_AVAILABLE
        
        self.current_date: Optional[datetime] = None
        self.total_duration = (end_date - start_date).total_seconds()
        self.progress_bar: Optional[tqdm] = None
        self.monitor_thread: Optional[Thread] = None
        self.stop_event = Event()
        self.last_position = 0
        
    def _parse_timestamp(self, date_str: str, time_str: str) -> Optional[datetime]:
        """
        Parse timestamp from log format.
        
        Args:
            date_str: YYYYMMDD format (e.g., "19800305")
            time_str: HHMM format (e.g., "1200" or "0")
            
        Returns:
            datetime object or None if parsing fails
        """
        try:
            year = int(date_str[:4])
            month = int(date_str[4:6])
            day = int(date_str[6:8])
            
            # Handle time format (can be "1200" or "0" for 00:00)
            time_str = time_str.zfill(4)  # Pad to 4 digits
            hour = int(time_str[:2])
            minute = int(time_str[2:4])
            
            return datetime(year, month, day, hour, minute)
        except (ValueError, IndexError):
            return None
    
    def _read_last_lines(self, num_bytes: int = 16384) -> str:
        """
        Read the last N bytes from the log file.
        
        Args:
            num_bytes: Number of bytes to read from end of file
            
        Returns:
            Last portion of file as string
        """
        if not self.log_file.exists():
            return ""
        
        try:
            with open(self.log_file, 'rb') as f:
                # Seek to end, then read backwards
                file_size = f.seek(0, 2)  # Seek to end
                if file_size == 0:
                    return ""
                
                # Read last N bytes (or entire file if smaller)
                read_size = min(num_bytes, file_size)
                f.seek(-read_size, 2)  # Seek backwards from end
                
                # Read and decode
                content = f.read(read_size)
                # Try to decode, handling potential encoding issues
                try:
                    return content.decode('utf-8')
                except UnicodeDecodeError:
                    # Fallback to latin-1 which can decode any byte
                    return content.decode('latin-1', errors='ignore')
        except (IOError, OSError):
            return ""
    
    def _find_latest_progress(self) -> Optional[datetime]:
        """
        Find the latest progress timestamp in the log file.
        
        Returns:
            Latest datetime found, or None if not found
        """
        content = self._read_last_lines()
        if not content:
            return None
        
        # Find all matches in the content
        matches = self.PROGRESS_PATTERN.findall(content)
        if not matches:
            return None
        
        # Get the last match (most recent)
        date_str, time_str = matches[-1]
        return self._parse_timestamp(date_str, time_str)
    
    def _calculate_progress(self, current_date: datetime) -> Tuple[float, str]:
        """
        Calculate progress percentage and status string.
        
        Args:
            current_date: Current simulation date
            
        Returns:
            Tuple of (progress_percentage, status_string)
        """
        if current_date < self.start_date:
            return 0.0, f"Starting... ({current_date.strftime('%Y-%m-%d %H:%M')})"
        
        if current_date >= self.end_date:
            return 100.0, f"Complete ({self.end_date.strftime('%Y-%m-%d %H:%M')})"
        
        elapsed = (current_date - self.start_date).total_seconds()
        progress = min(100.0, max(0.0, (elapsed / self.total_duration) * 100))
        
        # Calculate ETA
        if progress > 0:
            elapsed_wall = time.time() - self.start_time
            estimated_total = elapsed_wall / (progress / 100.0)
            remaining = estimated_total - elapsed_wall
            eta_str = f"ETA: {timedelta(seconds=int(remaining))}"
        else:
            eta_str = "ETA: calculating..."
        
        status = f"{current_date.strftime('%Y-%m-%d %H:%M')} | {eta_str}"
        return progress, status
    
    def _monitor_loop(self):
        """Background thread loop to monitor log file and update progress."""
        self.start_time = time.time()
        last_update_time = 0
        update_interval = 0.5  # Update every 0.5 seconds
        
        while not self.stop_event.is_set():
            current_date = self._find_latest_progress()
            
            if current_date:
                self.current_date = current_date
                progress, status = self._calculate_progress(current_date)
                
                # Update progress bar
                if self.progress_bar is not None:
                    self.progress_bar.n = int(progress)
                    self.progress_bar.set_postfix_str(status)
                    self.progress_bar.refresh()
            
            # Sleep with periodic check for stop event
            time.sleep(update_interval)
    
    def start(self):
        """Start monitoring progress in background thread."""
        if not self.enabled:
            return
        
        # Create progress bar
        self.progress_bar = tqdm(
            total=100,
            unit='%',
            desc="Simulation",
            bar_format='{l_bar}{bar}| {n:.1f}% [{elapsed}<{remaining}, {postfix}]',
            ncols=100
        )
        
        # Start monitoring thread
        self.monitor_thread = Thread(target=self._monitor_loop, daemon=True)
        self.monitor_thread.start()
    
    def stop(self):
        """Stop monitoring and close progress bar."""
        if not self.enabled:
            return
        
        # Signal thread to stop
        self.stop_event.set()
        
        # Wait for thread to finish (with timeout)
        if self.monitor_thread:
            self.monitor_thread.join(timeout=1.0)
        
        # Close progress bar
        if self.progress_bar is not None:
            # Set to 100% if simulation completed
            if self.current_date and self.current_date >= self.end_date:
                self.progress_bar.n = 100
                self.progress_bar.set_postfix_str("Complete")
            self.progress_bar.close()
            self.progress_bar = None

