# subnx/stats.py
import time
import psutil
from .models import ClientHardwareResourcesStatistics

def get_system_stats(c: ClientHardwareResourcesStatistics) -> None:
    try:
        c.cpu_usage = psutil.cpu_percent(interval=0.0)
        c.memory_usage = psutil.virtual_memory().percent
        c.disk_usage = psutil.disk_usage('/').percent

        io_start = psutil.disk_io_counters()
        time.sleep(1.0) 
        io_end = psutil.disk_io_counters()

        if io_start and io_end:
            total_io = (io_end.read_bytes - io_start.read_bytes) + (io_end.write_bytes - io_start.write_bytes)
            if total_io > 0:
                max_throughput = float(100 * 1024 * 1024) 
                disk_busy = (float(total_io) / max_throughput) * 100.0
                if disk_busy > 100.0:
                    disk_busy = 100.0
                c.disk_busy = disk_busy

    except Exception as e:
        raise Exception(f"error obteniendo métricas: {e}")