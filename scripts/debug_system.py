#!/usr/bin/env python3
"""
Script debug nâng cao cho hệ thống Air Quality Streaming
Update: Tự động phát hiện kiến trúc Hybrid (Docker + Local Process)
"""

import subprocess
import sys
import os
import time
import requests
import socket
from datetime import datetime
from cassandra.cluster import Cluster

# Cấu hình màu sắc hiển thị
class Colors:
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    RESET = '\033[0m'
    BOLD = '\033[1m'

def print_header(text):
    print(f"\n{Colors.BOLD}{Colors.CYAN}{'='*60}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.CYAN}   {text}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.CYAN}{'='*60}{Colors.RESET}\n")

def print_status(component, status, detail=""):
    if status == "OK":
        print(f"✅ {Colors.BOLD}{component:<25}{Colors.RESET} : {Colors.GREEN}RUNNING{Colors.RESET} {detail}")
    elif status == "WARNING":
        print(f"⚠️  {Colors.BOLD}{component:<25}{Colors.RESET} : {Colors.YELLOW}WARNING{Colors.RESET} {detail}")
    else:
        print(f"❌ {Colors.BOLD}{component:<25}{Colors.RESET} : {Colors.RED}FAILED{Colors.RESET}  {detail}")

def check_port(host, port):
    try:
        with socket.create_connection((host, port), timeout=2):
            return True
    except (socket.timeout, ConnectionRefusedError):
        return False

def tail_log(filename, lines=10):
    """Đọc n dòng cuối của file log"""
    log_path = os.path.join("logs", filename)
    if not os.path.exists(log_path):
        return "Log file not found."
    
    try:
        # Sử dụng tail command cho hiệu quả
        result = subprocess.run(['tail', '-n', str(lines), log_path], capture_output=True, text=True)
        return result.stdout.strip()
    except Exception:
        return "Could not read logs."

# --- 1. INFRASTRUCTURE LAYER (DOCKER) ---
def check_docker_infrastructure():
    print_header("1. INFRASTRUCTURE LAYER (DOCKER)")
    
    expected_containers = {
        'kafka': 3,      # kafka-1, kafka-2, kafka-3
        'zookeeper': 1,
        'cassandra': 2   # cassandra-1, cassandra-2
    }
    
    try:
        result = subprocess.run(['docker', 'ps', '--format', '{{.Names}}'], capture_output=True, text=True)
        running_containers = result.stdout.strip().split('\n')
        
        counts = {'kafka': 0, 'zookeeper': 0, 'cassandra': 0}
        
        for name in running_containers:
            for key in counts:
                if key in name:
                    counts[key] += 1

        all_ok = True
        for service, count in counts.items():
            expected = expected_containers[service]
            if count >= expected:
                print_status(f"Docker: {service.capitalize()}", "OK", f"({count}/{expected} nodes)")
            elif count > 0:
                print_status(f"Docker: {service.capitalize()}", "WARNING", f"({count}/{expected} nodes - Degraded)")
            else:
                print_status(f"Docker: {service.capitalize()}", "ERROR", "(Not Running)")
                all_ok = False
        
        return all_ok
    except Exception as e:
        print_status("Docker Daemon", "ERROR", str(e))
        return False

# --- 2. APPLICATION LAYER (LOCAL PROCESSES) ---
def check_process(process_name, log_file=None):
    """Kiểm tra process đang chạy bằng lệnh ps"""
    try:
        # Grep process, loại bỏ chính lệnh grep và script hiện tại
        cmd = f"ps aux | grep '{process_name}' | grep -v grep | grep -v debug_system.py"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        
        if result.stdout.strip():
            pid = result.stdout.split()[1]
            print_status(f"Process: {process_name.split('/')[-1]}", "OK", f"(PID: {pid})")
            return True
        else:
            print_status(f"Process: {process_name.split('/')[-1]}", "ERROR", "Stopped")
            if log_file:
                print(f"{Colors.YELLOW}   >>> Last 5 log lines for {process_name}:{Colors.RESET}")
                print(f"{Colors.YELLOW}{tail_log(log_file, 5)}{Colors.RESET}\n")
            return False
    except Exception as e:
        print(f"Error checking process: {e}")
        return False

def check_application_layer():
    print_header("2. APPLICATION LAYER (LOCAL PROCESSES)")
    
    status = {}
    
    # 1. Producer
    status['producer'] = check_process("kafka/producer.py", "producer.log")
    
    # 2. Spark Streaming
    status['spark'] = check_process("spark/streaming_job.py", "spark_streaming.log")
    if status['spark']:
        # Kiểm tra Spark UI port
        if check_port('localhost', 4040):
            print(f"   ℹ️  Spark UI accessible at: {Colors.BLUE}http://localhost:4040{Colors.RESET}")
        else:
            print(f"   ⚠️  Spark UI (port 4040) not accessible yet (Job starting?)")

    # 3. WebSocket Server
    status['websocket'] = check_process("dashboard/websocket_server.py", "websocket_server.log")
    if status['websocket']:
        try:
            r = requests.get("http://localhost:8765/health", timeout=2)
            if r.status_code == 200:
                print(f"   ✅ API Health Check: {Colors.GREEN}OK{Colors.RESET} (http://localhost:8765)")
            else:
                print(f"   ❌ API Health Check: {Colors.RED}Failed{Colors.RESET} (Status: {r.status_code})")
        except:
            print(f"   ❌ API Health Check: {Colors.RED}Connection Refused{Colors.RESET}")

    # 4. Streamlit Dashboard
    status['streamlit'] = check_process("streamlit_app.py") # Tên có thể chỉ là streamlit
    
    return all(status.values())

# --- 3. DATA FLOW & LATENCY ---
def check_data_pipeline():
    print_header("3. DATA PIPELINE & LATENCY CHECK")
    
    try:
        # Connect to Cassandra
        cluster = Cluster(['localhost'], port=9042)
        session = cluster.connect()
        
        # Check Keyspace
        row = session.execute("SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name = 'air_quality'").one()
        if not row:
            print_status("Cassandra Schema", "ERROR", "Keyspace 'air_quality' not found")
            return
            
        session.set_keyspace('air_quality')
        
        # Check Data Count
        count = session.execute("SELECT COUNT(*) FROM realtime_data").one()[0]
        
        if count == 0:
            print_status("Data Storage", "WARNING", "Table exists but NO DATA found.")
            print(f"   ℹ️  Check: Is Producer running? Is Spark Job actually writing?")
            return

        # Check Latency (Freshness)
        # Lấy record có processed_at mới nhất (cần allow filtering hoặc index, ở đây lấy mẫu limit)
        # Vì Cassandra khó sort global, ta lấy 100 record mới nhất theo datetime
        rows = session.execute("SELECT datetime, processed_at FROM realtime_data LIMIT 100")
        latest_record = None
        
        # Sort in Python
        data_list = list(rows)
        if data_list:
            data_list.sort(key=lambda x: x.datetime, reverse=True)
            latest_record = data_list[0]
        
        if latest_record:
            # Parse datetime
            try:
                # Định dạng trong code producer: isoformat()
                data_time = datetime.fromisoformat(latest_record.datetime)
                now = datetime.now(data_time.tzinfo)
                
                latency = (now - data_time).total_seconds()
                
                msg = f"{count} records total. Latest: {latency:.1f}s ago"
                
                if latency < 30:
                    print_status("Data Freshness", "OK", msg)
                elif latency < 120:
                    print_status("Data Freshness", "WARNING", msg)
                else:
                    print_status("Data Freshness", "ERROR", f"{msg} (Stale Data)")
            except Exception as e:
                print_status("Data Freshness", "WARNING", f"Found data but error parsing time: {e}")
        
        cluster.shutdown()
        
    except Exception as e:
        print_status("Cassandra Connection", "ERROR", str(e))

def main():
    print(f"\n🚀 {Colors.BOLD}AIR QUALITY SYSTEM DIAGNOSTIC TOOL{Colors.RESET}")
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 1. Check Infrastructure
    check_docker_infrastructure()
    
    # 2. Check Apps
    check_application_layer()
    
    # 3. Check Data
    check_data_pipeline()
    
    print("\n" + "="*60)
    print(f"{Colors.BOLD}💡 TROUBLESHOOTING TIPS:{Colors.RESET}")
    print("1. Nếu Spark chết: Kiểm tra logs/spark_streaming.log")
    print("2. Nếu Producer chết: Kiểm tra logs/producer.log")
    print("3. Để khởi động lại toàn bộ: bash stop_all.sh && bash start_all.sh")
    print("4. Dashboard: http://localhost:8501")
    print("="*60 + "\n")

if __name__ == "__main__":
    main()