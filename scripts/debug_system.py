#!/usr/bin/env python3
"""
Script debug toàn bộ hệ thống Air Quality Streaming
Kiểm tra tất cả các component: Docker, Kafka, Spark, Cassandra, Dashboard
"""

import subprocess
import sys
import os
import time
from datetime import datetime
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import json

# Colors for output
class Colors:
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    RESET = '\033[0m'
    BOLD = '\033[1m'

def print_header(text):
    print(f"\n{Colors.BOLD}{Colors.BLUE}{'='*60}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.BLUE}{text}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.BLUE}{'='*60}{Colors.RESET}\n")

def print_success(text):
    print(f"{Colors.GREEN}✅ {text}{Colors.RESET}")

def print_error(text):
    print(f"{Colors.RED}❌ {text}{Colors.RESET}")

def print_warning(text):
    print(f"{Colors.YELLOW}⚠️  {text}{Colors.RESET}")

def print_info(text):
    print(f"{Colors.BLUE}ℹ️  {text}{Colors.RESET}")

def check_docker_services():
    """Kiểm tra các Docker services"""
    print_header("1. KIỂM TRA DOCKER SERVICES")
    
    try:
        result = subprocess.run(
            ['docker', 'ps', '--format', '{{.Names}}\t{{.Status}}'],
            capture_output=True,
            text=True,
            check=True
        )
        
        services = {
            'zookeeper': False,
            'kafka': False,
            'cassandra': False
        }
        
        for line in result.stdout.strip().split('\n'):
            if line:
                name, status = line.split('\t')
                if 'zookeeper' in name.lower():
                    services['zookeeper'] = True
                    print_success(f"Zookeeper: {name} - {status}")
                elif 'kafka' in name.lower():
                    services['kafka'] = True
                    print_success(f"Kafka: {name} - {status}")
                elif 'cassandra' in name.lower():
                    services['cassandra'] = True
                    print_success(f"Cassandra: {name} - {status}")
        
        for service, running in services.items():
            if not running:
                print_error(f"{service.capitalize()} không đang chạy!")
        
        return all(services.values())
    except subprocess.CalledProcessError as e:
        print_error(f"Lỗi khi kiểm tra Docker: {e}")
        return False
    except FileNotFoundError:
        print_error("Docker không được cài đặt hoặc không có trong PATH")
        return False

def check_kafka_topic():
    """Kiểm tra Kafka topic và messages"""
    print_header("2. KIỂM TRA KAFKA TOPIC")
    
    try:
        # Kiểm tra topic tồn tại
        result = subprocess.run(
            ['docker', 'exec', 'kafka', 'kafka-topics', '--list', '--bootstrap-server', 'localhost:29092'],
            capture_output=True,
            text=True,
            check=True
        )
        
        topics = result.stdout.strip().split('\n')
        if 'air_quality_realtime' in topics:
            print_success(f"Topic 'air_quality_realtime' tồn tại")
        else:
            print_error("Topic 'air_quality_realtime' không tồn tại!")
            return False
        
        # Kiểm tra số lượng messages trong topic
        result = subprocess.run(
            ['docker', 'exec', 'kafka', 'kafka-run-class', 'kafka.tools.GetOffsetShell',
             '--broker-list', 'localhost:29092',
             '--topic', 'air_quality_realtime'],
            capture_output=True,
            text=True,
            check=True
        )
        
        if result.stdout:
            offsets = result.stdout.strip().split('\n')
            total_messages = 0
            for offset_line in offsets:
                if ':' in offset_line:
                    parts = offset_line.split(':')
                    if len(parts) >= 3:
                        total_messages += int(parts[2])
            print_info(f"Tổng số messages trong topic: {total_messages}")
            
            # Lấy message mới nhất
            result = subprocess.run(
                ['docker', 'exec', 'kafka', 'kafka-console-consumer',
                 '--bootstrap-server', 'localhost:29092',
                 '--topic', 'air_quality_realtime',
                 '--from-beginning',
                 '--max-messages', '1',
                 '--timeout-ms', '5000'],
                capture_output=True,
                text=True,
                timeout=10
            )
            
            if result.stdout:
                print_success(f"Message mới nhất: {result.stdout.strip()[:100]}")
            else:
                print_warning("Không có message nào trong topic")
        else:
            print_warning("Không thể lấy thông tin về messages")
        
        return True
    except subprocess.CalledProcessError as e:
        print_error(f"Lỗi khi kiểm tra Kafka: {e}")
        print_error(f"Output: {e.stderr}")
        return False
    except subprocess.TimeoutExpired:
        print_warning("Timeout khi lấy message mới nhất")
        return True
    except Exception as e:
        print_error(f"Lỗi không mong đợi: {e}")
        return False

def check_kafka_producer():
    """Kiểm tra Kafka Producer có đang chạy không"""
    print_header("3. KIỂM TRA KAFKA PRODUCER")
    
    try:
        result = subprocess.run(
            ['ps', 'aux'],
            capture_output=True,
            text=True,
            check=True
        )
        
        producer_running = False
        for line in result.stdout.split('\n'):
            if 'producer.py' in line and 'python' in line.lower():
                producer_running = True
                parts = line.split()
                pid = parts[1]
                print_success(f"Producer đang chạy (PID: {pid})")
                print_info(f"Command: {' '.join(parts[10:])}")
                break
        
        if not producer_running:
            print_error("Producer KHÔNG đang chạy!")
            print_info("Chạy: python kafka/producer.py")
        
        return producer_running
    except Exception as e:
        print_error(f"Lỗi khi kiểm tra Producer: {e}")
        return False

def check_spark_streaming():
    """Kiểm tra Spark Streaming Job"""
    print_header("4. KIỂM TRA SPARK STREAMING JOB")
    
    try:
        result = subprocess.run(
            ['ps', 'aux'],
            capture_output=True,
            text=True,
            check=True
        )
        
        spark_running = False
        for line in result.stdout.split('\n'):
            if 'streaming_job.py' in line and ('spark-submit' in line or 'python' in line.lower()):
                spark_running = True
                parts = line.split()
                pid = parts[1]
                print_success(f"Spark Streaming Job đang chạy (PID: {pid})")
                print_info(f"Command: {' '.join(parts[10:])}")
                break
        
        if not spark_running:
            print_error("Spark Streaming Job KHÔNG đang chạy!")
            print_info("Chạy: bash run_spark_cassandra.sh")
        
        return spark_running
    except Exception as e:
        print_error(f"Lỗi khi kiểm tra Spark: {e}")
        return False

def check_cassandra_connection():
    """Kiểm tra kết nối Cassandra"""
    print_header("5. KIỂM TRA CASSANDRA CONNECTION")
    
    try:
        cluster = Cluster(['localhost'], port=9042)
        session = cluster.connect()
        print_success("Kết nối Cassandra thành công")
        
        # Kiểm tra keyspace
        result = session.execute("SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name = 'air_quality'")
        if result.one():
            print_success("Keyspace 'air_quality' tồn tại")
        else:
            print_error("Keyspace 'air_quality' KHÔNG tồn tại!")
            cluster.shutdown()
            return False
        
        # Kiểm tra table
        session.set_keyspace('air_quality')
        result = session.execute("SELECT table_name FROM system_schema.tables WHERE keyspace_name = 'air_quality' AND table_name = 'realtime_data'")
        if result.one():
            print_success("Table 'realtime_data' tồn tại")
        else:
            print_error("Table 'realtime_data' KHÔNG tồn tại!")
            cluster.shutdown()
            return False
        
        cluster.shutdown()
        return True
    except Exception as e:
        print_error(f"Lỗi kết nối Cassandra: {e}")
        return False

def check_cassandra_data():
    """Kiểm tra dữ liệu trong Cassandra"""
    print_header("6. KIỂM TRA DỮ LIỆU TRONG CASSANDRA")
    
    try:
        cluster = Cluster(['localhost'], port=9042)
        session = cluster.connect('air_quality')
        
        # Đếm tổng số records
        result = session.execute("SELECT COUNT(*) FROM realtime_data")
        total_count = result.one()[0]
        print_info(f"Tổng số records: {total_count}")
        
        if total_count == 0:
            print_warning("KHÔNG có dữ liệu trong Cassandra!")
            cluster.shutdown()
            return False
        
        # Lấy 5 records mới nhất
        result = session.execute("""
            SELECT datetime, pm25, aqi, quality, processed_at 
            FROM realtime_data 
            LIMIT 5
        """)
        
        print_info("\n5 records mới nhất:")
        records = list(result)
        for i, row in enumerate(records, 1):
            print(f"  {i}. datetime={row.datetime}, pm25={row.pm25:.2f}, aqi={row.aqi}, quality={row.quality}, processed_at={row.processed_at}")
        
        # Lấy record mới nhất và cũ nhất (Cassandra không hỗ trợ ORDER BY trên non-primary key)
        # Sẽ sort trong Python
        result = session.execute("""
            SELECT datetime, pm25, aqi, quality, processed_at 
            FROM realtime_data 
            LIMIT 1000
        """)
        
        all_records = list(result)
        if all_records:
            # Sort theo datetime trong Python
            all_records.sort(key=lambda x: x.datetime, reverse=True)
            latest = all_records[0]
            print_info(f"\nRecord mới nhất:")
            print(f"  datetime: {latest.datetime}")
            print(f"  pm25: {latest.pm25:.2f}")
            print(f"  aqi: {latest.aqi}")
            print(f"  quality: {latest.quality}")
            print(f"  processed_at: {latest.processed_at}")
            
            all_records.sort(key=lambda x: x.datetime, reverse=False)
            oldest = all_records[0]
            print_info(f"\nRecord cũ nhất:")
            print(f"  datetime: {oldest.datetime}")
            print(f"  pm25: {oldest.pm25:.2f}")
            print(f"  aqi: {oldest.aqi}")
            print(f"  quality: {oldest.quality}")
            print(f"  processed_at: {oldest.processed_at}")
        
        # Kiểm tra dữ liệu có được cập nhật gần đây không (trong 1 phút qua)
        current_time = datetime.now()
        if latest and latest.processed_at:
            time_diff = (current_time - latest.processed_at.replace(tzinfo=None)).total_seconds()
            if time_diff < 60:
                print_success(f"Dữ liệu được cập nhật {time_diff:.0f} giây trước (realtime)")
            else:
                print_warning(f"Dữ liệu được cập nhật {time_diff:.0f} giây trước (KHÔNG realtime - quá cũ)")
        
        cluster.shutdown()
        return True
    except Exception as e:
        print_error(f"Lỗi khi kiểm tra dữ liệu Cassandra: {e}")
        import traceback
        print_error(f"Traceback: {traceback.format_exc()}")
        return False

def check_dashboard_connection():
    """Kiểm tra kết nối từ Dashboard đến Cassandra"""
    print_header("7. KIỂM TRA DASHBOARD CONNECTION")
    
    try:
        # Simulate dashboard connection
        cluster = Cluster(['localhost'], port=9042)
        session = cluster.connect()
        session.set_keyspace('air_quality')
        
        # Query giống như dashboard
        query = """
        SELECT datetime, pm25, aqi, quality, processed_at 
        FROM realtime_data
        LIMIT 1000
        """
        rows = session.execute(query)
        
        data = []
        for row in rows:
            data.append({
                'datetime': row.datetime,
                'pm25': row.pm25,
                'aqi': row.aqi,
                'quality': row.quality,
                'processed_at': row.processed_at
            })
        
        print_success(f"Dashboard có thể đọc được {len(data)} records")
        
        if len(data) > 0:
            # Convert datetime
            import pandas as pd
            df = pd.DataFrame(data)
            df['datetime'] = pd.to_datetime(df['datetime'])
            df = df.sort_values('datetime', ascending=False)
            
            latest = df.iloc[0]
            print_info(f"\nRecord mới nhất mà Dashboard sẽ hiển thị:")
            print(f"  datetime: {latest['datetime']}")
            print(f"  pm25: {latest['pm25']:.2f}")
            print(f"  aqi: {latest['aqi']}")
            print(f"  quality: {latest['quality']}")
        
        cluster.shutdown()
        return True
    except Exception as e:
        print_error(f"Lỗi khi kiểm tra Dashboard connection: {e}")
        import traceback
        print_error(f"Traceback: {traceback.format_exc()}")
        return False

def check_streamlit():
    """Kiểm tra Streamlit có đang chạy không"""
    print_header("8. KIỂM TRA STREAMLIT DASHBOARD")
    
    try:
        result = subprocess.run(
            ['ps', 'aux'],
            capture_output=True,
            text=True,
            check=True
        )
        
        streamlit_running = False
        for line in result.stdout.split('\n'):
            if 'streamlit' in line.lower() and 'streamlit_app.py' in line:
                streamlit_running = True
                parts = line.split()
                pid = parts[1]
                print_success(f"Streamlit Dashboard đang chạy (PID: {pid})")
                print_info(f"Command: {' '.join(parts[10:])}")
                break
        
        if not streamlit_running:
            print_warning("Streamlit Dashboard KHÔNG đang chạy!")
            print_info("Chạy: streamlit run dashboard/streamlit_app.py")
        
        return streamlit_running
    except Exception as e:
        print_error(f"Lỗi khi kiểm tra Streamlit: {e}")
        return False

def monitor_data_updates():
    """Monitor dữ liệu có được cập nhật không trong 30 giây"""
    print_header("9. MONITOR DỮ LIỆU CẬP NHẬT (30 giây)")
    
    try:
        cluster = Cluster(['localhost'], port=9042)
        session = cluster.connect('air_quality')
        
        # Lấy số lượng records ban đầu
        result = session.execute("SELECT COUNT(*) FROM realtime_data")
        initial_count = result.one()[0]
        print_info(f"Số lượng records ban đầu: {initial_count}")
        
        # Lấy record mới nhất ban đầu
        result = session.execute("""
            SELECT datetime, processed_at 
            FROM realtime_data 
            ORDER BY datetime DESC 
            LIMIT 1
        """)
        initial_latest = result.one()
        if initial_latest:
            print_info(f"Record mới nhất ban đầu: {initial_latest.datetime}, processed_at: {initial_latest.processed_at}")
        
        print_info("Đang monitor trong 30 giây...")
        time.sleep(30)
        
        # Kiểm tra lại
        result = session.execute("SELECT COUNT(*) FROM realtime_data")
        final_count = result.one()[0]
        print_info(f"Số lượng records sau 30 giây: {final_count}")
        
        result = session.execute("""
            SELECT datetime, processed_at 
            FROM realtime_data 
            ORDER BY datetime DESC 
            LIMIT 1
        """)
        final_latest = result.one()
        if final_latest:
            print_info(f"Record mới nhất sau 30 giây: {final_latest.datetime}, processed_at: {final_latest.processed_at}")
        
        # So sánh
        if final_count > initial_count:
            print_success(f"Có {final_count - initial_count} records mới được thêm vào!")
        elif final_latest and initial_latest and final_latest.datetime != initial_latest.datetime:
            print_success("Có dữ liệu mới được cập nhật!")
        else:
            print_warning("KHÔNG có dữ liệu mới được thêm vào trong 30 giây!")
            print_warning("Có thể Producer hoặc Spark Streaming không hoạt động đúng!")
        
        cluster.shutdown()
        return True
    except Exception as e:
        print_error(f"Lỗi khi monitor: {e}")
        import traceback
        print_error(f"Traceback: {traceback.format_exc()}")
        return False

def generate_report():
    """Tạo báo cáo tổng hợp"""
    print_header("📊 BÁO CÁO TỔNG HỢP")
    
    results = {
        'docker': check_docker_services(),
        'kafka_topic': check_kafka_topic(),
        'kafka_producer': check_kafka_producer(),
        'spark_streaming': check_spark_streaming(),
        'cassandra_connection': check_cassandra_connection(),
        'cassandra_data': check_cassandra_data(),
        'dashboard_connection': check_dashboard_connection(),
        'streamlit': check_streamlit(),
    }
    
    print("\n" + "="*60)
    print("KẾT QUẢ KIỂM TRA:")
    print("="*60)
    
    for component, status in results.items():
        if status:
            print_success(f"{component}: OK")
        else:
            print_error(f"{component}: FAILED")
    
    all_ok = all(results.values())
    
    if all_ok:
        print_success("\n✅ TẤT CẢ COMPONENTS ĐANG HOẠT ĐỘNG!")
    else:
        print_error("\n❌ MỘT SỐ COMPONENTS CÓ VẤN ĐỀ!")
        print_info("\nCác bước khắc phục:")
        if not results['docker']:
            print_info("  1. Khởi động Docker services: cd docker && docker-compose up -d")
        if not results['kafka_topic']:
            print_info("  2. Tạo Kafka topic: bash scripts/create_topics.sh")
        if not results['kafka_producer']:
            print_info("  3. Chạy Producer: python kafka/producer.py")
        if not results['spark_streaming']:
            print_info("  4. Chạy Spark Streaming: bash run_spark_cassandra.sh")
        if not results['cassandra_connection'] or not results['cassandra_data']:
            print_info("  5. Khởi tạo Cassandra schema: docker exec -it cassandra cqlsh -f /scripts/init_cassandra.cql")
        if not results['streamlit']:
            print_info("  6. Chạy Dashboard: streamlit run dashboard/streamlit_app.py")
    
    return results

def main():
    """Main function"""
    print(f"\n{Colors.BOLD}{Colors.BLUE}")
    print("="*60)
    print("  AIR QUALITY STREAMING SYSTEM - DEBUG TOOL")
    print("="*60)
    print(f"{Colors.RESET}\n")
    
    print_info(f"Thời gian chạy: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Chạy các kiểm tra
    results = generate_report()
    
    # Hỏi có muốn monitor không
    print("\n" + "="*60)
    response = input("Bạn có muốn monitor dữ liệu cập nhật trong 30 giây? (y/n): ")
    if response.lower() == 'y':
        monitor_data_updates()
    
    print(f"\n{Colors.BOLD}{Colors.BLUE}")
    print("="*60)
    print("  DEBUG HOÀN TẤT")
    print("="*60)
    print(f"{Colors.RESET}\n")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nĐã dừng debug tool.")
        sys.exit(0)
    except Exception as e:
        print_error(f"Lỗi không mong đợi: {e}")
        import traceback
        print_error(traceback.format_exc())
        sys.exit(1)

