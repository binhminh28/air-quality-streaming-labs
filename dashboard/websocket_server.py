"""
WebSocket Server với HTTP API để stream dữ liệu real-time từ Redis (Real-time) và Cassandra (History)
"""
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query
from fastapi.responses import JSONResponse
from cassandra.cluster import Cluster
from datetime import datetime
import uvicorn
import os
import asyncio
import redis

# Cấu hình Cassandra - hỗ trợ multiple contact points
# Format: "host1:port1,host2:port2" hoặc "host1,host2" (dùng port mặc định)
CASSANDRA_CONTACT_POINTS = os.getenv(
    "CASSANDRA_CONTACT_POINTS",
    "localhost:9042,localhost:9043"
)
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "air_quality")
CASSANDRA_TABLE = os.getenv("CASSANDRA_TABLE", "realtime_data")
HTTP_PORT = int(os.getenv("HTTP_PORT", "8765"))

# Cấu hình Redis
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

app = FastAPI()

class HybridDataStreamer:
    def __init__(self):
        self.cluster = None
        self.session = None
        self.redis_client = None
        
    def _parse_contact_points(self):
        """Parse contact points từ chuỗi 'host1:port1,host2:port2' thành list tuples"""
        contact_points = []
        for point in CASSANDRA_CONTACT_POINTS.split(','):
            point = point.strip()
            if ':' in point:
                host, port = point.split(':')
                contact_points.append((host.strip(), int(port.strip())))
            else:
                # Nếu không có port, dùng port mặc định 9042
                contact_points.append((point.strip(), 9042))
        return contact_points
        
    def connect_cassandra(self):
        if self.cluster is None:
            contact_points = self._parse_contact_points()
            print(f"Connecting to Cassandra cluster at: {contact_points}")
            self.cluster = Cluster(contact_points)
            self.session = self.cluster.connect()
            self.session.set_keyspace(CASSANDRA_KEYSPACE)
    
    def connect_redis(self):
        if self.redis_client is None:
            try:
                self.redis_client = redis.Redis(
                    host=REDIS_HOST, 
                    port=REDIS_PORT, 
                    decode_responses=True,
                    socket_connect_timeout=5
                )
                # Test connection
                self.redis_client.ping()
                print(f"Connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
            except Exception as e:
                print(f"Warning: Could not connect to Redis: {e}")
                self.redis_client = None
        
    def disconnect(self):
        if self.cluster:
            self.cluster.shutdown()
            self.cluster = None
            self.session = None
        if self.redis_client:
            self.redis_client.close()
            self.redis_client = None
    
    def fetch_realtime_data_from_redis(self):
        """
        Đọc dữ liệu real-time từ Redis (nhanh, < 10ms)
        Trả về danh sách các location với dữ liệu mới nhất
        """
        try:
            self.connect_redis()
            if self.redis_client is None:
                return []
            
            # Tìm tất cả keys có pattern location:*
            location_keys = self.redis_client.keys("location:*")
            
            data = []
            for key in location_keys:
                # Lấy hash data từ Redis
                hash_data = self.redis_client.hgetall(key)
                
                if hash_data:
                    # Extract location_id từ key (location:123 -> 123)
                    location_id = int(key.split(':')[1])
                    
                    # Parse dữ liệu từ hash
                    data.append({
                        'datetime': hash_data.get('timestamp', ''),
                        'location_id': location_id,
                        'pm25': float(hash_data.get('pm25', 0.0)),
                        'pm10': None,  # Redis chỉ lưu snapshot, không có đầy đủ fields
                        'pm1': None,
                        'temperature': None,
                        'relativehumidity': None,
                        'um003': None,
                        'aqi': int(hash_data.get('aqi', 0)),
                        'aqi_pm25': None,
                        'aqi_pm10': None,
                        'quality': hash_data.get('quality', 'N/A'),
                        'processed_at': hash_data.get('timestamp', '')
                    })
            
            # Sort by datetime descending
            if data:
                data.sort(key=lambda x: x['datetime'], reverse=True)
            
            return data
        except Exception as e:
            print(f"Error fetching real-time data from Redis: {e}")
            return []
    
    def fetch_history_data_from_cassandra(self, limit=1000):
        """
        Đọc dữ liệu lịch sử từ Cassandra (dùng cho biểu đồ, phân tích)
        """
        try:
            self.connect_cassandra()
            query = f"""
            SELECT datetime, location_id, pm25, pm10, pm1, temperature, relativehumidity, um003,
                   aqi, aqi_pm25, aqi_pm10, quality, processed_at 
            FROM {CASSANDRA_TABLE}
            LIMIT {limit}
            """
            rows = self.session.execute(query)
            
            data = []
            for row in rows:
                processed_at_str = None
                if row.processed_at:
                    if isinstance(row.processed_at, datetime):
                        processed_at_str = row.processed_at.isoformat()
                    else:
                        processed_at_str = str(row.processed_at)
                
                data.append({
                    'datetime': row.datetime,
                    'location_id': int(row.location_id) if row.location_id else None,
                    'pm25': float(row.pm25) if row.pm25 else None,
                    'pm10': float(row.pm10) if row.pm10 else None,
                    'pm1': float(row.pm1) if row.pm1 else None,
                    'temperature': float(row.temperature) if row.temperature else None,
                    'relativehumidity': float(row.relativehumidity) if row.relativehumidity else None,
                    'um003': float(row.um003) if row.um003 else None,
                    'aqi': int(row.aqi) if row.aqi else None,
                    'aqi_pm25': int(row.aqi_pm25) if row.aqi_pm25 else None,
                    'aqi_pm10': int(row.aqi_pm10) if row.aqi_pm10 else None,
                    'quality': row.quality,
                    'processed_at': processed_at_str
                })
            
            if data:
                data.sort(key=lambda x: x['datetime'], reverse=True)
            
            return data
        except Exception as e:
            print(f"Error fetching history data from Cassandra: {e}")
            return []
    
    def fetch_latest_data(self, limit=1000, use_redis=True):
        """
        Lấy dữ liệu mới nhất:
        - Nếu use_redis=True: Đọc từ Redis (real-time snapshot) - nhanh
          Nếu Redis trống, tự động fallback về Cassandra
        - Nếu use_redis=False: Đọc từ Cassandra (history) - đầy đủ hơn
        
        Returns: (data, source) tuple where source is 'redis' or 'cassandra'
        """
        if use_redis:
            redis_data = self.fetch_realtime_data_from_redis()
            # Fallback to Cassandra if Redis is empty
            if not redis_data:
                print("Redis is empty, falling back to Cassandra...")
                cassandra_data = self.fetch_history_data_from_cassandra(limit=limit)
                return cassandra_data, 'cassandra'
            return redis_data, 'redis'
        else:
            cassandra_data = self.fetch_history_data_from_cassandra(limit=limit)
            return cassandra_data, 'cassandra'

streamer = HybridDataStreamer()

@app.get("/api/data")
async def get_data(
    limit: int = Query(1000, ge=1, le=10000),
    type: str = Query("realtime", description="Type: 'realtime' (Redis) or 'history' (Cassandra)")
):
    try:
        use_redis = (type.lower() == "realtime")
        data, actual_source = streamer.fetch_latest_data(limit=limit, use_redis=use_redis)
        
        return JSONResponse({
            'type': 'data',
            'source': actual_source,
            'timestamp': datetime.now().isoformat(),
            'data': data,
            'count': len(data)
        })
    except Exception as e:
        return JSONResponse(
            {'error': str(e)},
            status_code=500
        )

@app.get("/health")
async def health():
    return {"status": "ok", "timestamp": datetime.now().isoformat()}

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    try:
        print(f"WebSocket client connected: {websocket.client}")
        
        while True:
            # Use Redis for real-time data (faster), with fallback to Cassandra
            data, source = streamer.fetch_latest_data(limit=1000, use_redis=True)
            
            if data:
                message = {
                    'type': 'data',
                    'source': source,
                    'timestamp': datetime.now().isoformat(),
                    'data': data,
                    'count': len(data)
                }
                await websocket.send_json(message)
            else:
                await websocket.send_json({
                    'type': 'empty',
                    'message': 'No data available'
                })
            
            await asyncio.sleep(5)
            
    except WebSocketDisconnect:
        print(f"WebSocket client disconnected: {websocket.client}")
    except Exception as e:
        print(f"WebSocket error: {e}")

if __name__ == "__main__":
    print(f"Starting server on http://localhost:{HTTP_PORT}")
    print(f"HTTP API: http://localhost:{HTTP_PORT}/api/data")
    print(f"WebSocket: ws://localhost:{HTTP_PORT}/ws")
    uvicorn.run(app, host="0.0.0.0", port=HTTP_PORT)
