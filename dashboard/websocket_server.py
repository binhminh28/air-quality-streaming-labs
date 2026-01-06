"""
WebSocket Server với HTTP API để stream dữ liệu real-time từ Cassandra
"""
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query
from fastapi.responses import JSONResponse
from cassandra.cluster import Cluster
from datetime import datetime
import uvicorn
import os
import asyncio

# Cấu hình Cassandra - hỗ trợ multiple contact points
# Format: "host1:port1,host2:port2" hoặc "host1,host2" (dùng port mặc định)
CASSANDRA_CONTACT_POINTS = os.getenv(
    "CASSANDRA_CONTACT_POINTS",
    "localhost:9042,localhost:9043"
)
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "air_quality")
CASSANDRA_TABLE = os.getenv("CASSANDRA_TABLE", "realtime_data")
HTTP_PORT = int(os.getenv("HTTP_PORT", "8765"))

app = FastAPI()

class CassandraStreamer:
    def __init__(self):
        self.cluster = None
        self.session = None
        
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
        
    def connect(self):
        if self.cluster is None:
            contact_points = self._parse_contact_points()
            print(f"Connecting to Cassandra cluster at: {contact_points}")
            # Cluster() nhận list tuples (host, port) hoặc list host strings
            # Vì mỗi node có thể có port khác nhau, ta truyền list tuples trực tiếp
            self.cluster = Cluster(contact_points)
            self.session = self.cluster.connect()
            self.session.set_keyspace(CASSANDRA_KEYSPACE)
        
    def disconnect(self):
        if self.cluster:
            self.cluster.shutdown()
            self.cluster = None
            self.session = None
            
    def fetch_latest_data(self, limit=1000):
        try:
            self.connect()
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
            print(f"Error fetching data: {e}")
            return []

streamer = CassandraStreamer()

@app.get("/api/data")
async def get_data(limit: int = Query(1000, ge=1, le=10000)):
    try:
        data = streamer.fetch_latest_data(limit=limit)
        return JSONResponse({
            'type': 'data',
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
            data = streamer.fetch_latest_data(limit=1000)
            
            if data:
                message = {
                    'type': 'data',
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
