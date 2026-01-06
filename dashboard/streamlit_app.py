"""
Streamlit Dashboard cho Air Quality Monitoring
Nhận dữ liệu real-time từ WebSocket
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import time
import requests

WEBSOCKET_SERVER_URL = "http://localhost:8765/api/data"

@st.cache_data(ttl=1)
def fetch_data_from_websocket(limit=1000):
    try:
        response = requests.get(f"{WEBSOCKET_SERVER_URL}?limit={limit}", timeout=10)
        if response.status_code == 200:
            result = response.json()
            data = result.get('data', [])
            if data and len(data) > 0:
                df = pd.DataFrame(data)
                # Parse datetime - handle multiple formats
                if 'datetime' in df.columns:
                    df['datetime'] = pd.to_datetime(df['datetime'], errors='coerce', utc=True)
                    # Remove rows with invalid datetime
                    df = df.dropna(subset=['datetime'])
                if not df.empty:
                    df = df.sort_values('datetime', ascending=False).reset_index(drop=True)
                    return df
        return pd.DataFrame()
    except requests.exceptions.ConnectionError:
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Error fetching data: {str(e)}")
        return pd.DataFrame()

def get_aqi_color(aqi):
    if aqi <= 50:
        return "#00E400"
    elif aqi <= 100:
        return "#FFFF00"
    elif aqi <= 150:
        return "#FF7E00"
    elif aqi <= 200:
        return "#FF0000"
    elif aqi <= 300:
        return "#8F3F97"
    else:
        return "#7E0023"

def main():
    st.set_page_config(
        page_title="Air Quality Monitoring Dashboard",
        page_icon="🌬️",
        layout="wide"
    )
    
    st.title("🌬️ Air Quality Monitoring Dashboard")
    st.markdown("**Real-time Air Quality Data - QCVN 05:2013/BTNMT (Việt Nam)**")
    
    with st.sidebar:
        st.header("⚙️ Cấu hình")
        data_limit = st.slider("Số lượng records", 100, 5000, 1000)
        auto_refresh = st.checkbox("🔄 Auto-refresh (5 giây)", value=True)
        
        col1, col2 = st.columns(2)
        with col1:
            if st.button("🔄 Refresh", use_container_width=True):
                fetch_data_from_websocket.clear()
                st.rerun()
        with col2:
            if st.button("🗑️ Clear Cache", use_container_width=True):
                fetch_data_from_websocket.clear()
                st.success("Cache cleared!")
                st.rerun()
    
    display_dashboard(data_limit)
    
    if auto_refresh:
        time.sleep(5)
        st.rerun()

def display_dashboard(data_limit):
    # Clear cache if button is pressed (handled in sidebar)
    df = fetch_data_from_websocket(limit=data_limit)
    
    if df.empty:
        st.warning("⚠️ Chưa có dữ liệu hoặc không kết nối được WebSocket server.")
        
        # Test connection
        col1, col2 = st.columns(2)
        with col1:
            st.info("💡 Kiểm tra:")
            st.info("   1. WebSocket server: `curl http://localhost:8765/health`")
            st.info("   2. Chạy: `bash start_all.sh`")
            st.info("   3. Đợi vài giây để Spark xử lý dữ liệu")
        
        with col2:
            try:
                response = requests.get("http://localhost:8765/health", timeout=2)
                if response.status_code == 200:
                    st.success("✅ WebSocket server đang chạy")
                    
                    # Check data directly
                    data_response = requests.get("http://localhost:8765/api/data?limit=10", timeout=5)
                    if data_response.status_code == 200:
                        data_json = data_response.json()
                        count = data_json.get('count', 0)
                        if count > 0:
                            st.success(f"✅ Có {count} records trong database")
                            st.info("💡 Nếu vẫn không hiển thị, thử refresh lại trang (F5)")
                            # Try to show raw data for debugging
                            with st.expander("🔍 Debug: Xem dữ liệu thô"):
                                st.json(data_json)
                        else:
                            st.warning("⚠️ Server chạy nhưng chưa có dữ liệu")
                            st.info("💡 Đợi thêm vài giây để Producer và Spark Streaming xử lý")
                    else:
                        st.warning(f"⚠️ Không thể lấy dữ liệu từ API (Status: {data_response.status_code})")
                else:
                    st.error("❌ WebSocket server không phản hồi đúng")
            except requests.exceptions.ConnectionError:
                st.error("❌ Không thể kết nối đến WebSocket server")
                st.info("💡 Chạy: `bash start_all.sh` để khởi động services")
            except Exception as e:
                st.error(f"❌ Lỗi: {str(e)}")
        return
    
    latest_time = df['datetime'].max() if not df.empty else None
    current_time = datetime.now()
    time_diff = None
    
    if latest_time:
        if hasattr(latest_time, 'tzinfo') and latest_time.tzinfo:
            latest_time_naive = latest_time.replace(tzinfo=None)
        else:
            latest_time_naive = latest_time
        time_diff = (current_time - latest_time_naive).total_seconds()
    
    if time_diff and time_diff < 60:
        time_status = f"🟢 Realtime ({int(time_diff)}s)"
    elif time_diff and time_diff < 300:
        time_status = f"🟡 Gần realtime ({int(time_diff)}s)"
    elif time_diff:
        time_status = f"🔴 Không realtime ({int(time_diff)}s)"
    else:
        time_status = "🔴 Không có dữ liệu"
    
    st.caption(f"📊 {len(df)} records | {time_status} | {latest_time.strftime('%H:%M:%S') if latest_time else 'N/A'}")
    
    latest = df.iloc[0]
    
    # Metrics row 1: AQI và chất lượng
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("📊 AQI Tổng hợp", latest.get('aqi', 'N/A'))
        if 'aqi_pm25' in latest and latest['aqi_pm25']:
            st.caption(f"PM2.5: {latest['aqi_pm25']}")
        if 'aqi_pm10' in latest and latest['aqi_pm10']:
            st.caption(f"PM10: {latest['aqi_pm10']}")
    with col2:
        st.metric("🌡️ Chất lượng", latest.get('quality', 'N/A').split('(')[0].strip())
    with col3:
        st.metric("🌡️ Nhiệt độ", f"{latest.get('temperature', 0):.1f}°C" if latest.get('temperature') else "N/A")
    with col4:
        st.metric("💧 Độ ẩm", f"{latest.get('relativehumidity', 0):.1f}%" if latest.get('relativehumidity') else "N/A")
    
    # Metrics row 2: PM values
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("💨 PM2.5", f"{latest.get('pm25', 0):.2f} µg/m³" if latest.get('pm25') else "N/A")
    with col2:
        st.metric("💨 PM10", f"{latest.get('pm10', 0):.2f} µg/m³" if latest.get('pm10') else "N/A")
    with col3:
        st.metric("💨 PM1", f"{latest.get('pm1', 0):.2f} µg/m³" if latest.get('pm1') else "N/A")
    with col4:
        st.metric("📍 Location ID", latest.get('location_id', 'N/A'))
    
    st.divider()
    
    # Charts
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📈 AQI theo thời gian")
        fig_aqi = go.Figure()
        if 'aqi' in df.columns:
            colors = [get_aqi_color(aqi) if pd.notna(aqi) else "#CCCCCC" for aqi in df['aqi']]
            fig_aqi.add_trace(go.Scatter(
                x=df['datetime'],
                y=df['aqi'],
                mode='lines+markers',
                name='AQI Tổng hợp',
                line=dict(color='#1f77b4', width=2),
                marker=dict(size=4, color=colors)
            ))
        if 'aqi_pm25' in df.columns:
            fig_aqi.add_trace(go.Scatter(
                x=df['datetime'],
                y=df['aqi_pm25'],
                mode='lines',
                name='AQI PM2.5',
                line=dict(color='#ff7f0e', width=1, dash='dash')
            ))
        if 'aqi_pm10' in df.columns:
            fig_aqi.add_trace(go.Scatter(
                x=df['datetime'],
                y=df['aqi_pm10'],
                mode='lines',
                name='AQI PM10',
                line=dict(color='#2ca02c', width=1, dash='dot')
            ))
        fig_aqi.add_hline(y=50, line_dash="dash", line_color="green", annotation_text="Tốt (50)")
        fig_aqi.add_hline(y=100, line_dash="dash", line_color="yellow", annotation_text="Trung bình (100)")
        fig_aqi.add_hline(y=150, line_dash="dash", line_color="orange", annotation_text="Kém (150)")
        fig_aqi.add_hline(y=200, line_dash="dash", line_color="red", annotation_text="Xấu (200)")
        fig_aqi.update_layout(xaxis_title="Thời gian", yaxis_title="AQI", height=400, hovermode='x unified')
        st.plotly_chart(fig_aqi, width='stretch', key='aqi_chart')
    
    with col2:
        st.subheader("💨 PM theo thời gian")
        fig_pm = go.Figure()
        if 'pm25' in df.columns:
            fig_pm.add_trace(go.Scatter(x=df['datetime'], y=df['pm25'], mode='lines+markers', name='PM2.5', line=dict(color='#ff7f0e')))
        if 'pm10' in df.columns:
            fig_pm.add_trace(go.Scatter(x=df['datetime'], y=df['pm10'], mode='lines+markers', name='PM10', line=dict(color='#2ca02c')))
        if 'pm1' in df.columns:
            fig_pm.add_trace(go.Scatter(x=df['datetime'], y=df['pm1'], mode='lines+markers', name='PM1', line=dict(color='#d62728')))
        fig_pm.update_layout(xaxis_title="Thời gian", yaxis_title="PM (µg/m³)", height=400, hovermode='x unified')
        st.plotly_chart(fig_pm, width='stretch', key='pm_chart')
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🌡️ Nhiệt độ & Độ ẩm")
        fig_env = go.Figure()
        if 'temperature' in df.columns:
            fig_env.add_trace(go.Scatter(
                x=df['datetime'], 
                y=df['temperature'], 
                mode='lines+markers', 
                name='Nhiệt độ (°C)',
                yaxis='y',
                line=dict(color='#ff7f0e')
            ))
        if 'relativehumidity' in df.columns:
            fig_env.add_trace(go.Scatter(
                x=df['datetime'], 
                y=df['relativehumidity'], 
                mode='lines+markers', 
                name='Độ ẩm (%)',
                yaxis='y2',
                line=dict(color='#1f77b4')
            ))
        fig_env.update_layout(
            xaxis_title="Thời gian",
            yaxis=dict(title="Nhiệt độ (°C)", side='left'),
            yaxis2=dict(title="Độ ẩm (%)", side='right', overlaying='y'),
            height=300,
            hovermode='x unified'
        )
        st.plotly_chart(fig_env, width='stretch', key='env_chart')
    
    with col2:
        st.subheader("🎨 Phân bố Chất lượng")
        if 'quality' in df.columns:
            quality_counts = df['quality'].value_counts()
            fig_pie = px.pie(values=quality_counts.values, names=quality_counts.index, title='Tỷ lệ Chất lượng không khí')
            fig_pie.update_layout(height=300)
            st.plotly_chart(fig_pie, width='stretch', key='quality_pie_chart')
    
    st.divider()
    
    st.subheader("📋 Dữ liệu mới nhất")
    df_display = df.copy()
    df_display['datetime'] = df_display['datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')
    if 'processed_at' in df_display.columns:
        df_display['processed_at'] = pd.to_datetime(df_display['processed_at'], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
    
    # Chọn các cột quan trọng để hiển thị
    display_cols = ['datetime', 'aqi', 'aqi_pm25', 'aqi_pm10', 'pm25', 'pm10', 'pm1', 
                   'temperature', 'relativehumidity', 'quality']
    available_cols = [col for col in display_cols if col in df_display.columns]
    st.dataframe(df_display[available_cols].head(50), width='stretch', hide_index=True)
    
    with st.expander("📊 Thống kê chi tiết"):
        col1, col2, col3 = st.columns(3)
        with col1:
            if 'aqi' in df.columns:
                st.write("**AQI Statistics:**")
                st.write(df['aqi'].describe())
        with col2:
            if 'pm25' in df.columns:
                st.write("**PM2.5 Statistics:**")
                st.write(df['pm25'].describe())
        with col3:
            if 'pm10' in df.columns:
                st.write("**PM10 Statistics:**")
                st.write(df['pm10'].describe())
    
    if latest.get('aqi') and latest['aqi'] > 150:
        st.error(f"⚠️ **CẢNH BÁO:** AQI hiện tại là {latest['aqi']} - {latest.get('quality', 'N/A')}")
    elif latest.get('aqi') and latest['aqi'] > 100:
        st.warning(f"⚠️ **LƯU Ý:** AQI hiện tại là {latest['aqi']} - {latest.get('quality', 'N/A')}")
    elif latest.get('aqi'):
        st.success(f"✅ AQI hiện tại là {latest['aqi']} - {latest.get('quality', 'N/A')}")

if __name__ == "__main__":
    main()
