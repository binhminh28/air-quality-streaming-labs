import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import time
import requests

# Cấu hình Server
WEBSOCKET_SERVER_URL = "http://localhost:8765/api/data"

# --- CONFIG & STYLING ---
def setup_page():
    st.set_page_config(
        page_title="Air Quality Monitor",
        page_icon="🌬️",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    st.markdown("""
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap');
        
        html, body, [class*="css"] {
            font-family: 'Inter', sans-serif;
        }
        
        /* Custom Header */
        .main-header {
            background: linear-gradient(90deg, #4b6cb7 0%, #182848 100%);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            font-weight: 800;
            font-size: 2.5rem;
            text-align: center;
            margin-bottom: 0;
            padding-bottom: 0;
        }
        
        .sub-header {
            text-align: center;
            color: #6c757d;
            font-size: 1rem;
            margin-bottom: 2rem;
        }

        /* Metric Cards Style */
        div[data-testid="metric-container"] {
            background-color: #ffffff;
            border: 1px solid #e0e0e0;
            padding: 1rem;
            border-radius: 10px;
            box-shadow: 0 2px 5px rgba(0,0,0,0.05);
            transition: transform 0.2s;
        }
        
        div[data-testid="metric-container"]:hover {
            transform: translateY(-2px);
            box-shadow: 0 5px 15px rgba(0,0,0,0.1);
        }

        /* Section Dividers */
        .section-title {
            font-size: 1.25rem;
            font-weight: 700;
            color: #2c3e50;
            margin-top: 1rem;
            margin-bottom: 1rem;
            border-left: 5px solid #4b6cb7;
            padding-left: 10px;
        }
        </style>
    """, unsafe_allow_html=True)

# --- DATA FETCHING ---
@st.cache_data(ttl=1)
def fetch_realtime_data():
    """
    Lấy dữ liệu real-time từ Spark (qua Redis)
    Dùng cho phần "🎯 Chỉ số hiện tại"
    """
    try:
        response = requests.get(f"{WEBSOCKET_SERVER_URL}?limit=100&type=realtime", timeout=5)
        if response.status_code == 200:
            result = response.json()
            data = result.get('data', [])
            if data:
                df = pd.DataFrame(data)
                if 'datetime' in df.columns:
                    df['datetime'] = pd.to_datetime(df['datetime'], errors='coerce', utc=True)
                    df = df.dropna(subset=['datetime'])
                if not df.empty:
                    df = df.sort_values('datetime', ascending=False).reset_index(drop=True)
                    return df
        return pd.DataFrame()
    except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Lỗi khi lấy dữ liệu real-time: {str(e)}")
        return pd.DataFrame()

@st.cache_data(ttl=5)
def fetch_history_data(limit=1000):
    """
    Lấy dữ liệu lịch sử từ Cassandra
    Dùng cho phần "📈 Xu hướng theo thời gian" và "📋 Xem dữ liệu chi tiết"
    """
    try:
        response = requests.get(f"{WEBSOCKET_SERVER_URL}?limit={limit}&type=history", timeout=10)
        if response.status_code == 200:
            result = response.json()
            data = result.get('data', [])
            if data:
                df = pd.DataFrame(data)
                if 'datetime' in df.columns:
                    df['datetime'] = pd.to_datetime(df['datetime'], errors='coerce', utc=True)
                    df = df.dropna(subset=['datetime'])
                if not df.empty:
                    df = df.sort_values('datetime', ascending=False).reset_index(drop=True)
                    return df
        return pd.DataFrame()
    except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Lỗi khi lấy dữ liệu lịch sử: {str(e)}")
        return pd.DataFrame()

def safe_get_value(series, key, default=0.0):
    """
    An toàn lấy giá trị từ pandas Series với xử lý None/NaN
    """
    try:
        if key not in series.index:
            return default
        value = series[key]
        if pd.isna(value) or value is None:
            return default
        return float(value)
    except (ValueError, TypeError, KeyError):
        return default

# --- UTILS ---
def get_aqi_color(aqi):
    if pd.isna(aqi): return "#CCCCCC"
    val = float(aqi)
    if val <= 50: return "#00E400"  # Good
    if val <= 100: return "#FFFF00" # Moderate
    if val <= 150: return "#FF7E00" # Unhealthy for Sensitive
    if val <= 200: return "#FF0000" # Unhealthy
    if val <= 300: return "#8F3F97" # Very Unhealthy
    return "#7E0023"                # Hazardous

def get_aqi_level(aqi):
    if pd.isna(aqi): return "N/A"
    val = float(aqi)
    if val <= 50: return "Tốt"
    if val <= 100: return "Trung bình"
    if val <= 150: return "Kém"
    if val <= 200: return "Xấu"
    if val <= 300: return "Rất xấu"
    return "Nguy hại"

# --- CHARTS ---
def create_gauge_chart(value, title="AQI Index"):
    """Tạo biểu đồ đồng hồ đo AQI"""
    if pd.isna(value): value = 0
    
    fig = go.Figure(go.Indicator(
        mode = "gauge+number",
        value = value,
        domain = {'x': [0, 1], 'y': [0, 1]},
        title = {'text': title, 'font': {'size': 24, 'color': "#2c3e50"}},
        number = {'font': {'size': 40, 'weight': 700}},
        gauge = {
            'axis': {'range': [0, 500], 'tickwidth': 1, 'tickcolor': "darkblue"},
            'bar': {'color': get_aqi_color(value)},
            'bgcolor': "white",
            'borderwidth': 2,
            'bordercolor': "gray",
            'steps': [
                {'range': [0, 50], 'color': 'rgba(0, 228, 0, 0.2)'},
                {'range': [50, 100], 'color': 'rgba(255, 255, 0, 0.2)'},
                {'range': [100, 150], 'color': 'rgba(255, 126, 0, 0.2)'},
                {'range': [150, 200], 'color': 'rgba(255, 0, 0, 0.2)'},
                {'range': [200, 300], 'color': 'rgba(143, 63, 151, 0.2)'},
                {'range': [300, 500], 'color': 'rgba(126, 0, 35, 0.2)'}
            ],
            'threshold': {
                'line': {'color': "red", 'width': 4},
                'thickness': 0.75,
                'value': value
            }
        }
    ))
    fig.update_layout(height=300, margin=dict(l=20, r=20, t=50, b=20))
    return fig

def create_donut_chart(pm25, pm10):
    """Biểu đồ tỷ lệ PM2.5 trong PM10"""
    # Handle None values
    if pm25 is None:
        pm25 = 0.0
    if pm10 is None:
        pm10 = 0.0
    
    # Convert to float and handle NaN
    try:
        pm25 = float(pm25) if not pd.isna(pm25) else 0.0
        pm10 = float(pm10) if not pd.isna(pm10) else 0.0
    except (ValueError, TypeError):
        pm25 = 0.0
        pm10 = 0.0
    
    # Return None if pm10 is 0 or invalid
    if pm10 == 0 or pm10 is None:
        return None
    
    other_pm = max(0.0, pm10 - pm25)
    labels = ['PM2.5 (Bụi mịn)', 'PM thô khác']
    values = [pm25, other_pm]
    colors = ['#ff7f0e', '#e0e0e0']
    
    # Calculate percentage safely
    try:
        percentage = int((pm25 / pm10) * 100) if pm10 > 0 else 0
    except (ZeroDivisionError, TypeError):
        percentage = 0
    
    fig = go.Figure(data=[go.Pie(labels=labels, values=values, hole=.7, marker_colors=colors)])
    fig.update_layout(
        title_text="Tỷ trọng PM2.5 / PM10",
        showlegend=True,
        height=300,
        margin=dict(l=20, r=20, t=50, b=20),
        annotations=[dict(text=f'{percentage}%', x=0.5, y=0.5, font_size=20, showarrow=False)]
    )
    return fig

def create_trend_chart(df, cols, title):
    fig = go.Figure()
    colors = {'pm25': '#ff7f0e', 'pm10': '#2ca02c', 'pm1': '#d62728', 'temperature': '#e74c3c', 'relativehumidity': '#3498db'}
    
    for col in cols:
        if col in df.columns:
            fig.add_trace(go.Scatter(
                x=df['datetime'], y=df[col],
                mode='lines',
                name=col.upper(),
                line=dict(width=2, color=colors.get(col, '#333')),
                fill='tozeroy' if len(cols) == 1 else None # Fill nếu chỉ có 1 đường
            ))
            
    fig.update_layout(
        title=title,
        height=350,
        hovermode='x unified',
        xaxis_title="Thời gian",
        yaxis_title="Giá trị",
        template='plotly_white',
        legend=dict(orientation="h", y=1.1)
    )
    return fig

# --- MAIN APP ---
def main():
    setup_page()
    
    st.markdown('<h1 class="main-header">🌬️ Air Quality Monitor</h1>', unsafe_allow_html=True)
    st.markdown('<div class="sub-header">Hệ thống giám sát chất lượng không khí Real-time</div>', unsafe_allow_html=True)
    
    # --- SIDEBAR ---
    with st.sidebar:
        st.header("⚙️ Điều khiển")
        data_limit = st.select_slider("Số lượng mẫu dữ liệu", options=[100, 500, 1000, 2000, 5000], value=1000)
        auto_refresh = st.toggle("🔄 Tự động làm mới (5s)", value=True)
        
        st.divider()
        col1, col2 = st.columns(2)
        if col1.button("Làm mới", use_container_width=True):
            fetch_realtime_data.clear()
            fetch_history_data.clear()
            st.rerun()
        if col2.button("Xóa Cache", use_container_width=True):
            fetch_realtime_data.clear()
            fetch_history_data.clear()
            st.success("Đã xóa cache")
            time.sleep(1)
            st.rerun()

    # --- FETCH DATA ---
    # Real-time data từ Spark (Redis) cho chỉ số hiện tại
    df_realtime = fetch_realtime_data()
    
    # Historical data từ Cassandra cho xu hướng và bảng chi tiết
    df_history = fetch_history_data(limit=data_limit)
    
    # Kiểm tra dữ liệu real-time
    if df_realtime.empty:
        st.warning("⚠️ Không có dữ liệu real-time. Vui lòng kiểm tra Spark Streaming và Redis.")
        # Fallback: dùng dữ liệu lịch sử nếu có
        if df_history.empty:
            st.error("❌ Không có dữ liệu nào. Vui lòng kiểm tra Server.")
            return
        df_realtime = df_history.head(1)  # Dùng bản ghi mới nhất từ lịch sử
    
    latest = df_realtime.iloc[0]
    
    # --- ALERT SECTION ---
    # Hiển thị cảnh báo nếu AQI > 150
    aqi_val = safe_get_value(latest, 'aqi', 0)
    if aqi_val > 150:
        location_id = safe_get_value(latest, 'location_id', None)
        quality = latest.get('quality', 'N/A') if 'quality' in latest.index else 'N/A'
        location_str = f"{int(location_id)}" if location_id is not None and location_id != 'N/A' else 'N/A'
        
        if aqi_val > 200:
            st.error(f"🚨 **CẢNH BÁO NGUY HẠI**: Trạm {location_str} có AQI = **{int(aqi_val)}** ({quality}). Mức độ ô nhiễm rất cao!")
        elif aqi_val > 150:
            st.warning(f"⚠️ **CẢNH BÁO**: Trạm {location_str} có AQI = **{int(aqi_val)}** ({quality}). Chất lượng không khí kém!")
    
    # --- HEADER INFO ---
    col_info1, col_info2 = st.columns([3, 1])
    with col_info1:
        try:
            if 'datetime' in latest.index and pd.notna(latest['datetime']):
                if isinstance(latest['datetime'], pd.Timestamp):
                    last_update = latest['datetime'].strftime('%H:%M:%S %d/%m/%Y')
                else:
                    last_update = str(latest['datetime'])
            else:
                last_update = 'N/A'
        except Exception:
            last_update = 'N/A'
        total_records = len(df_history) if not df_history.empty else 0
        st.info(f"📅 Cập nhật lần cuối (Real-time): **{last_update}** | Tổng số bản ghi (History): **{total_records}**")
    
    # --- HEADLINE SECTION (KPIs) - Dùng dữ liệu REAL-TIME từ Spark ---
    st.markdown('<div class="section-title">🎯 Chỉ số hiện tại (Real-time từ Spark)</div>', unsafe_allow_html=True)
    
    # Layout: Gauge (Left) | Metrics & Info (Right)
    col_kpi1, col_kpi2, col_kpi3 = st.columns([2, 1, 1])
    
    with col_kpi1:
        # Biểu đồ Gauge cho AQI
        aqi_val = safe_get_value(latest, 'aqi', 0)
        st.plotly_chart(create_gauge_chart(aqi_val), use_container_width=True)
        
        # Cảnh báo text
        aqi_lvl = get_aqi_level(aqi_val)
        aqi_clr = get_aqi_color(aqi_val)
        st.markdown(f"""
            <div style="text-align: center; font-weight: bold; color: {aqi_clr}; font-size: 1.2rem; margin-top: -20px;">
                Mức độ: {aqi_lvl}
            </div>
        """, unsafe_allow_html=True)

    with col_kpi2:
        pm25_val = safe_get_value(latest, 'pm25', 0)
        pm10_val = safe_get_value(latest, 'pm10', 0)
        pm1_val = safe_get_value(latest, 'pm1', 0)
        st.metric("PM2.5 (Bụi mịn)", f"{pm25_val:.1f} µg/m³", delta_color="inverse")
        st.metric("PM10 (Bụi thô)", f"{pm10_val:.1f} µg/m³")
        st.metric("PM1 (Siêu mịn)", f"{pm1_val:.1f} µg/m³")

    with col_kpi3:
        temp_val = safe_get_value(latest, 'temperature', 0)
        humidity_val = safe_get_value(latest, 'relativehumidity', 0)
        st.metric("Nhiệt độ", f"{temp_val:.1f} °C")
        st.metric("Độ ẩm", f"{humidity_val:.1f} %")
        # Biểu đồ Donut tỷ lệ bụi
        donut_chart = create_donut_chart(pm25_val, pm10_val)
        if donut_chart is not None:
            st.plotly_chart(donut_chart, use_container_width=True)
        else:
            st.info("PM10 data not available for ratio chart")

    st.divider()

    # --- TRENDS SECTION - Dùng dữ liệu LỊCH SỬ từ Cassandra ---
    st.markdown('<div class="section-title">📈 Xu hướng theo thời gian (Lịch sử từ Cassandra)</div>', unsafe_allow_html=True)
    
    if df_history.empty:
        st.warning("⚠️ Không có dữ liệu lịch sử từ Cassandra để hiển thị xu hướng.")
    else:
        tab1, tab2 = st.tabs(["💨 Nồng độ Bụi", "🌡️ Môi trường"])
        
        with tab1:
            st.plotly_chart(create_trend_chart(df_history, ['pm25', 'pm10'], "Diễn biến PM2.5 và PM10"), use_container_width=True)
        
        with tab2:
            st.plotly_chart(create_trend_chart(df_history, ['temperature', 'relativehumidity'], "Diễn biến Nhiệt độ & Độ ẩm"), use_container_width=True)

    # --- DATA TABLE SECTION - Dùng dữ liệu LỊCH SỬ từ Cassandra ---
    st.divider()
    with st.expander("📋 Xem dữ liệu chi tiết (Lịch sử từ Cassandra)", expanded=False):
        if df_history.empty:
            st.warning("⚠️ Không có dữ liệu lịch sử từ Cassandra để hiển thị.")
        else:
            # Stats
            st.markdown("##### Thống kê nhanh")
            stats_cols = ['aqi', 'pm25', 'pm10', 'temperature']
            available_stats_cols = [col for col in stats_cols if col in df_history.columns]
            if available_stats_cols:
                stats = df_history[available_stats_cols].mean().to_frame().T
                stats.index = ['Trung bình']
                st.dataframe(stats.style.format("{:.2f}"), width='stretch', hide_index=True)
            
            st.markdown("##### Dữ liệu thô")
            cols_to_show = ['datetime', 'aqi', 'quality', 'pm25', 'pm10', 'temperature', 'relativehumidity']
            available_cols = [col for col in cols_to_show if col in df_history.columns]
            if available_cols:
                st.dataframe(
                    df_history[available_cols].head(100),
                    width='stretch',
                    height=300,
                    hide_index=True
                )
            else:
                st.warning("⚠️ Không có cột dữ liệu phù hợp để hiển thị.")

    # --- AUTO REFRESH ---
    if auto_refresh:
        time.sleep(5)
        st.rerun()

if __name__ == "__main__":
    main()