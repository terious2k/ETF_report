import streamlit as st
import pandas as pd
import requests
import time
from datetime import datetime

# --- KRX API 정보 (이전에 제공해주신 정보 사용) ---
API_URL = 'https://data-dbg.krx.co.kr/svc/sample/apis/etp/etf_bydd_trd.json'
API_ID = 'etf_bydd_trd'
AUTH_KEY = '74D1B99DFBF345BBA3FB4476510A4BED4C78D13A'

# --- 데이터 가져오기 함수 (인증키 포함) ---
@st.cache_data(ttl=60) # 데이터를 60초 동안 캐싱하여 API 호출 최소화 및 자동 업데이트 구현
def fetch_etf_data(api_url, api_id, auth_key):
    """KRX API에서 ETF 데이터를 가져오는 함수"""
    
    # 1. API 요청 설정 (헤더에 인증키와 ID 포함)
    headers = {
        'Content-Type': 'application/json',
        # KRX API에 맞춰 인증키를 헤더에 포함 (정확한 필드명은 KRX 문서 확인 필요)
        'API-KEY': auth_key, 
        'API-ID': api_id
    }
    
    try:
        response = requests.get(api_url, headers=headers, timeout=10) # 10초 타임아웃 설정
        response.raise_for_status() # HTTP 오류 발생 시 예외 발생
        data = response.json()
        
        # 2. 데이터 유효성 검사 및 DataFrame 생성
        etf_list = data.get('output', [])
        if not etf_list:
            st.error("API 응답에 유효한 ETF 데이터가 없습니다.")
            return pd.DataFrame()

        df = pd.DataFrame(etf_list)
        
        # 3. 필요한 컬럼 선택 및 데이터 타입 변환
        df = df.rename(columns={
            'SAMPLE_ETC_KOR_NM': '종목명',
            'TRD_PRIC': '현재가',
            'flucRate': '등락률 (%)',
            'ACC_TRD_QTY': '거래량'
        })
        
        df['현재가'] = pd.to_numeric(df['현재가'], errors='coerce').fillna(0).astype(int)
        df['등락률 (%)'] = pd.to_numeric(df['등락률 (%)'], errors='coerce').fillna(0).round(2)
        df['거래량'] = pd.to_numeric(df['거래량'], errors='coerce').fillna(0).astype(int)
        
        return df[['종목명', '현재가', '등락률 (%)', '거래량']]

    except requests.exceptions.RequestException as e:
        st.error(f"데이터 로드 실패: API 연결 또는 인증 오류. 키({auth_key[:8]}...) 확인 필요.")
        st.error(f"오류 상세: {e}")
        return pd.DataFrame()

# --- Streamlit 앱 메인 로직 ---
def main():
    st.set_page_config(
        page_title="국내 ETF 등락률 순위",
        layout="wide",
        initial_sidebar_state="collapsed"
    )
    
    # 1. 제목 및 설명
    st.title("📈 국내 ETF 실시간 등락률 순위")
    st.markdown("데이터는 **1분**마다 자동으로 업데이트되며, 등락률 순으로 정렬됩니다.")
    
    # 2. 데이터 업데이트 시간 표시를 위한 PlaceHolder 설정
    status_placeholder = st.empty()
    table_placeholder = st.empty()
    
    # 3. 1분(60초)마다 반복 실행하여 업데이트 효과 구현
    while True:
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        status_placeholder.markdown(f"**최종 업데이트 시간:** {current_time} (KRX 샘플 데이터)")
        
        # 4. 데이터 가져오기 및 처리
        etf_df = fetch_etf_data(API_URL, API_ID, AUTH_KEY)
        
        if not etf_df.empty:
            # 5. 등락률 순으로 정렬 (내림차순)
            sorted_df = etf_df.sort_values(by='등락률 (%)', ascending=False).reset_index(drop=True)
            
            # 순위 추가
            sorted_df.index = sorted_df.index + 1
            sorted_df = sorted_df.reset_index().rename(columns={'index': '순위'})

            # 상위 10개만 표시
            top_10_df = sorted_df.head(10)
            
            # 6. 등락률에 따라 셀 색상을 지정하는 함수 (CSS 스타일링)
            def color_rate(val):
                color = 'red' if val > 0 else ('blue' if val < 0 else 'gray')
                return f'color: {color}; font-weight: bold;'
            
            # 7. 표 표시
            table_placeholder.dataframe(
                top_10_df.style.applymap(
                    color_rate, 
                    subset=['등락률 (%)']
                ).format({
                    '현재가': '{:,.0f}',
                    '거래량': '{:,.0f}'
                }),
                use_container_width=True,
                hide_index=True
            )
        
        # 60초 대기 후 루프 재실행 (st.cache_data의 TTL과 연동)
        time.sleep(60)

if __name__ == "__main__":
    main()
