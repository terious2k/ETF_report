import streamlit as st
import pandas as pd
import requests
import time
from datetime import datetime
import calendar 

# --- KRX API 정보 설정 ---
API_URL = 'https://data-dbg.krx.co.kr/svc/sample/apis/etp/etf_bydd_trd.json'
API_ID = 'etf_bydd_trd'
AUTH_KEY = '74D1B99DFBF345BBA3FB4476510A4BED4C78D13A'

# --- 데이터 가져오기 함수 (60초 동안 캐싱되어 1분에 한 번만 API 호출) ---
@st.cache_data(ttl=60) 
def fetch_etf_data(api_url, api_id, auth_key):
    """KRX API에서 ETF 데이터를 가져와 DataFrame으로 반환합니다."""
    
    headers = {
        'Content-Type': 'application/json',
        'API-KEY': auth_key, 
        'API-ID': api_id
    }
    
    try:
        response = requests.get(api_url, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        etf_list = data.get('output', [])
        if not etf_list:
            # 유효 데이터가 없을 경우 빈 DataFrame 반환
            return pd.DataFrame() 

        df = pd.DataFrame(etf_list)
        
        # 컬럼 이름 정리 및 데이터 타입 변환
        df = df.rename(columns={
            'SAMPLE_ETC_KOR_NM': '종목명',
            'TRD_PRIC': '현재가',
            'flucRate': '등락률 (%)',
            'ACC_TRD_QTY': '거래량'
        })
        
        # 숫자 형식으로 변환 (오류 발생 시 0으로 처리)
        df['현재가'] = pd.to_numeric(df['현재가'], errors='coerce').fillna(0).astype(int)
        df['등락률 (%)'] = pd.to_numeric(df['등락률 (%)'], errors='coerce').fillna(0).round(2)
        df['거래량'] = pd.to_numeric(df['거래량'], errors='coerce').fillna(0).astype(int)
        
        return df[['종목명', '현재가', '등락률 (%)', '거래량']]

    except requests.exceptions.RequestException as e:
        # API 연결 또는 인증 오류 발생 시
        st.error(f"데이터 로드 실패: API 연결 오류. 오류 상세: {e}")
        return pd.DataFrame()


# --- Streamlit 앱 메인 로직 ---
def main():
    st.set_page_config(
        page_title="국내 ETF 등락률 순위",
        layout="wide",
        initial_sidebar_state="collapsed"
    )
    
    st.title("📈 국내 ETF 실시간 등락률 순위")
    st.markdown("데이터는 **1분**마다 자동으로 업데이트되며, 등락률 순으로 정렬됩니다.")
    
    # 상태 메시지 및 테이블 출력을 위한 플레이스홀더 (Placeholder) 설정
    status_placeholder = st.empty()
    table_placeholder = st.empty()
    
    # 마지막으로 성공적으로 가져온 데이터를 저장할 변수
    last_valid_df = pd.DataFrame() 

    while True:
        now = datetime.now()
        current_time = now.strftime("%Y-%m-%d %H:%M:%S")
        
        # 현재 요일 확인 (월=0, 화=1, ..., 토=5, 일=6)
        weekday = now.weekday()
        
        if weekday >= calendar.SATURDAY: 
            # 주말(토/일) 처리: API 호출을 건너뜁니다.
            status_placeholder.markdown(
                f"**최종 업데이트 시간:** {current_time} (오늘은 **주말**로, KRX 시장 휴장일입니다. 이전 데이터가 표시됩니다.)"
            )
            # 주말에는 1시간(3600초)마다 루프를 돌게 하여 부하를 줄입니다.
            sleep_time = 3600 
            
        else:
            # 평일 처리: 데이터 로드를 시도하고 1분마다 업데이트합니다.
            status_placeholder.markdown(f"**최종 업데이트 시간:** {current_time} (KRX 샘플 데이터)")

            etf_df = fetch_etf_data(API_URL, API_ID, AUTH_KEY)
            
            if not etf_df.empty:
                # 유효한 데이터가 있다면 저장
                last_valid_df = etf_df 
            
            sleep_time = 60 # 평일에는 60초마다 업데이트

        # 데이터 표시 로직 (last_valid_df 사용)
        if not last_valid_df.empty:
            # 1. 등락률 순으로 정렬 (내림차순)
            sorted_df = last_valid_df.sort_values(by='등락률 (%)', ascending=False).reset_index(drop=True)
            
            # 2. 순위 추가 및 상위 10개만 선택
            sorted_df.index = sorted_df.index + 1
            sorted_df = sorted_df.reset_index().rename(columns={'index': '순위'})
            top_10_df = sorted_df.head(10)
            
            # 3. 등락률에 따라 색상을 지정하는 스타일링 함수
            def color_rate(val):
                color = 'red' if val > 0 else ('blue' if val < 0 else 'gray')
                return f'color: {color}; font-weight: bold;'
            
            # 4. Streamlit에 최종 표 표시 (스타일 적용)
            table_placeholder.dataframe(
                top_10_df.style.applymap(
                    color_rate, 
                    subset=['등락률 (%)']
                ).format({
                    '현재가': '{:,.0f}', # 천 단위 쉼표
                    '거래량': '{:,.0f}'
                }),
                use_container_width=True,
                hide_index=True # 인덱스 (DataFrame의 기본 행 번호) 숨김
            )
        else:
             # 데이터 로드 실패 또는 초기 상태 메시지
             table_placeholder.info("데이터 로딩 중이거나, API 호출에 오류가 발생했습니다. 잠시 후 재시도됩니다.")

        # 설정된 시간만큼 대기
        time.sleep(sleep_time)

if __name__ == "__main__":
    main()
