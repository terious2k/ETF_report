import streamlit as st
import pandas as pd
import requests
import time
from datetime import datetime
import calendar 

# --- KRX API 정보 설정 ---
# ⚠️ 새로운 엔드포인트 주소로 업데이트되었습니다.
API_URL = 'https://data-dbg.krx.co.kr/svc/apis/etp/etf_bydd_trd.json' 

try:
    # Streamlit Secrets에서 AUTH_KEY를 안전하게 불러옵니다.
    AUTH_KEY = st.secrets["krx_api"]["auth_key"]
except (KeyError, AttributeError):
    st.error("⚠️ Streamlit Secrets 설정이 필요합니다. 'krx_api' 섹션에 'auth_key'를 확인하세요.")
    # 제공해주신 새 인증키를 기본값으로 사용합니다.
    AUTH_KEY = '16B23371BBDC4107AB07CBBBBA14ADBCD2525DF0' 
    st.info("현재는 코드에 직접 입력된 테스트 키로 실행됩니다. 보안을 위해 Secrets를 사용해주세요.")


# --- 데이터 가져오기 함수 (60초 동안 캐싱) ---
@st.cache_data(ttl=60) 
def fetch_etf_data(api_url, auth_key):
    """KRX API에서 ETF 데이터를 가져와 DataFrame으로 반환합니다."""
    
    # 헤더에 인증키를 포함하여 API를 호출합니다.
    headers = {
        'Content-Type': 'application/json',
        'API-KEY': auth_key, 
    }
    
    try:
        response = requests.get(api_url, headers=headers, timeout=10)
        response.raise_for_status() # HTTP 오류(4xx, 5xx) 발생 시 예외 처리
        data = response.json()
        
        # 제공해주신 JSON 구조('OutBlock_1')를 사용
        etf_list = data.get('OutBlock_1', []) 
        
        if not etf_list:
            st.warning("API 응답에서 유효한 데이터('OutBlock_1')를 찾을 수 없습니다. 키 또는 엔드포인트를 확인하세요.")
            return pd.DataFrame() 

        df = pd.DataFrame(etf_list)
        
        # 컬럼 이름 매핑 변경: 제공된 JSON 필드명으로 수정
        df = df.rename(columns={
            'ISU_NM': '종목명',         
            'TDD_CLSPRC': '현재가',     
            'FLUC_RT': '등락률 (%)',    
            'ACC_TRDVOL': '거래량'     
        })
        
        # 데이터 타입 변환 및 정리
        df['현재가'] = pd.to_numeric(df['현재가'], errors='coerce').fillna(0).astype(int)
        df['등락률 (%)'] = pd.to_numeric(df['등락률 (%)'], errors='coerce').fillna(0).round(2)
        df['거래량'] = pd.to_numeric(df['거래량'], errors='coerce').fillna(0).astype(int)
        
        return df[['종목명', '현재가', '등락률 (%)', '거래량']]

    except requests.exceptions.RequestException as e:
        st.error(f"데이터 로드 실패: API 연결 오류. 상세: {e}")
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
    
    status_placeholder = st.empty()
    table_placeholder = st.empty()
    
    last_valid_df = pd.DataFrame() 

    while True:
        now = datetime.now()
        current_time = now.strftime("%Y-%m-%d %H:%M:%S")
        
        weekday = now.weekday()
        
        if weekday >= calendar.SATURDAY: 
            status_placeholder.markdown(
                f"**최종 업데이트 시간:** {current_time} (오늘은 **주말**로, KRX 시장 휴장일입니다. 이전 데이터가 표시됩니다.)"
            )
            sleep_time = 3600 
            
        else:
            status_placeholder.markdown(f"**최종 업데이트 시간:** {current_time} (KRX 샘플 데이터)")

            etf_df = fetch_etf_data(API_URL, AUTH_KEY)
            
            if not etf_df.empty:
                last_valid_df = etf_df 
            
            sleep_time = 60 

        # 데이터 표시 로직 (last_valid_df 사용)
        if not last_valid_df.empty:
            
            # 등락률 순으로 정렬 (내림차순)
            sorted_df = last_valid_df.sort_values(by='등락률 (%)', ascending=False).reset_index(drop=True)
            
            # 순위 추가 및 상위 10개만 선택
            sorted_df.index = sorted_df.index + 1
            sorted_df = sorted_df.reset_index().rename(columns={'index': '순위'})
            top_10_df = sorted_df.head(10)
            
            # 등락률에 따라 색상을 지정하는 스타일링 함수
            def color_rate(val):
                color = 'red' if val > 0 else ('blue' if val < 0 else 'gray')
                return f'color: {color}; font-weight: bold;'
            
            # Streamlit에 최종 표 표시
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
        else:
             table_placeholder.info("데이터 로딩 중이거나, API 호출에 오류가 발생했습니다. 잠시 후 재시도됩니다.")

        # 설정된 시간만큼 대기
        time.sleep(sleep_time)

if __name__ == "__main__":
    main()
