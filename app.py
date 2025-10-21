import streamlit as st
import pandas as pd
import requests
import time
from datetime import datetime
import calendar 

# --- KRX API 정보 설정 ---
API_URL = 'https://data-dbg.krx.co.kr/svc/apis/etp/etf_bydd_trd' 

try:
    AUTH_KEY = st.secrets["krx_api"]["auth_key"]
except (KeyError, AttributeError):
    st.error("⚠️ Streamlit Secrets 설정이 필요합니다. 'krx_api' 섹션에 'auth_key'를 확인하세요.")
    AUTH_KEY = '16B23371BBDC4107AB07CBBBBA14ADBCD2525DF0' 
    st.info("현재는 코드에 직접 입력된 테스트 키로 실행됩니다. 보안을 위해 Secrets를 사용해주세요.")


# --- 데이터 가져오기 함수 (POST 요청 및 인증키/기준일자 포함) ---
@st.cache_data(ttl=60) 
def fetch_etf_data(api_url, auth_key):
    """KRX API에 POST 요청을 보내 ETF 데이터를 가져와 DataFrame과 기준일자를 반환합니다."""
    
    # 1. API 요청 본문(Body) 데이터 구성
    # ⚠️ 인증키와 기준일자(basDd)를 요청 본문에 JSON 형태로 포함합니다.
    # 기준일자는 오늘 날짜로 자동 설정합니다 (YYYYMMDD 형식).
    today_str = datetime.now().strftime('%Y%m%d') 
    
    payload = {
        # KRX API의 POST 인증 및 서비스 키 필드에 대한 추정
        # 'Authorization' 또는 'serviceKey'를 'AUTH_KEY'로 전달하도록 추정
        'AUTH_KEY': auth_key, 
        'basDd': today_str,
        'etc_parm': 'Y', # 필요한 기타 파라미터가 있을 경우를 대비 (필요 없으면 제거 가능)
    }

    headers = {
        # Content-Type을 application/json으로 설정
        'Content-Type': 'application/json',
    }
    
    try:
        # 2. POST 요청 실행 (data 인수에 JSON 직렬화된 payload를 전달)
        response = requests.post(api_url, json=payload, headers=headers, timeout=15)
        response.raise_for_status() # HTTP 오류(401 포함) 발생 시 예외 처리
        data = response.json()
        
        # 3. 데이터 블록 추출
        # 응답이 최상위 레벨에 'OutBlock_1'을 바로 가지지 않을 수도 있어, 전체 응답을 확인합니다.
        etf_list = data.get('OutBlock_1', data.get('outBlock1', [])) 
        
        if not etf_list:
            # KRX API가 오류를 JSON 메시지로 반환하는 경우를 처리
            error_msg = data.get('error_message', 'API 응답에서 유효한 데이터("OutBlock_1")를 찾을 수 없습니다.')
            st.warning(f"데이터 추출 실패: {error_msg}")
            return pd.DataFrame(), None 

        df = pd.DataFrame(etf_list)
        
        # 4. 기준일자 추출 및 포맷팅
        base_date_raw = etf_list[0].get('BAS_DD')
        if base_date_raw and len(base_date_raw) == 8:
            base_date = f"{base_date_raw[:4]}-{base_date_raw[4:6]}-{base_date_raw[6:]}"
        else:
            base_date = "알 수 없음"

        # 5. 컬럼 이름 매핑 및 데이터 타입 변환
        df = df.rename(columns={
            'ISU_NM': '종목명',         
            'TDD_CLSPRC': '현재가',     
            'FLUC_RT': '등락률 (%)',    
            'ACC_TRDVOL': '거래량'     
        })
        
        df['현재가'] = pd.to_numeric(df['현재가'], errors='coerce').fillna(0).astype(int)
        df['등락률 (%)'] = pd.to_numeric(df['등락률 (%)'], errors='coerce').fillna(0).round(2)
        df['거래량'] = pd.to_numeric(df['거래량'], errors='coerce').fillna(0).astype(int)
        
        return df[['종목명', '현재가', '등락률 (%)', '거래량']], base_date

    except requests.exceptions.RequestException as e:
        # 인증 오류(401) 또는 연결 오류 처리
        st.error(f"데이터 로드 실패: API 연결 오류. 상세: {e}")
        return pd.DataFrame(), None 


# --- Streamlit 앱 메인 로직 (이전과 동일) ---
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
    last_base_date = None

    while True:
        now = datetime.now()
        current_time = now.strftime("%Y-%m-%d %H:%M:%S")
        
        weekday = now.weekday()
        
        if weekday >= calendar.SATURDAY: 
            status_placeholder.markdown(
                f"**데이터 기준일:** **{last_base_date if last_base_date else '확인 중'}** | "
                f"**최종 업데이트 시간:** {current_time} (오늘은 **주말**로, KRX 시장 휴장일입니다. 이전 데이터가 표시됩니다.)"
            )
            sleep_time = 3600 
            
        else:
            status_placeholder.markdown(f"**최종 업데이트 시간:** {current_time} (데이터 로딩 중...)")

            etf_df, base_date = fetch_etf_data(API_URL, AUTH_KEY)
            
            if not etf_df.empty:
                last_valid_df = etf_df 
                last_base_date = base_date 
                
                status_placeholder.markdown(
                    f"**데이터 기준일:** **{last_base_date}** | "
                    f"**최종 업데이트 시간:** {current_time}"
                )
            
            sleep_time = 60 

        if not last_valid_df.empty:
            
            sorted_df = last_valid_df.sort_values(by='등락률 (%)', ascending=False).reset_index(drop=True)
            
            sorted_df.index = sorted_df.index + 1
            sorted_df = sorted_df.reset_index().rename(columns={'index': '순위'})
            top_10_df = sorted_df.head(10)
            
            def color_rate(val):
                color = 'red' if val > 0 else ('blue' if val < 0 else 'gray')
                return f'color: {color}; font-weight: bold;'
            
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
             table_placeholder.info(
                 f"데이터 로딩 중이거나, API 호출에 오류가 발생했습니다. (마지막 시도: {current_time}). POST 인증 방식을 확인해주세요."
             )

        time.sleep(sleep_time)

if __name__ == "__main__":
    main()
