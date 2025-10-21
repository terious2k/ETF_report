import streamlit as st
import pandas as pd
import requests
import time
from datetime import datetime, timedelta
import calendar 

# --- KRX API 정보 설정 ---
API_URL = 'https://data-dbg.krx.co.kr/svc/apis/etp/etf_bydd_trd' 
MARKET_CLOSE_TIME = 15 # 장 마감 시 (시간)
MARKET_CLOSE_MINUTE = 30 # 장 마감 분

try:
    AUTH_KEY = st.secrets["krx_api"]["auth_key"]
except (KeyError, AttributeError):
    st.error("⚠️ Streamlit Secrets 설정이 필요합니다. 'krx_api' 섹션에 'auth_key'를 확인하세요.")
    AUTH_KEY = '16B23371BBDC4107AB07CBBBBA14ADBCD2525DF0' 
    st.info("현재는 코드에 직접 입력된 테스트 키로 실행됩니다. 보안을 위해 Secrets를 사용해주세요.")


# --- 기준일자 계산 함수 추가 ---
def get_trading_date():
    """현재 시각을 기준으로 API 요청에 사용할 기준일자(YYYYMMDD)를 반환합니다."""
    now = datetime.now()
    
    # 1. 요일 확인 (월=0, 화=1, ..., 일=6)
    weekday = now.weekday()
    
    # 2. 기준일자 초기화
    target_date = now.date()
    
    # 3. 평일(월~금) 로직
    if weekday < calendar.SATURDAY: 
        # 장 마감 시각 (15:30) 설정
        close_time = now.replace(hour=MARKET_CLOSE_TIME, minute=MARKET_CLOSE_MINUTE, second=0, microsecond=0)
        
        if now < close_time:
            # 15:30 이전: 아직 당일 최종 데이터가 나오지 않았으므로 전날(T-1)의 데이터를 요청
            target_date -= timedelta(days=1)
            
        # 15:30 이후: 당일 최종 데이터가 나왔으므로 오늘 날짜(T)를 요청 (target_date = now.date() 유지)
        
    # 4. 토/일요일 로직 (주말에는 API 호출을 하지 않으므로, 이 함수는 평일에만 주로 호출됨)
    # 다만, 혹시 모를 상황에 대비하여 가장 최근 거래일로 날짜를 맞춥니다.
    else:
        # 일요일(6)이면 2일 전(금요일), 토요일(5)이면 1일 전(금요일)로 이동
        days_to_subtract = weekday - calendar.FRIDAY
        target_date -= timedelta(days=days_to_subtract)
        
    # 5. 최종 YYYYMMDD 형식 반환
    return target_date.strftime('%Y%m%d')


# --- 데이터 가져오기 함수 (POST 요청 및 인증키/기준일자 포함) ---
@st.cache_data(ttl=60) 
def fetch_etf_data(api_url, auth_key):
    """KRX API에 POST 요청을 보내 ETF 데이터를 가져와 DataFrame과 기준일자를 반환합니다."""
    
    # ⚠️ 1. 요청할 기준일자 계산
    target_basDd = get_trading_date()
    
    # 2. API 요청 본문(Body) 데이터 구성
    payload = {
        'AUTH_KEY': auth_key, 
        'basDd': target_basDd, # ⚠️ 계산된 기준일자 사용
        'etc_parm': 'Y', 
    }

    headers = {
        'Content-Type': 'application/json',
    }
    
    try:
        response = requests.post(api_url, json=payload, headers=headers, timeout=15)
        response.raise_for_status() 
        data = response.json()
        
        etf_list = data.get('OutBlock_1', data.get('outBlock1', [])) 
        
        if not etf_list:
            error_msg = data.get('error_message', 'API 응답에서 유효한 데이터("OutBlock_1")를 찾을 수 없습니다.')
            st.warning(f"데이터 추출 실패: {error_msg}")
            return pd.DataFrame(), None 

        df = pd.DataFrame(etf_list)
        
        # 3. 기준일자 추출 및 포맷팅 (데이터에서 추출된 최종 기준일자를 사용)
        base_date_raw = etf_list[0].get('BAS_DD')
        if base_date_raw and len(base_date_raw) == 8:
            base_date = f"{base_date_raw[:4]}-{base_date_raw[4:6]}-{base_date_raw[6:]}"
        else:
            base_date = "알 수 없음"

        # 4. 컬럼 이름 매핑 및 데이터 타입 변환 (이전과 동일)
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
        st.error(f"데이터 로드 실패: API 연결 오류. 상세: {e}")
        return pd.DataFrame(), None 


# --- Streamlit 앱 메인 로직 ---
def main():
    st.set_page_config(
        page_title="국내 ETF 등락률 순위",
        layout="wide",
        initial_sidebar_state="collapsed"
    )
    
    st.title("📈 국내 ETF 실시간 등락률 순위")
    st.markdown(
        f"데이터는 **1분**마다 자동으로 업데이트됩니다. "
        f"({MARKET_CLOSE_TIME}시 {MARKET_CLOSE_MINUTE}분 장 마감 기준)"
    )
    
    status_placeholder = st.empty()
    table_placeholder = st.empty()
    
    last_valid_df = pd.DataFrame() 
    last_base_date = None
    
    # ⚠️ 메인 루프에서 주말 처리는 이제 'get_trading_date' 함수를 통해 간접적으로 처리되지만, 
    # API 호출을 건너뛰는 기존 로직을 유지하여 불필요한 호출을 막습니다.
    while True:
        now = datetime.now()
        current_time = now.strftime("%Y-%m-%d %H:%M:%S")
        weekday = now.weekday()
        
        # 주말(토/일) 처리
        if weekday >= calendar.SATURDAY: 
            status_placeholder.markdown(
                f"**데이터 기준일:** **{last_base_date if last_base_date else '확인 중'}** | "
                f"**최종 업데이트 시간:** {current_time} (오늘은 **주말**로, KRX 시장 휴장일입니다. 이전 데이터가 표시됩니다.)"
            )
            sleep_time = 3600 # 주말에는 1시간 대기
            
        else:
            # 평일 처리
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
