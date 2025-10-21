import streamlit as st
import pandas as pd
import requests
from datetime import datetime, timedelta
import calendar 

# --- KRX API 정보 설정 ---
# ETF 일별 매매 정보 엔드포인트
API_URL = 'https://data-dbg.krx.co.kr/svc/apis/etp/etf_bydd_trd' 

try:
    # ⚠️ Secrets 키 이름을 통일합니다. (이전 코드에서 사용된 이름으로 가정)
    # 실제로는 이 부분의 변수 이름도 KRX_API_KEY로 통일해야 안전하지만,
    # 이전 코드의 흐름을 유지하기 위해 AUTH_KEY를 사용합니다.
    AUTH_KEY = st.secrets["krx_api"]["auth_key"] 
except (KeyError, AttributeError):
    st.error("⚠️ Streamlit Secrets 설정이 필요합니다. 'krx_api' 섹션에 'auth_key'를 확인하세요.")
    AUTH_KEY = '16B23371BBDC4107AB07CBBBBA14ADBCD2525DF0' 
    st.info("현재는 코드에 직접 입력된 테스트 키로 실행됩니다. 보안을 위해 Secrets를 사용해주세요.")


# --- 데이터 가져오기 함수 (GET 요청 및 AUTH_KEY 헤더 사용) ---
@st.cache_data(ttl=60) 
def fetch_etf_data(api_url, auth_key, target_basDd):
    """KRX API에 GET 요청을 보내 ETF 데이터를 가져와 DataFrame과 기준일자를 반환합니다."""
    
    # ⚠️ 1. 인증 방식 확정: Header에 'AUTH_KEY' 사용
    headers = {
        # Content-Type은 GET 요청에서 필수는 아니지만, 명확성을 위해 유지
        'Content-Type': 'application/json',
        'AUTH_KEY': auth_key, # 👈 성공한 코드의 인증 헤더 사용!
    }
    
    # 2. 파라미터 설정: 쿼리 파라미터로 'basDd' 전달
    params = {
        'basDd': target_basDd, # 👈 선택된 조회 기준일자 사용
        'etc_parm': 'Y', 
    }
    
    try:
        # ⚠️ 3. GET 요청 실행 (POST -> GET 변경)
        response = requests.get(api_url, headers=headers, params=params, timeout=15)
        response.raise_for_status() 
        data = response.json()
        
        # 4. 데이터 블록 추출
        etf_list = data.get('OutBlock_1', data.get('outBlock1', [])) 
        
        if not etf_list:
            error_msg = data.get('error_message', 'API 응답에서 유효한 데이터("OutBlock_1")를 찾을 수 없습니다.')
            st.warning(f"데이터 추출 실패: {error_msg}")
            return pd.DataFrame(), None 

        df = pd.DataFrame(etf_list)
        
        # 5. 기준일자 추출 및 포맷팅 (이하 동일)
        base_date_raw = etf_list[0].get('BAS_DD')
        if base_date_raw and len(base_date_raw) == 8:
            base_date = f"{base_date_raw[:4]}-{base_date_raw[4:6]}-{base_date_raw[6:]}"
        else:
            base_date = "알 수 없음"

        # 6. 컬럼 이름 매핑 및 데이터 타입 변환
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
        page_title="국내 ETF 일별 등락률 순위",
        layout="wide",
        initial_sidebar_state="collapsed"
    )
    
    st.title("📈 국내 ETF 일별 등락률 순위")
    st.markdown("데이터는 선택하신 기준일자의 일별 매매 정보입니다.")
    
    # 1. 날짜 선택 위젯
    today = datetime.now().date()
    # 기본 날짜를 가장 최근의 평일로 설정 (API 호출의 성공 확률 높이기)
    default_date = today - timedelta(days=1)
    if default_date.weekday() == calendar.SUNDAY:
        default_date -= timedelta(days=2)
    elif default_date.weekday() == calendar.SATURDAY:
        default_date -= timedelta(days=1)
        
    selected_date = st.date_input(
        "📅 조회 기준 날짜를 선택해주세요. (최근 거래일 기준)", 
        value=default_date,
        max_value=today
    )

    # 2. 날짜를 API 형식(YYYYMMDD)으로 변환
    target_basDd = selected_date.strftime('%Y%m%d')
    
    st.subheader(f"조회 기준일: {selected_date.strftime('%Y년 %m월 %d일')}")
    st.text(f"데이터 조회 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # 3. 데이터 로딩 및 표시
    # ⚠️ 변경된 fetch_etf_data 함수 호출
    etf_df, base_date = fetch_etf_data(API_URL, AUTH_KEY, target_basDd)
    
    if not etf_df.empty:
        
        sorted_df = etf_df.sort_values(by='등락률 (%)', ascending=False).reset_index(drop=True)
        
        sorted_df.index = sorted_df.index + 1
        sorted_df = sorted_df.reset_index().rename(columns={'index': '순위'})
        
        def color_rate(val):
            color = 'red' if val > 0 else ('blue' if val < 0 else 'gray')
            return f'color: {color}; font-weight: bold;'
        
        st.dataframe(
            sorted_df.style.applymap(
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
        st.warning(
            f"데이터 로드에 실패했거나 {selected_date.strftime('%Y-%m-%d')}에 조회된 데이터가 없습니다. "
            "현재 코드는 성공적으로 작동한 API의 인증 방식을 적용했습니다. 계속 오류가 발생하면, 선택한 날짜가 휴장일이거나 API 엔드포인트를 다시 확인해야 합니다."
        )

if __name__ == "__main__":
    main()
