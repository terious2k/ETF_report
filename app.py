import streamlit as st
import pandas as pd
import requests
from datetime import datetime, timedelta
import calendar 
import pytz # 👈 pytz 라이브러리 임포트 추가

# --- KRX API 정보 설정 ---
ETF_DAILY_API_URL = 'https://data-dbg.krx.co.kr/svc/apis/etp/etf_bydd_trd' 
ETF_COMP_API_URL = 'https://data-dbg.krx.co.kr/svc/apis/etp/etf_comp_list' 

# ⚠️ 한국 시간대(KST) 정의
KST = pytz.timezone('Asia/Seoul')

try:
    AUTH_KEY = st.secrets["krx_api"]["auth_key"]
except (KeyError, AttributeError):
    st.error("⚠️ Streamlit Secrets 설정이 필요합니다. 'krx_api' 섹션에 'auth_key'를 확인하세요.")
    AUTH_KEY = '16B23371BBDC4107AB07CBBBBA14ADBCD2525DF0' 
    st.info("현재는 코드에 직접 입력된 테스트 키로 실행됩니다. 보안을 위해 Secrets를 사용해주세요.")


# --- 1. ETF 일별 매매 정보 (목록) 가져오기 함수 ---
# (이 함수는 시간 로직이 없으므로 그대로 유지됩니다.)
@st.cache_data(ttl=3600)
def fetch_etf_daily_data(api_url, auth_key, target_basDd):
    """KRX API에 GET 요청을 보내 ETF 일별 매매 데이터를 가져옵니다."""
    
    headers = {
        'Content-Type': 'application/json',
        'AUTH_KEY': auth_key,
    }
    
    params = {
        'basDd': target_basDd, 
        'etc_parm': 'Y',
    }
    
    try:
        response = requests.get(api_url, headers=headers, params=params, timeout=15)
        response.raise_for_status() 
        data = response.json()
        
        etf_list = data.get('OutBlock_1', data.get('outBlock1', [])) 
        
        if not etf_list:
            error_msg = data.get('error_message', 'API 응답에서 유효한 데이터("OutBlock_1")를 찾을 수 없습니다.')
            st.warning(f"데이터 추출 실패: {error_msg}")
            return pd.DataFrame(), None 

        df = pd.DataFrame(etf_list)
        
        base_date_raw = etf_list[0].get('BAS_DD')
        base_date = f"{base_date_raw[:4]}-{base_date_raw[4:6]}-{base_date_raw[6:]}" if base_date_raw and len(base_date_raw) == 8 else "알 수 없음"

        df = df.rename(columns={
            'ISU_NM': '종목명',         
            'TDD_CLSPRC': '현재가',     
            'FLUC_RT': '등락률 (%)',    
            'ACC_TRDVOL': '거래량',
            'ISU_CD': '종목코드'       
        })
        
        df['현재가'] = pd.to_numeric(df['현재가'], errors='coerce').fillna(0).astype(int)
        df['등락률 (%)'] = pd.to_numeric(df['등락률 (%)'], errors='coerce').fillna(0).round(2)
        df['거래량'] = pd.to_numeric(df['거래량'], errors='coerce').fillna(0).astype(int)
        
        return df[['종목명', '종목코드', '현재가', '등락률 (%)', '거래량']], base_date

    except requests.exceptions.RequestException as e:
        st.error(f"ETF 일별 데이터 로드 실패: {e}")
        return pd.DataFrame(), None 


# --- 2. ETF 구성 종목 상세 정보 가져오기 함수 ---
# (이 함수는 시간 로직이 없으므로 그대로 유지됩니다.)
@st.cache_data(ttl=3600)
def fetch_etf_composition(api_url, auth_key, target_basDd, isuCd):
    """선택된 ETF의 구성 종목 상세 정보를 가져옵니다."""
    
    headers = {
        'Content-Type': 'application/json',
        'AUTH_KEY': auth_key,
    }
    
    params = {
        'basDd': target_basDd, 
        'isuCd': isuCd, 
        'etc_parm': 'Y',
    }
    
    try:
        response = requests.get(api_url, headers=headers, params=params, timeout=15)
        response.raise_for_status() 
        data = response.json()
        
        comp_list = data.get('OutBlock_1', data.get('outBlock1', [])) 
        
        if not comp_list:
            st.warning(f"'{isuCd}'의 구성 종목 데이터를 찾을 수 없습니다. (휴장일이거나 구성 정보 미제공)")
            return pd.DataFrame() 

        df = pd.DataFrame(comp_list)
        
        df = df.rename(columns={
            'ISU_NM': '구성종목명',         
            'ISU_CD': '구성종목코드',     
            'CMP_SHR_RT': '편입비중 (%)', 
            'MKT_TP_NM': '시장구분',      
        })
        
        df['편입비중 (%)'] = pd.to_numeric(df['편입비중 (%)'], errors='coerce').fillna(0).round(2)
        
        return df[['구성종목명', '구성종목코드', '편입비중 (%)', '시장구분']]

    except requests.exceptions.RequestException as e:
        st.error(f"ETF 구성 종목 데이터 로드 실패: {e}")
        return pd.DataFrame() 


# --- Streamlit 앱 메인 로직 ---
def main():
    st.set_page_config(
        page_title="국내 ETF 일별 등락률 및 구성종목",
        layout="wide",
        initial_sidebar_state="collapsed"
    )
    
    st.title("📈 국내 ETF 일별 등락률 및 구성종목 조회")
    
    # 1. 날짜 선택 위젯 (KST 기준으로 날짜 계산)
    now_kst = datetime.now(KST) # 👈 KST 현재 시간
    today = now_kst.date()
    
    # KST 기준 최근 영업일 계산
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

    # 날짜를 API 형식(YYYYMMDD)으로 변환
    target_basDd = selected_date.strftime('%Y%m%d')
    
    st.subheader(f"조회 기준일: {selected_date.strftime('%Y년 %m월 %d일')}")
    # ⚠️ KST로 조회 시각 표시
    st.text(f"데이터 조회 시각: {now_kst.strftime('%Y-%m-%d %H:%M:%S')} (KST)")

    # 2. ETF 목록 데이터 로딩
    etf_df, base_date = fetch_etf_daily_data(ETF_DAILY_API_URL, AUTH_KEY, target_basDd)
    
    if not etf_df.empty:
        
        sorted_df = etf_df.sort_values(by='등락률 (%)', ascending=False).reset_index(drop=True)
        
        sorted_df['순위'] = sorted_df.index + 1
        display_df = sorted_df[['순위', '종목명', '현재가', '등락률 (%)', '거래량', '종목코드']]
        
        # 3. ETF 목록 표시 및 클릭 이벤트 처리
        st.markdown("### 1. ETF 목록 (클릭하여 구성종목 조회)")
        
        def color_rate(val):
            color = 'red' if val > 0 else ('blue' if val < 0 else 'gray')
            return f'color: {color}; font-weight: bold;'
        
        styled_df = display_df.style.applymap(
            color_rate, 
            subset=['등락률 (%)']
        ).format({
            '현재가': '{:,.0f}', 
            '거래량': '{:,.0f}'
        })

        col_config = {"종목코드": st.column_config.Column(disabled=True, hide_label=True)}
        
        selected_rows = st.dataframe(
            styled_df,
            use_container_width=True,
            hide_index=True,
            column_config=col_config,
            selection_mode="single-row",
            key="etf_selection_table"
        )
        
        # 4. 클릭된 ETF의 구성 종목 조회 및 표시
        if selected_rows and selected_rows["selection"]["rows"]:
            selected_index = selected_rows["selection"]["rows"][0]
            selected_etf = display_df.iloc[selected_index]
            selected_isu_cd = selected_etf['종목코드']
            selected_isu_nm = selected_etf['종목명']
            
            st.markdown("---")
            st.markdown(f"### 2. '{selected_isu_nm}' ({selected_isu_cd}) 구성 종목 상세")
            
            with st.spinner("구성 종목 정보를 불러오는 중..."):
                comp_df = fetch_etf_composition(ETF_COMP_API_URL, AUTH_KEY, target_basDd, selected_isu_cd)
            
            if not comp_df.empty:
                comp_df = comp_df.sort_values(by='편입비중 (%)', ascending=False).reset_index(drop=True)
                st.dataframe(comp_df, use_container_width=True, hide_index=True)
            else:
                st.info("선택하신 ETF의 구성 종목 상세 정보를 가져올 수 없습니다. API 또는 날짜를 확인해 주세요.")

    else:
        st.warning(
            f"ETF 목록 데이터 로드에 실패했거나 {selected_date.strftime('%Y-%m-%d')}에 조회된 데이터가 없습니다. "
            "선택한 날짜가 휴장일이거나 API 엔드포인트를 확인해 주세요."
        )

if __name__ == "__main__":
    main()
