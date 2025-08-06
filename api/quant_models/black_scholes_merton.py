import numpy as np
from scipy.stats import norm
from typing import Dict, Any

def black_scholes_merton_enhanced(S: float, K: float, T: float, r: float, sigma: float, q: float, option_type: str = 'call') -> Dict[str, Any]:
    """
    실제 시장 데이터를 고려하여 블랙-숄즈-머튼 모델을 개선한 옵션 가격 계산 함수.

    Args:
        S (float): 현재 주식 가격.
        K (float): 옵션의 행사가격.
        T (float): 만기까지의 시간 (연 단위).
        r (float): 무위험 이자율 (연 단위).
        sigma (float): 주식의 변동성 (연 단위).
        q (float): 배당 수익률 (연 단위).
        option_type (str): 'call' 또는 'put' 옵션.

    Returns:
        Dict[str, Any]: 옵션 가격과 델타, 감마 등 주요 그리스 문자를 포함하는 딕셔너리.
    """
    if not all(isinstance(arg, (int, float)) and arg >= 0 for arg in [S, K, T, sigma, r, q]):
        raise ValueError("모든 입력값(S, K, T, sigma, r, q)은 0 이상의 숫자여야 합니다.")
        
    if T <= 0:
        intrinsic_value = max(0, S - K) if option_type == 'call' else max(0, K - S)
        return {
            "price": intrinsic_value,
            "delta": 1.0 if option_type == 'call' and S > K else (0.0 if option_type == 'call' and S < K else -1.0 if option_type == 'put' and S < K else 0.0),
            "gamma": 0.0,
            "vega": 0.0,
            "theta": 0.0,
            "rho": 0.0,
            "d1": np.nan,
            "d2": np.nan,
        }

    # d1, d2 계산
    sigma_T_sqrt = sigma * np.sqrt(T)
    d1 = (np.log(S / K) + (r - q + 0.5 * sigma**2) * T) / sigma_T_sqrt
    d2 = d1 - sigma_T_sqrt
    
    # 누적 분포 함수 (CDF)
    nd1 = norm.cdf(d1)
    nd2 = norm.cdf(d2)
    n_minus_d1 = norm.cdf(-d1)
    n_minus_d2 = norm.cdf(-d2)
    
    # 옵션 가격
    if option_type == 'call':
        price = S * np.exp(-q * T) * nd1 - K * np.exp(-r * T) * nd2
        delta = np.exp(-q * T) * nd1
        gamma = np.exp(-q * T) * norm.pdf(d1) / (S * sigma_T_sqrt)
        vega = S * np.exp(-q * T) * norm.pdf(d1) * np.sqrt(T)
        theta = -S * np.exp(-q * T) * norm.pdf(d1) * sigma / (2 * np.sqrt(T)) - r * K * np.exp(-r * T) * nd2 + q * S * np.exp(-q * T) * nd1
        rho = K * T * np.exp(-r * T) * nd2
        
    elif option_type == 'put':
        price = K * np.exp(-r * T) * n_minus_d2 - S * np.exp(-q * T) * n_minus_d1
        delta = np.exp(-q * T) * (nd1 - 1)
        gamma = np.exp(-q * T) * norm.pdf(d1) / (S * sigma_T_sqrt)
        vega = S * np.exp(-q * T) * norm.pdf(d1) * np.sqrt(T)
        theta = -S * np.exp(-q * T) * norm.pdf(d1) * sigma / (2 * np.sqrt(T)) + r * K * np.exp(-r * T) * n_minus_d2 - q * S * np.exp(-q * T) * n_minus_d1
        rho = -K * T * np.exp(-r * T) * n_minus_d2
        
    else:
        raise ValueError("유효하지 않은 옵션 유형입니다. 'call' 또는 'put'이어야 합니다.")
        
    return {
        "price": price,
        "delta": delta,
        "gamma": gamma,
        "vega": vega,
        "theta": theta,
        "rho": rho,
    }

# 사용 예시
if __name__ == '__main__':
    S, K, T, r, sigma, q = 100.0, 100.0, 1.0, 0.05, 0.2, 0.02

    # 콜 옵션 가격 및 그리스 문자 계산
    call_results = black_scholes_merton_enhanced(S, K, T, r, sigma, q, option_type='call')
    print("유럽형 콜 옵션 결과:")
    for key, value in call_results.items():
        print(f"  {key:<10}: {value:.4f}")

    print("-" * 20)
    
    # 풋 옵션 가격 및 그리스 문자 계산
    put_results = black_scholes_merton_enhanced(S, K, T, r, sigma, q, option_type='put')
    print("유럽형 풋 옵션 결과:")
    for key, value in put_results.items():
        print(f"  {key:<10}: {value:.4f}")