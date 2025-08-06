document.addEventListener('DOMContentLoaded', () => {
    const calculateBtn = document.querySelector('.calculate-btn');

    calculateBtn.addEventListener('click', async () => {
        const stockPrice = document.getElementById('stock-price').value;
        const strikePrice = document.getElementById('strike-price').value;
        const timeToMaturity = document.getElementById('time-to-maturity').value;
        const riskFreeRate = document.getElementById('risk-free-rate').value;
        const volatility = document.getElementById('volatility').value;

        // 콜/풋 옵션 선택 기능 추가 (선택 필드가 있다고 가정)
        // 예: <select id="option-type">에 "call"과 "put"이 있다고 가정
        const optionType = 'call'; // 임시로 'call'로 고정

        try {
            const response = await fetch('/api/calculate_option', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    stock_price: stockPrice,
                    strike_price: strikePrice,
                    time_to_maturity: timeToMaturity,
                    risk_free_rate: riskFreeRate,
                    volatility: volatility,
                    option_type: optionType
                }),
            });

            if (!response.ok) {
                const errorData = await response.json();
                throw new Error(errorData.error || 'API 요청 실패');
            }

            const data = await response.json();

            if (data.success) {
                document.getElementById('option-price').textContent = data.price;
                document.getElementById('delta').textContent = data.delta;
                document.getElementById('gamma').textContent = data.gamma;
                document.getElementById('vega').textContent = data.vega;
            } else {
                throw new Error(data.error || '계산 오류 발생');
            }
        } catch (error) {
            console.error('Fetch error:', error);
            alert('오류: ' + error.message);
        }
    });
});