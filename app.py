# app.py
from flask import Flask, render_template, request, jsonify
from quant_models.black_scholes_merton import black_scholes_merton

app = Flask(__name__, template_folder='.')

@app.route('/')
def dashboard():
    # 대시보드 페이지를 렌더링하는 로직 (필요시)
    return render_template('dashboard.html')

@app.route('/option_calculator')
def option_calculator():
    # 옵션 계산기 페이지를 렌더링하는 로직
    return render_template('option_calculator.html')

@app.route('/api/calculate_option', methods=['POST'])
def calculate_option():
    data = request.json
    try:
        S = float(data.get('stock_price'))
        K = float(data.get('strike_price'))
        T = float(data.get('time_to_maturity'))
        r = float(data.get('risk_free_rate'))
        sigma = float(data.get('volatility'))
        option_type = data.get('option_type')

        results = black_scholes_merton(S, K, T, r, sigma, option_type=option_type)

        return jsonify({
            'success': True,
            'price': round(results['price'], 4),
            'delta': round(results['delta'], 4),
            'gamma': round(results['gamma'], 4),
            'vega': round(results['vega'], 4),
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 400

if __name__ == '__main__':
    # Flask 서버 실행
    app.run(debug=True)