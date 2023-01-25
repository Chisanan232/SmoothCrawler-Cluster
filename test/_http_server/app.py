"""
A simple HTTP server for testing. It would return a JSON type data.
"""

import time

from flask import Flask, request

app: Flask = Flask(__name__)


@app.route("/exchangeReport/STOCK_DAY", methods=["GET"])
def get_stock_data() -> str:
    """
    API: /exchangeReport/STOCK_DAY
    API Parameters:
        * response: The data format. Default is JSON type.
        * date: The date of data.
        * stockNo: The stock symbol no.

    Example:
    http://10.20.23.3:12345/exchangeReport/STOCK_DAY?response=json&date=20170101&stockNo=2330

    :return: A string type data with JSON type format.
    """

    # _response = request.args.get("response", "json")
    _date = request.args.get("date", None)  # Example: 20170101
    _stockNo = request.args.get("stockNo", None)  # Example: 2330

    _data = (
        "{"
        '"stat":"OK",'
        f'"date":"{_date}",'
        f'"title":"111年06月 {_stockNo} 台積電           各日成交資訊",'
        '"fields":["日期","成交股數","成交金額","開盤價","最高價","最低價","收盤價","漲跌價差","成交筆數"],'
        '"data":['
        '["111/06/01","32,970,903","18,171,598,472","550.00","555.00","548.00","549.00","-11.00","33,456"],'
        '["111/06/02","26,063,495","14,122,936,388","544.00","545.00","540.00","540.00","-9.00","30,042"],'
        '["111/06/06","23,732,327","12,843,324,209","541.00","544.00","538.00","540.00"," 0.00","16,614"],'
        '["111/06/07","22,152,512","11,846,386,906","535.00","538.00","532.00","535.00","-5.00","28,586"],'
        '["111/06/08","19,609,522","10,636,701,303","539.00","545.00","538.00","544.00","+9.00","18,487"],'
        '["111/06/09","16,894,479","9,115,934,006","538.00","542.00","537.00","541.00","-3.00","18,802"],'
        '["111/06/10","22,614,596","12,011,615,014","530.00","533.00","529.00","530.00","-11.00","44,802"],'
        '["111/06/13","36,758,925","18,998,155,460","518.00","519.00","515.00","516.00","-14.00","112,023"],'
        '["111/06/14","38,838,778","19,813,036,892","507.00","514.00","507.00","513.00","-3.00","85,483"],'
        '["111/06/15","38,360,508","19,580,150,319","508.00","515.00","508.00","509.00","-4.00","72,687"],'
        '["111/06/16","31,908,028","16,331,470,764","515.00","516.00","507.00","508.00","X0.00","42,177"],'
        '["111/06/17","48,400,798","24,260,277,915","499.50","503.00","499.00","501.00","-7.00","119,618"],'
        '["111/06/20","36,664,463","18,267,359,790","500.00","502.00","495.00","498.00","-3.00","89,541"],'
        '["111/06/21","34,432,537","17,298,234,720","501.00","505.00","499.00","505.00","+7.00","32,427"],'
        '["111/06/22","33,438,921","16,630,857,096","501.00","503.00","494.50","494.50","-10.50","81,024"],'
        '["111/06/23","46,808,462","22,836,692,325","492.00","493.50","485.00","485.50","-9.00","104,661"],'
        '["111/06/24","29,003,676","14,184,287,155","489.50","492.50","485.50","486.50","+1.00","43,609"],'
        '["111/06/27","38,684,368","19,379,396,938","496.00","506.00","495.50","498.50","+12.00","37,438"],'
        '["111/06/28","16,867,955","8,392,290,378","496.00","500.00","496.00","497.50","-1.00","18,988"],'
        '["111/06/29","33,124,986","16,352,376,816","496.00","498.50","491.00","491.00","-6.50","40,024"],'
        '["111/06/30","49,820,824","23,900,613,642","484.50","486.50","476.00","476.00","-15.00","111,117"]'
        "],"
        '"notes":["符號說明:+/-/X表示漲/跌/不比價","當日統計資訊含一般、零股、盤後定價、鉅額交易，不含拍賣、標購。","ETF證券代號第六碼為K、M、S、C者，表示該ETF以外幣交易。"]'
        "}"
    )
    return _data


@app.route("/example.com", methods=["GET"])
def example_web() -> str:
    _sleep_time = request.args.get("sleep", None)
    if _sleep_time is not None:
        time.sleep(int(_sleep_time))
    return (
        "<!doctype html>\n"
        "<html>\n"
        "<head>\n"
        "    <title>Example Domain</title>\n\n"
        '    <meta charset="utf-8" />\n'
        '    <meta http-equiv="Content-type" content="text/html; charset=utf-8" />\n'
        '    <meta name="viewport" content="width=device-width, initial-scale=1" />\n'
        '    <style type="text/css">\n'
        "    body {\n"
        "        background-color: #f0f0f2;\n"
        "        margin: 0;\n"
        "        padding: 0;\n"
        '        font-family: -apple-system, system-ui, BlinkMacSystemFont, "Segoe UI", "Open Sans", "Helvetica Neue", Helvetica, Arial, sans-serif;\n'
        "        \n"
        "    }\n"
        "    div {\n"
        "        width: 600px;\n"
        "        margin: 5em auto;\n"
        "        padding: 2em;\n"
        "        background-color: #fdfdff;\n"
        "        border-radius: 0.5em;\n"
        "        box-shadow: 2px 3px 7px 2px rgba(0,0,0,0.02);\n"
        "    }\n"
        "    a:link, a:visited {\n"
        "        color: #38488f;\n"
        "        text-decoration: none;\n"
        "    }\n"
        "    @media (max-width: 700px) {\n"
        "        div {\n"
        "            margin: 0 auto;\n"
        "            width: auto;\n"
        "        }\n"
        "    }\n"
        "    </style>    \n"
        "</head>\n\n"
        "<body>\n"
        "<div>\n"
        "    <h1>Example Domain</h1>\n"
        "    <p>This domain is for use in illustrative examples in documents. You may use this\n"
        "    domain in literature without prior coordination or asking for permission.</p>\n"
        '    <p><a href="https://www.iana.org/domains/example">More information...</a></p>\n'
        "</div>\n"
        "</body>\n"
        "</html>\n"
    )
