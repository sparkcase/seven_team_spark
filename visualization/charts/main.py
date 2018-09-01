# -*- coding:utf-8 -*-

import json

from flask import Flask, render_template
from models import Chart
from query_presto import Presto_Query

app = Flask(__name__)


@app.route("/")
def index():
    query = Presto_Query()
    # 各年龄段消费者每日购买商品总价值
    xdata, series = query.total_price_perday()
    multiLineChart = Chart().multiLine("年龄日期统计", xdata, "日期", series)
    # 男女消费者每日、每周和每月总借贷金额
    xdata_day, series_day = query.gender_loan_query()
    multiBarChart_day = Chart().multiBar("性别借款统计(每日)", xdata_day, "日期", series_day)
    xdata_week, series_week = query.gender_loan_query("week")
    multiBarChart_week = Chart().multiBar("性别借款统计(每周)", xdata_week, "日期", series_week)
    xdata_month, series_month = query.gender_loan_query("month")
    multiBarChart_month = Chart().multiBar("性别借款统计(每月)", xdata_month, "日期", series_month)

    render = {
        "title": "京东金融信贷需求预分析",
        "templates": [
            {"type": "chart", "title": u"年龄日期统计", "index": 1, "option": json.dumps(multiLineChart, indent=2)},
            {"type": "chart", "title": u"性别借款统计(每日)", "index": 2, "option": json.dumps(multiBarChart_day, indent=2)},
            {"type": "chart", "title": u"性别借款统计(每周)", "index": 3, "option": json.dumps(multiBarChart_week, indent=2)},
            {"type": "chart", "title": u"性别借款统计(每月)", "index": 4, "option": json.dumps(multiBarChart_month, indent=2)}
        ]
    }
    return render_template("main.html", **render)


@app.route("/stream")
def stream():
    return render_template("stream.html")


if __name__ == "__main__":
    app.run(debug=True)