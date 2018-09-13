# -*- coding:utf-8 -*-

import json

from flask import Flask, render_template
from models import Chart
from query_mysql import Mysql_Query
from query_presto import Presto_Query

app = Flask(__name__)


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/presto")
def presto():
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


@app.route("/mysql")
def mysql():
    query = Mysql_Query()
    # 分析借款金额、年龄、性别的数据分布(平均借款)
    xdata1, series1 = query.age_sex_loan_query()
    multiLineChart_avg = Chart().multiLine("分析借款金额、年龄、性别的数据分布(平均借款)", xdata1, "年龄", series1)
    # 分析借款金额、年龄、性别的数据分布(最大借款)
    xdata2, series2 = query.age_sex_loan_query(value_type="max")
    multiLineChart_max = Chart().multiLine("分析借款金额、年龄、性别的数据分布(最大借款)", xdata2, "年龄", series2)
    # 分析借款金额、年龄、性别的数据分布(最小借款)
    xdata3, series3 = query.age_sex_loan_query(value_type="min")
    multiLineChart_min = Chart().multiLine("分析借款金额、年龄、性别的数据分布(最小借款)", xdata3, "年龄", series3)
    # 分别统计用户账号激活后3 天、7 天、一个月和三个月内会借款的用户的数量
    tuples = query.active_after_query()
    keys = query.get_keys(tuples)
    values = query.get_values(tuples)
    bar1 = Chart() \
        .x_axis(data=keys) \
        .y_axis(formatter="{value}") \
        .bar(u"人数", values, show_item_label=True)
    # 从不买打折产品且不借款的用户
    xdata4, series4 = query.no_discount_loan_query()
    multiBarChart_no = Chart().multiBar("从不买打折产品且不借款的用户", xdata4, "日期", series4)

    render = {
        "title": "京东金融信贷需求预分析",
        "templates": [
            {"type": "chart", "title": u"分析借款金额、年龄、性别的数据分布(平均借款)", "index": 1, "option": json.dumps(multiLineChart_avg, indent=2)},
            {"type": "chart", "title": u"分析借款金额、年龄、性别的数据分布(最大借款)", "index": 2, "option": json.dumps(multiLineChart_max, indent=2)},
            {"type": "chart", "title": u"分析借款金额、年龄、性别的数据分布(最小借款)", "index": 3, "option": json.dumps(multiLineChart_min, indent=2)},
            {"type": "chart", "title": u"分别统计用户账号激活后3 天、7 天、一个月和三个月内会借款的用户的数量", "index": 4, "option": json.dumps(bar1, indent=2)},
            {"type": "chart", "title": u"从不买打折产品且不借款的用户", "index": 5, "option": json.dumps(multiBarChart_no, indent=2)},
        ]
    }
    return render_template("main.html", **render)


@app.route("/stream")
def stream():
    return render_template("stream.html")


if __name__ == "__main__":
    app.run(debug=True)
