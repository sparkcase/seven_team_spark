# -*- coding: utf-8 -*-

import json
from functools import reduce


class Chart(dict):
    """
    图表模板
    """
    def __init__(self):
        super(Chart, self).__init__()
        self["calculable"] = True
        self["tooltip"] = {"show": True}
        self["toolbox"] = {
            "show": True,
            "x": "left",
            "feature": {
                "dataView": {
                    "show": True,
                    "readOnly": False
                },
                "magicType": {
                    "show": True,
                    "type": ["line", "bar"]
                },
                "restore": {
                    "show": True
                },
                "saveAsImage": {
                    "show": True
                },
                "dataZoom": {
                    "show": True,
                    "title": {
                        "dataZoom": u"区域缩放",
                        "dataZoomReset": u"区域缩放后退"
                    }
                }
            }
        }
        self["legend"] = {
            "show": True,
            "data": []
        }
        self["series"] = []

    def title(self, x="center", **kwargs):
        """
        设置图表标题
        """
        self["title"].update({
            "x": x
        })
        self["title"].update(kwargs)
        return self

    def tooltip(self, show=True, trigger='axis', formatter=None, **kwargs):
        """
        设置提示信息
        """
        self["tooltip"].update({
            "show": show,
            "trigger": trigger
        })
        if formatter is not None:
            self["tooltip"].update({"formatter": formatter})
        self["tooltip"].update(kwargs)
        return self

    def legend(self, show=True, data=None, orient='horizontal', **kwargs):
        """
        设置图例
        `data`: [u"图例1", u"图例2", u"图例3"]
        `orient`: "vertical"|"horizontal"
        """
        data = [] if data is None else data
        self["legend"].update({
            "show": show,
            "data": data,
            "orient": orient
        })
        self["legend"].update(kwargs)
        return self

    def toolbox(self, show=True, x='left', **kwargs):
        """
        设置工具箱
        """
        self["toolbox"].update({
            "show": show,
            "x": x
        })
        self["toolbox"].update(kwargs)
        return self

    def x_axis(self, data=None, type_="category", name="", **kwargs):
        """
        添加X轴
        """
        data = [] if data is None else data
        if "xAxis" not in self:
            self["xAxis"] = []
        self["xAxis"].append(self.__merge_dict({
            "type": type_,
            "name": name,
            "data": data
        }, kwargs))
        return self

    def y_axis(self, data=None, type_="value", name="", formatter=None, **kwargs):
        """
        添加X轴
        """
        if "yAxis" not in self:
            self["yAxis"] = []
        self["yAxis"].append(self.__merge_dict({
            "type": type_,
            "name": name,
        }, {"axisLabel": {"formatter": formatter}} if formatter is not None else {}, kwargs))
        if data is not None:
            self["yAxis"] = data
        return self

    @staticmethod
    def __merge_dict(*args):
        """
        合并多个字典并返回
        """
        return reduce(lambda x, y: {**x, **y}, args)

    def multiLine(self, name, xdata, xname, series, auto_legend=True, **kwargs):
        """
        添加堆积折线图
        `data`:  (ylabel, series)
        """
        self.x_axis(xdata, name=xname)
        legendData = []
        for n, data in series:
            self["series"].append({
                "type": "line",
                "name": n,
                "data": data,
                "itemStyle": {
                    "normal": {
                        "label": {"show": True}
                    }
                },
                "yAxisIndex": 0
            })
            legendData.append(n)
        self.legend(data=legendData)
        if "yAxis" not in self:
            self.y_axis()
        if name not in self["legend"]["data"] and auto_legend:
            self["legend"]["data"].append(name)
        return self

    def multiBar(self, name, xdata, xname, series, auto_legend=True, **kwargs):
        """
        添加多系列彩虹柱形图
        `data`:  (ylabel, series)
        """
        self.x_axis(xdata, name=xname)
        legendData = []
        for n, data in series:
            self["series"].append({
                "type": "bar",
                "name": n,
                "data": data,
                "itemStyle": {
                    "normal": {
                        "label": {"show": True}
                    }
                }
            })
            legendData.append(n)
        self.legend(data=legendData)
        if "yAxis" not in self:
            self.y_axis()
        if name not in self["legend"]["data"] and auto_legend:
            self["legend"]["data"].append(name)
        return self

    def bar(self, name, data=None, auto_legend=True, y_axis_index=0, **kwargs):
        """
        添加一个柱状图
        `data`: [10, 20, 30, 40]
        `auto_legend`: 自动生成图例
        """
        data = [] if data is None else data
        self["series"].append(self.__merge_dict({
            "type": "bar",
            "name": name,
            "data": data,
            "yAxisIndex": y_axis_index
        }, kwargs))
        if "yAxis" not in self:
            self.y_axis()
        if name not in self["legend"]["data"] and auto_legend:
            self["legend"]["data"].append(name)
        return self


def main():
    c = Chart().tooltip()
    print(json.dumps(c))


if __name__ == "__main__":
    main()
