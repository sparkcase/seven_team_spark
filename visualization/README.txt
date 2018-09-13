1、运行可视化模块时，需要安装python3.5+
需添加的python依赖：
flask
pandas
pyhive
mysql-connector-python

2、presto的配置文件是位于visualization/charts目录下的prestoConfig.py

3、运行时可以进入项目的visualization目录，执行命令：python charts/main.py

4、可视化相关mysql数据库表：
CREATE TABLE sparksql_analysis_data(
age INT,
sex VARCHAR(50),
loan_start VARCHAR(50),
count_value BIGINT,
avg_value DECIMAL(12, 10),
max_value DECIMAL(12, 10),
min_value DECIMAL(12, 10),
type VARCHAR(10)
)
