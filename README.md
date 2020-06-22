# Json-to-Parquet

Parquet是一种HDFS列式文件格式，实现了对Google Protocol Buffers的支持。PB是树状结构数据，与json在本质上是相同的，但是Parquet要求定义Schema才能够使用。该项目在parquet的基础上实现了对json列式的支持，分为两步：
1. 根据json数据自动产生schema（支持嵌套、多值、缺值情况）
2. 利用schema，将json数据存储为parquet文件
