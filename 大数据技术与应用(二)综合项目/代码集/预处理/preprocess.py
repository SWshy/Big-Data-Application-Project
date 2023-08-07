import pandas as pd

# 读取CSV文件并转换为DataFrame对象
df = pd.read_csv('E:\大三下学期作业\大数据\大作业\movie.csv', header=0)
# 显示前10行
print(df.head(10))

# 预处理

# 删除所有列的空格
df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
# 删除重复行
df.drop_duplicates(subset=['电影名称'], inplace=True)

# 删除不必要的符号[],''
df['类型'] = df['类型'].apply(lambda x: x.replace("[", '').replace("]", '').replace("'", "").replace(",", "/").replace(" ", ""))
df['导演'] = df['导演'].apply(lambda x: x.replace("[", '').replace("]", '').replace("'", ""))
# 删除无用的列
df = df.drop(['又名', '主演', '编剧'], axis=1)

# 处理缺失值
df.dropna(inplace=True)  # 删除包含缺失值的行
# 将"[]"替换为上一个非缺失值
df['上映日期'] = df['上映日期'].replace('[]', method='ffill')
df['上映日期'] = df['上映日期'].apply(lambda x: x.replace("[", '').replace("]", '').replace("'", ""))


# 对四列中的每个单元格进行处理
df['上映日期'] = df['上映日期'].str.split('(').str[0].str.strip()
df['上映日期'] = df['上映日期'].str.split(' ').str[0].str.strip()
df['导演'] = df['导演'].str.split(',').str[0].str.strip()
df['语言'] = df['语言'].str.split('/').str[0].str.strip()
df['制片国家/地区'] = df['制片国家/地区'].str.split('/').str[0].str.strip()

# 数据转换
df['片长'] = df['片长'].str.extract(r'(\d+)').astype(int)  # 从片长列中提取数字并转换为整型
df['星级'] = df['星级'].astype(float) / 10  # 转换为float类型，然后将其除以10
df['五星'] = df['五星'].astype(float) / 100  # 转换为float类型，然后将其除以100
df['四星'] = df['四星'].astype(float) / 100  # 转换为float类型，然后将其除以100
df['三星'] = df['三星'].astype(float) / 100  # 转换为float类型，然后将其除以100
df['二星'] = df['二星'].astype(float) / 100  # 转换为float类型，然后将其除以100
df['一星'] = df['一星'].astype(float) / 100  # 转换为float类型，然后将其除以100

# 将每一列中的逗号替换为一个空格
df = df.replace(',', ' ', regex=True)

print(df)

# 增加ID字段
df.insert(0, 'ID', range(1, len(df) + 1))
# 将结果写入CSV文件
df.to_csv('movies.csv', index=False, encoding='utf-8-sig')
df.to_csv('movie_info.csv', index=False, encoding='utf-8')
