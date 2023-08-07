import re
import matplotlib.pyplot as plt
import csv
import matplotlib.font_manager as font_manager
from wordcloud import WordCloud

# 设置全局字体
font_path = font_manager.findfont(font_manager.FontProperties(family='SimHei'))
plt.rcParams['font.family'] = font_manager.FontProperties(fname=font_path).get_name()

# 设置中文字体路径
font_path = 'C:WINDOWS\FONTS\SIMKAI.TTF'

# 统计电影评分前5的国家
countries = []
ratings = []

with open('1.csv', 'r', encoding='utf-8-sig') as csvfile:
    reader = csv.reader(csvfile)
    next(reader)  # 跳过第一行表头
    for row in reader:
        countries.append(row[0])
        ratings.append(float(row[1]))

top_countries = countries[:5]
top_ratings = ratings[:5]

# 创建柱状图
plt.figure(figsize=(8, 6))
plt.bar(top_countries, top_ratings)
plt.xlabel('国家', fontdict={'fontsize': 12, 'fontweight': 'bold'})
plt.ylabel('评分', fontdict={'fontsize': 12, 'fontweight': 'bold'})
plt.title('电影评分前5的国家', fontdict={'fontsize': 14, 'fontweight': 'bold'})
# 在柱状图顶部显示数值
for i in range(len(top_ratings)):
    plt.text(i, ratings[i], '{:.2f}'.format(ratings[i]), ha='center', va='bottom')
plt.show()


# 统计总片长前5的导演
directors = []
runtimes = []

with open('2.csv', 'r', encoding='utf-8-sig') as csvfile:
    reader = csv.reader(csvfile)
    next(reader)  # 跳过第一行表头
    for row in reader:
        directors.append(row[0])
        runtimes.append(float(row[1]))


# 获取前5个导演和对应的总片长
top_directors = directors[:5]
top_runtimes = runtimes[:5]

# 创建柱状图
plt.figure(figsize=(10, 6))
plt.bar(top_directors, top_runtimes)

# 设置 x 轴和 y 轴标题
plt.xlabel('导演', fontdict={'fontsize': 12, 'fontweight': 'bold'})
plt.ylabel('总片长', fontdict={'fontsize': 12, 'fontweight': 'bold'})

# 设置图表标题
plt.title('总片长前5的导演', fontdict={'fontsize': 14, 'fontweight': 'bold'})

# 在柱状图顶部显示数值
for i in range(len(top_runtimes)):
    plt.text(i, runtimes[i], str(runtimes[i]), ha='center', va='bottom')

plt.show()


# 统计每个语种的平均电影评分并按平均评分降序排列
languages = []
ratings = []

with open('3.csv', 'r', encoding='utf-8-sig') as csvfile:
    reader = csv.reader(csvfile)
    next(reader)  # 跳过第一行表头
    for row in reader:
        languages.append(row[0])
        ratings.append(float(row[1]))

# 组合语种为字符串
text = ' '.join(languages)
# 创建词云对象
wordcloud = WordCloud(
    width=800,
    height=400,
    background_color='white',
    colormap='cool',
    font_path=font_path,  # 设置字体文件的路径
    contour_width=2,  # 设置轮廓线宽度
    contour_color='black',  # 设置轮廓线颜色
    random_state=42  # 设置随机种子，保证每次生成的词云图一致
)
# 生成词云图的词语权重
wordcloud.generate_from_frequencies(dict(zip(languages, ratings)))
# 绘制词云图
plt.figure(figsize=(10, 6), dpi=220)
plt.imshow(wordcloud, interpolation='bilinear')
plt.axis('off')
# 设置图表标题
plt.title('每个语种的电影评分平均值', fontdict={'fontsize': 14, 'fontweight': 'bold'})
# 显示词云图
plt.show()


# 统计获得五星比例前10的电影
movies = []
five_star_ratios = []

with open('4.csv', 'r', encoding='utf-8-sig') as csvfile:
    reader = csv.reader(csvfile)
    next(reader)  # 跳过第一行表头
    for row in reader:
        movies.append(row[1])
        five_star_ratios.append(float(row[0]))

# 获取前10部电影和对应的五星比例
top_movies = movies[:10]
top_ratios = five_star_ratios[:10]

# 水平条形图
plt.figure(figsize=(10, 6))
y_pos = range(len(movies))
plt.barh(top_movies, top_ratios)
plt.yticks(y_pos, [re.sub(r'(?<=[\u4e00-\u9fa5])\s(?=[a-zA-Z])', '\n', name) for name in movies])  # 在中文和英文之间的空格处插入换行符
plt.xlabel('获得五星比例', fontsize=12, fontweight='bold')
plt.ylabel('电影名称', fontsize=12, fontweight='bold')
plt.title('获得五星比例前10的电影', fontsize=14, fontweight='bold')
plt.tight_layout()
# 在条形图顶部显示数值
for i, ratio in enumerate(top_ratios):
    plt.text(ratio, i, f'{ratio:.4f}', ha='left', va='center')
plt.show()


# 统计每月份的电影数量
labels = []
values = []

with open('5.csv', 'r', encoding='utf-8-sig') as csvfile:
    reader = csv.reader(csvfile)
    next(reader)  # 跳过第一行列标题
    for row in reader:
        labels.append(row[0] + '月')
        values.append(int(row[1]))

# 创建饼图
fig, ax = plt.subplots(figsize=(8, 6))
wedges, _, autotexts = ax.pie(values, labels=labels, autopct='%.2f%%', startangle=90)
# 设置图表标题
ax.set_title('每月电影数量占比')
# 自定义图例处理方法
legend_labels = [f'{label} ({value})' for label, value in zip(labels, values)]
ax.legend(wedges, legend_labels, title='月份', loc='center left', bbox_to_anchor=(1, 0, 0.5, 1))
# 调整布局
plt.tight_layout()
# 显示图表
plt.show()


# 统计想看人数加看过人数前10的电影类型组合
genres = []
audience_counts = []

with open('6.csv', 'r', encoding='utf-8-sig') as csvfile:
    reader = csv.reader(csvfile)
    next(reader)  # 跳过第一行表头
    for row in reader:
        genres.append(row[1].replace('/', '\n'))
        audience_counts.append(int(row[0]))

top_genre = genres[:10]
top_counts = audience_counts[:10]

# 创建柱状图
plt.figure(figsize=(12, 6))  # 调整图表尺寸适应长类型组合
plt.bar(top_genre, top_counts)

# 设置 x 轴和 y 轴标题
plt.xlabel('电影类型', fontdict={'fontsize': 12, 'fontweight': 'bold'})
plt.ylabel('人数', fontdict={'fontsize': 12, 'fontweight': 'bold'})

# 设置图表标题
plt.title('想看人数加看过人数前10的电影类型组合', fontdict={'fontsize': 14, 'fontweight': 'bold'})
# 在条形图顶部显示数值
for i, ratio in enumerate(top_counts):
    plt.text(i, audience_counts[i], str(audience_counts[i]), ha='center', va='bottom')

plt.show()


# 统计每年电影的数量并按年份降序排列
years = []
movie_counts = []

with open('7.csv', 'r', encoding='utf-8-sig') as csvfile:
    reader = csv.reader(csvfile)
    next(reader)  # 跳过第一行表头
    for row in reader:
        years.append(row[0])
        movie_counts.append(int(row[1]))

# 反转年份和电影数量列表
years = list(reversed(years))
movie_counts = list(reversed(movie_counts))

# 创建折线图
plt.figure(figsize=(10, 6))
plt.plot(years, movie_counts, linestyle='-', marker='.', color='blue')
# 设置 x 轴和 y 轴标题
plt.xlabel('年份', fontdict={'fontsize': 12, 'fontweight': 'bold'})
plt.ylabel('电影数量', fontdict={'fontsize': 12, 'fontweight': 'bold'})
# 设置图表标题
plt.title('每年电影的数量', fontdict={'fontsize': 14, 'fontweight': 'bold'})
# 模仿股票走势图的样式
plt.grid(True, linestyle='--', linewidth=0.5, alpha=0.5)
plt.fill_between(years, movie_counts, alpha=0.2)
plt.xticks(rotation=90)
# 找到坡峰的位置
max_index = movie_counts.index(max(movie_counts))
max_year = years[max_index]
max_count = movie_counts[max_index]
# 在坡峰的位置添加注释
plt.annotate(f'Peak: ({max_year}, {max_count})', xy=(max_year, max_count), xytext=(max_year, max_count+200),
             arrowprops=dict(facecolor='black', arrowstyle='->'), fontsize=10,ha='center')
# 调整箭头线的长度
plt.xlim(min(years), max(years))
plt.ylim(min(movie_counts), max(movie_counts)+250)

plt.show()


# 统计每个类型的电影平均评分并按平均评分降序排列
genres = []
avg_ratings = []

with open('8.csv', 'r', encoding='utf-8-sig') as csvfile:
    reader = csv.reader(csvfile)
    next(reader)  # 跳过第一行表头
    for row in reader:
        genres.append(row[0])
        avg_ratings.append(float(row[1]))

# 获取前10个类型和对应的平均评分
top_genres = genres[:10]
top_avg_ratings = avg_ratings[:10]

# 创建柱状图
plt.figure(figsize=(10, 6))
plt.bar(top_genres, top_avg_ratings)

# 设置 x 轴和 y 轴标题
plt.xlabel('电影类型', fontdict={'fontsize': 12, 'fontweight': 'bold'})
plt.ylabel('平均评分', fontdict={'fontsize': 12, 'fontweight': 'bold'})

# 设置图表标题
plt.title('每个类型的电影平均评分', fontdict={'fontsize': 14, 'fontweight': 'bold'})
# 在柱状图顶部显示数值
for i in range(len(top_avg_ratings)):
    plt.text(i, avg_ratings[i], '{:.2f}'.format(avg_ratings[i]), ha='center', va='bottom')

plt.show()