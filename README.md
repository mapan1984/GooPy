## goopy

goopy是一个搜索引擎。

## 使用

建立索引：

    >>> from searchengine import Crawler as C
    >>> c = C('searchindex.db')
    >>> c.create_index_tables()  # 创建数据表
    >>> c.crawl(['https://en.wikipedia.org/wiki/Programming_language'])  # 开始爬取网页并建立索引
    >>> c.calculatepagerank()  # 计算每个网页的PageRank值

查询：

    >>> from searchengine import Searcher as S
    >>> s = S('searchindex.db')
    >>> s.query('functional programming')
