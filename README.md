## goopy

goopy是一个搜索引擎。

## 使用

建立索引：

    >>> from searchengine import Crawler as C
    >>> c = C('searchindex.db')
    >>> c.create_index_tables()
    >>> c.crawl('https://en.wikipedia.org/wiki/Programming_language')
    >>> c.calculatepagerank()

查询：

    >>> from searchengine import Searcher as S
    >>> s = S('searchindex.db')
    >>> s.query('functional programming')
