import re
import sqlite3 as sqlite
from collections import defaultdict
from urllib.request import urlopen, urljoin

from bs4 import BeautifulSoup

ignorewords = set(['the', 'of', 'to', 'and','in', 'a', 'is', 'it'])

class Crawler:

    # 初始化crawler类并传入数据库名称
    def __init__(self, dbname):
        self.con = sqlite.connect(dbname)

    def __del__(self):
        self.con.close()

    def dbcommit(self):
        self.con.commit()

    # 辅助函数，用于获取条目的id，并且如果条目不存在，就将其加入数据库中
    def get_entryid(self, table, field, value, createnew=True):
        cur = self.con.execute(
                "select rowid from %s where %s='%s'" % (table, field, value))
        res = cur.fetchone()
        if res == None:
            cur = self.con.execute(
                    "insert into %s(%s) values('%s')" % (table, field, value))
            return cur.lastrowid
        else:
            return res[0]

    # 为每个网页建立索引
    def add_to_index(self, url, soup):
        if self.is_indexed(url):
            return
        print('Indexing %s' % url)

        # 获取每个单词
        text = self.get_text(soup)
        words = self.separate_words(text)

        # 得到URL的id
        urlid = self.get_entryid('urllist', 'url', url)

        # 将每个单词与该url关联
        for loc,word in enumerate(words):
            if word in ignorewords:
                continue
            wordid = self.get_entryid('wordlist', 'word', word)
            self.con.execute(
                    'insert into wordlocation(urlid, wordid, location) \
                    values(%d, %d, %d)' % (urlid, wordid, loc))

    # 从一个HTML网页中提取文字(不带标签的)
    def get_text(self, soup):
        v = soup.string
        if v == None:
            c = soup.contents
            result_text = ''
            for t in c:
                subtext = self.get_text(t)
                result_text += subtext+'\n'
            return result_text
        else:
            return v.strip()

    # 根据任何非空白字符进行分词处理
    def separate_words(self, text):
        splitter = re.compile('\\W*')
        return [s.lower() for s in splitter.split(text) if s != '']

    # 如果url已经建立过索引，则返回ture
    def is_indexed(self, url):
        u = self.con.execute(
                "select rowid from urllist where url = '%s'" % url).fetchone()
        if u != None:
            v = self.con.execute(
                "select * from wordlocation where urlid=%d" % u[0]).fetchone()
            if v != None:
                return True
        return False

    # 添加一个关联两个网页的链接
    def add_linkref(self, urlFrom, urlTo, linkText):
        self.con.execute("insert into link(fromid, toid) values('%s', '%s')" \
                                                            % (urlFrom, urlTo))

    # 从一小组网页开始进行广度优先搜索，直至某一给定深度
    # 期间为网页建立索引
    def crawl(self, pages, depth=2):
        for i in range(depth):
            new_pages = set()
            for page in pages:
                try:
                    c = urlopen(page, timeout=60)
                except:
                    print('Could not open %s' % page)
                    continue
                try:
                    soup = BeautifulSoup(c.read(), 'html.parser')
                except:
                    print('Could not read %s' % page)
                self.add_to_index(page, soup)

                links = soup('a')
                for link in links:
                    if 'href' in link.attrs:
                        url = urljoin(page, link['href'])
                        if url.find("'") != -1:
                            continue
                        url = url.split('#')[0]  # 去掉位置部分
                        if url[0:4] == 'http' and not self.is_indexed(url):
                            new_pages.add(url)
                        text = self.get_text(link)
                        self.add_linkref(page, url, text)
                self.dbcommit()
            pages = new_pages

    # 创建数据库表
    def create_index_tables(self):
        self.con.execute('create table urllist(url)')
        self.con.execute('create table wordlist(word)')
        self.con.execute('create table wordlocation(urlid, wordid, location)')
        self.con.execute('create table link(fromid integer, toid integer)')
        self.con.execute('create table linkwords(wordid, linkid)')
        self.con.execute('create index wordidx on wordlist(word)')
        self.con.execute('create index urlidx on urllist(url)')
        self.con.execute('create index wordurlidx on wordlocation(wordid)')
        self.con.execute('create index urltoidx on link(toid)')
        self.con.execute('create index urlfromidx on link(fromid)')
        self.dbcommit()

    def calculate_pagerank(self, iterations=20):
        """计算page的pagerank"""
        # 清除当前的PageRank表
        self.con.execute('drop table if exists pagerank')
        self.con.execute('create table pagerank(urlid primary key, score)')

        # 初始化每个url，令其PageRank值为1
        self.con.execute('insert into pagerank select rowid, 1.0 from urllist')
        self.dbcommit()

        for i in range(iterations):
            print("Iteration %d" % i)
            for (urlid,) in self.con.execute('select rowid from urllist'):
                pr = 0.15

                # 循环遍历指向当前网页的所有其他网页
                for (linker,) in self.con.execute(
                    "select distinct fromid from link where toid=%d" % urlid):
                    # 得到链接源对应网页的PageRank值
                    linking_pr = self.con.execute(
                        "select score from pagerank where urlid=%d" \
                                            % linker).fetchone()[0]
                    # 根据链接源，求得总的链接数
                    linking_count = self.con.execute(
                            "select count(*) from link where fromid=%d" \
                                            % linker).fetchoe()[0]
                    pr += 0.85 * (linking_pr/linking_count)
                self.con.execute(
                   "update pagerank set score=%f where urlid=%d" % (pr, urlid))
            self.dbcommit()


class Searcher:
    def __init__(self, dbname):
        self.con = sqlite.connect(dbname)

    def __del__(self):
        self.con.close()

    def get_match_rows(self, q):
        """
        假设q为两个单词，单词id分别为10和17
        则fullquery为：
            select w0.urlid, w0.location, w1.location
            from wordlocation w0, wordlocation w1
            where w0.urlid=w1.urlid
                and w0.wordid=10
                and w1.wordid=17
        即返回同时含有这两个单词的urlid
        """
        # 构造查询的字符串
        fieldlist = 'w0.urlid'
        tablelist = ''
        clauselist = ''
        wordids = []

        # 根据空格拆分单词
        words = q.split(' ')
        table_number = 0

        for word in words:
            # 获取单词的ID
            wordrow = self.con.execute(
                "select rowid from wordlist where word='%s'" % word).fetchone()
            if wordrow != None:
                wordid = wordrow[0]
                wordids.append(wordid)
                if table_number > 0:
                    tablelist += ','
                    clauselist += ' and w%d.urlid=w%d.urlid and ' \
                                        % (table_number-1, table_number)
                fieldlist += ',w%d.location' % table_number
                tablelist += 'wordlocation w%d' % table_number
                clauselist += 'w%d.wordid=%d' % (table_number, wordid)
                table_number += 1

        # 根据各个组分，建立查询
        fullquery = 'select %s from %s where %s' \
                            % (fieldlist, tablelist, clauselist)
        cur = self.con.execute(fullquery)
        # rows的形式为[[urlid, location, ...], ...]
        rows = [row for row in cur]

        return rows, wordids

    def get_scored_list(self, rows, wordids):
        total_scores = {row[0]:0 for row in rows}

        # 此处是稍后放置评价函数的地方
        weights = [
            (1.0, self.frequency_score(rows)),
            (1.0, self.location_score(rows)),
            (1.0, self.distance_score(rows)),
            (1.0, self.inboundlink_score(rows)),
            (1.0, self.pagerank_score(rows)),
        ]

        for weight, scores in weights:
            for url in total_scores:
                total_scores[url] += weight*scores[url]

        return total_scores

    def get_url_name(self, id):
        return self.con.execute(
                "select url from urllist where rowid=%d" % id).fetchone()[0]

    def query(self, q):
        rows, wordids = self.get_match_rows(q)
        scores = self.get_scored_list(rows, wordids)
        ranked_scores = sorted([(score, url) for url,score in scores.items()],
                               reverse=True)
        for score, urlid in ranked_scores[0:10]:
            print("%f\t%s" % (score, self.get_url_name(urlid)))

    def normalize_scores(self, scores, smallIsBetter=False):
        """
        scores为一个包含URL ID与评价值的字典，
        函数根据smallIsBetter是否，返回一个带有相同ID，
        而评价值介于0和1之间的新字典(最佳结果的对应值为1)
        """
        vsmall = 0.00001  # 避免被零整除
        if smallIsBetter:
            minscore = min(scores.values())
            return {u:float(minscore)/max(vsmall, l)
                        for u,l in scores.items()}
        else:
            maxscore = max(scores.values())
            if maxscore == 0:
                maxscore = vsmall
            return {u:float(c)/maxscore
                        for u,c in scores.items()}

    def frequency_score(self, rows):
        """
        根据词频对每个网页进行打分，词频越大分值越高
        """
        counts = defaultdict(int)
        for row in rows:
            counts[row[0]] += 1
        return self.normalize_scores(counts)

    def location_score(self, rows):
        """
        根据词在网页中出现的位置对网页进行打分，越靠前分值越高
        """
        locations = {row[0]:100000 for row in rows}
        for row in rows:
            loc = sum(row[1:])
            if loc < locations[row[0]]:
                locations[row[0]] = loc
        return self.normalize_scores(locations, smallIsBetter=True)

    def distance_score(self, rows):
        """
        根据词与词之间的距离对网页进行打分，距离越小分值越高
        """
        # 如果仅有一个单词，则得分都一样
        if len(rows[0]) <= 2:
            return {row[0]:1.0 for row in rows}

        mindistance = {row[0]:1000000 for row in rows}

        for row in rows:
            dist = sum([abs(row[i] - row[i-1]) for i in range(2, len(row))])
            if dist < mindistance[row[0]]:
                mindistance[row[0]] = dist
        return self.normalize_scores(mindistance, smallIsBetter=True)

    def inboundlink_score(self, rows):
        """
        根据指向自己的链接数对网页进行打分，链接数越大分值越高
        """
        uniqueurls = set([row[0] for row in rows])
        inboundcount = {u:self.con.execute(
            "select count(*) from link where toid=%d" % u).fetchone()[0] \
            for u in uniqueurls}
        return self.normalize_scores(inboundcount)

    def pagerank_score(self, rows):
        """
        根据网页的PageRank值对网页进行打分
        """
        pageranks = {}
        for row in rows:
            score = self.con.execute(
                    'select score from pagerank where urlid=%d' \
                            % row[0]).fetchone()[0]
            pageranks[row[0]] = score
        maxrank = max(pageranks.values())
        normalize_scores = {u:float(l)/maxrank for u,l in pageranks.items()}
        return normalize_scores
