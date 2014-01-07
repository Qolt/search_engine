#!/usr/bin/python
# -*- coding: utf-8 -*-
import urllib2, MySQLdb
from BeautifulSoup import *
from urlparse import urljoin

ignorewords=set(['the','of','to','and','a','in','is','it'])

class Crawler:
    def __init__(self, dbname = 'crawler_db'):
        #self.conn = MySQLdb.connect(user='crawler', db = dbname, passwd='crawler', host='localhost')
        self.conn = MySQLdb.connect(user = 'crawler', db = dbname, passwd = 'crawler', unix_socket = '/var/run/mysqld/mysqld.sock')
        self.cursor = self.conn.cursor()

    def __del__(self):
        self.conn.close()

    def dbcommit(self):
        self.conn.commit()

    def delete_index_tables(self):
        self.cursor.execute("DROP TABLE IF EXISTS wordlocation")
        self.cursor.execute("DROP TABLE IF EXISTS linkwords")
        self.cursor.execute("DROP TABLE IF EXISTS link")
        self.cursor.execute("DROP TABLE IF EXISTS urllist")
        self.cursor.execute("DROP TABLE IF EXISTS wordlist")
        self.dbcommit()

    def create_index_tables(self):
        self.cursor.execute("""create table urllist(
                                id int(20) unsigned NOT NULL auto_increment, 
                                url varchar(2000) NOT NULL, 
                                PRIMARY KEY (id))""")
        self.cursor.execute("""create table wordlist(
                                id int(20) unsigned NOT NULL auto_increment, 
                                word varchar(1000) NOT NULL, 
                                PRIMARY KEY (id))""")
        self.cursor.execute("""create table wordlocation(
                                id int(10) unsigned NOT NULL auto_increment, 
                                urlid int(20) unsigned NOT NULL, 
                                wordid int(20) unsigned NOT NULL, 
                                location int(20) unsigned NOT NULL, 
                                PRIMARY KEY (id), 
                                FOREIGN KEY (urlid) REFERENCES urllist(id), 
                                FOREIGN KEY (wordid) REFERENCES wordlist(id))""")
        self.cursor.execute("""create table link(
                                id int(20) unsigned NOT NULL auto_increment, 
                                fromid int(20) unsigned NOT NULL, 
                                toid int(20) unsigned NOT NULL, 
                                PRIMARY KEY (id), 
                                FOREIGN KEY (fromid) REFERENCES urllist(id), 
                                FOREIGN KEY (toid) REFERENCES urllist(id))""")
        self.cursor.execute("""create table linkwords(
                                id int(20) unsigned NOT NULL auto_increment, 
                                wordid int(20) unsigned NOT NULL, 
                                linkid int(20) unsigned NOT NULL, 
                                PRIMARY KEY (id), 
                                FOREIGN KEY (wordid) REFERENCES wordlist(id), 
                                FOREIGN KEY (linkid) REFERENCES link(id))""")
        self.dbcommit()

    # Get entry id, or add entry to database if it not exsist yet.
    def get_entry_id(self,table,field,value,createnew=True):
        self.cursor.execute(
                "select id from %s where %s='%s'" % (table, field, value.encode('utf-8')))
        res = self.cursor.fetchone()
        if res == None:
            #Построение индекса
            self.cursor.execute('SET NAMES `utf8`')
            cur = self.cursor.execute(
                    "insert into %s (%s) values ('%s')" % (table, field, value.encode('utf-8')))
            cur = self.conn.insert_id()
            return cur
        else:
            return res[0]

    # Index page
    def add_to_index(self,url,soup):
        if self.is_indexed(url): return
        print 'Indexing ' + str(url)
        # Get words list
        text = self.get_text_only(soup)
        words = self.separate_words(text)
        # Get URL id
        urlid = self.get_entry_id('urllist','url',url)
        # Add link for each word to URL
        for i in range(len(words)):
            word = words[i]
            if word in ignorewords: continue
            wordid = self.get_entry_id('wordlist','word',word)
            self.cursor.execute('SET NAMES `utf8`')
            self.cursor.execute("insert into wordlocation(urlid, wordid, location) \
                    values (%d, %d, %d)" % (urlid, wordid, i))

    # Get text from page
    def get_text_only(self, soup):
        v = soup.string
        if v == None:
            content = soup.contents
            result_text = ''
            for t in content:
                subtext = self.get_text_only(t)
                result_text += subtext + '\n'
            return result_text
        else:
            return v.strip()

    # Split text
    def separate_words(self,text):
        text = re.sub("[\\\\,=+!#№\?/^:'()@#|$;%&*{}\_\]\[]", "", text)
        return [s.lower() for s in text.split() if s!='']

    # Returns True if page is indexed
    def is_indexed(self,url):
        u = self.cursor.execute \
                ("select id from urllist where url='%s'" % url.encode("utf-8"))
        u = self.cursor.fetchone()
        if u != None:
            v = self.cursor.execute(
                    'select * from wordlocation where urlid=%d' % u[0])
            v = self.cursor.fetchone()
            if v != None: return True
            return False

    # Add links from one page to anothers
    def add_link_ref(self,urlFrom,urlTo,linkText):
        id_from = self.get_entry_id('urllist', 'url', urlFrom)
        id_to = self.get_entry_id('urllist', 'url', urlTo)
        cur = self.cursor.execute("insert into link (fromid, toid) values (%d, %d)" % (id_from, id_to))

    # Indexing all page from list 'pages' with depth.
    def crawl(self,pages,depth=2):
        for i in range(depth):
            newpages=set()
            for page in pages:
                try:
                    c=urllib2.urlopen(page)
                except:
                    print "Can't open page", page
                    continue
                soup=BeautifulSoup(c.read())
                for elem in soup.findAll(['script', 'style']):
                    elem.extract()
                self.add_to_index(page,soup)
                links=soup('a')
                for link in links:
                    if ('href' in dict(link.attrs)):
                        url=urljoin(page,link['href'])
                        if url.find("'")!=-1: continue
                        url=url.split('#')[0] # Delete part of URL after '#'
                        if url[0:4]=='http' and not self.is_indexed(url):
                            newpages.add(url)
                            linkText=self.get_text_only(link)
                            self.add_link_ref(page,url,linkText)
                            self.dbcommit( )
                            pages=newpages

if __name__ == "__main__":
    pagelist = ['https://www.google.ru']
    crawler = Crawler()
    #crawler.delete_index_tables()
    #crawler.create_index_tables()
    crawler.crawl(pagelist, 3)
