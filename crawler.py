#!/usr/bin/python
# -*- coding: utf-8 -*-
import urllib2, MySQLdb
from BeautifulSoup import *
from urlparse import urljoin
import datetime, threading, time
from pymorphy import get_morph

ignorewords=set(['the','of','to','and','a','in','is','it'])

def tracer(f):
    def tmp(*args, **kwargs):
        print "*" * 80
        print "Call:", f.__name__
        print "Args:", args, kwargs
        res = f(*args, **kwargs)
        print f.__name__, "return", res
        print "*" * 80
        return res
    return tmp    

class Crawler_Error(Exception): pass

class Db_manager(object):
    
    def __init__(self, dbname = 'crawler_db'):
        self.conn  = None
        while not self.conn:
            try:
                self.conn = MySQLdb.connect(user = 'crawler', db = dbname, passwd = 'crawler', unix_socket = '/var/run/mysqld/mysqld.sock')
            except Exception:
                time.sleep(0.01)
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


class Crawler(Db_manager, threading.Thread):
    def __init__(self, page, working_list, mutex, dbname = 'crawler_db'):
        Db_manager.__init__(self, dbname)
        threading.Thread.__init__(self)
        self.working_list = working_list
        self.page = page
        self.mutex = mutex
        self.morph = get_morph('dicts')
        with self.mutex:
            working_list.append(page)

    # Get entry id, or add entry to database if it not exsist yet.
    #@tracer
    def get_entry_id(self, table, field, value, createnew=True):
        self.cursor.execute(
                "select id from %s where %s='%s'" % (table, field, value.encode('utf-8')))
        res = self.cursor.fetchone()
        if res == None:
            self.cursor.execute('SET NAMES `utf8`')
            cur = self.cursor.execute(
                    "insert into %s (%s) values ('%s')" % (table, field, value.encode('utf-8')))
            cur = self.conn.insert_id()
            return cur
        else:
            return res[0]

    # Index page
    #@tracer
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
            morph_word = self.morph.normalize(words[i])
            if type(morph_word) == "set":
                word = list(self.morph.normalize(words[i]))
            else:
                word = morph_word
            print "Word", word
            if word in ignorewords: continue
            wordid = self.get_entry_id('wordlist','word',word)
            self.cursor.execute('SET NAMES `utf8`')
            self.cursor.execute("insert into wordlocation(urlid, wordid, location) \
                    values (%d, %d, %d)" % (urlid, wordid, i))

    # Get text from page
    #@tracer
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
    #@tracer
    def separate_words(self,text):
        text = re.sub("[\\\\,=+!#№\?/^:'()@#|$;%&*{}\_\]\[]", "", text)
        return [s.lower() for s in text.split() if s!='']

    # Returns True if page is indexed
    #@tracer
    def is_indexed(self, url):
        #print "[Crawler.is_indexed]", url
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
    #@tracer
    def add_link_ref(self, urlFrom, urlTo):
        id_from = self.get_entry_id('urllist', 'url', urlFrom)
        id_to = self.get_entry_id('urllist', 'url', urlTo)
        cur = self.cursor.execute("insert into link (fromid, toid) values (%d, %d)" % (id_from, id_to))

    #@tracer
    def normalize_url(self, url):
        url = url.split('#')[0].split("?")[0] 
        if url[-1] == "/": return url[0:-1]
        return url

    # Indexing all page from list 'pages' with depth.
    #@tracer
    def run(self):
        newpages = []
        if not self.page: return False
        try:
            c = urllib2.urlopen(self.page)
        except Exception:
            print "Can't open page", self.page
            return False
        soup = BeautifulSoup(c.read())
        for elem in soup.findAll(['script', 'style']):
            elem.extract()
        self.add_to_index(self.page, soup)
        links=soup('a')
        for link in links:
            if ('href' in dict(link.attrs)):
                url = urljoin(self.page, link['href'])
                if url.find("'") != -1: continue
                url = self.normalize_url(url)
                if url[0:4]=='http' and not self.is_indexed(url):
                    linkText = self.get_text_only(link)
                    self.add_link_ref(self.page, url)
                    self.dbcommit()
                    if url not in self.working_list:
                        crawler = Crawler(url, self.working_list, self.mutex)
                        crawler.start()
        with self.mutex:
            self.working_list.remove(self.page)            
        return True

if __name__ == "__main__":
    pagelist = ['https://www.google.ru']

    mutex = threading.Lock()

    db_manager = Db_manager()
    db_manager.delete_index_tables()
    db_manager.create_index_tables()

    crawlers = []; working_list = []
    t1 = datetime.datetime.now()
    for page in  pagelist:
            crawler = Crawler(page, working_list, mutex)
            crawler.start()

    print "Finished in", datetime.datetime.now() - t1
