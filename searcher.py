#!/usr/bin/python
# -*- coding: utf-8 -*-
import  MySQLdb, sys
from pymorphy import get_morph

class Searcher:
    def __init__(self, dbname = 'crawler_db'):
        self.conn = MySQLdb.connect(user = 'crawler', db = dbname, passwd = 'crawler', unix_socket = '/var/run/mysqld/mysqld.sock')
        self.cursor = self.conn.cursor()
        self.morph = get_morph('dicts')

    def __del__(self):
        self.conn.close()

    def get_match_rows(self, q):
        # String for query building
        field_list = 'w0.urlid'
        table_list = ''
        clause_list = ''
        word_ids = []
        # Split query by words
        words = q.split(' ')
        table_number = 0
        for w in words:
            morph_word = self.morph.normalize(w)
            if type(morph_word) == "set":
                word = list(self.morph.normalize(words[i]))
            else:
                word = morph_word

            # Get words ids
            self.cursor.execute('SET NAMES `utf8`')
            self.cursor.execute(
                    "select id from wordlist where word='%s'" % word)
            word_row = self.cursor.fetchone()
            if word_row:
                word_id = word_row[0]
                word_ids.append(word_id)
                if table_number > 0:
                    table_list += ','
                    clause_list += ' and '
                    clause_list += 'w%d.urlid=w%d.urlid and ' % (table_number-1, table_number)
                field_list += ',w%d.location' % table_number
                table_list += 'wordlocation w%d' % table_number
                clause_list += 'w%d.wordid=%d' % (table_number, word_id)
                table_number += 1
        # Build query
        full_query='select %s from %s where %s' % (field_list, table_list, clause_list)
        self.cursor.execute(full_query)
        cur = self.cursor.fetchall()
        rows = [row for row in cur]
        return rows, word_ids

    def get_scored_list(self, rows, word_ids):
        total_scores = dict([(row[0],0) for row in rows])
        # Page ranging
        weights = [(1.5, self.location_score(rows)), 
                   (1.0, self.frequency_score(rows)), 
                   (1.0, self.inbound_link_score(rows)), 
                   (1.0, self.distance_score(rows))]
        for (weight, scores) in weights:
            for url in total_scores:
                total_scores[url] += weight * scores[url]
        return total_scores

    def get_url_name(self, id):
        self.cursor.execute('SET NAMES `utf8`')
        self.cursor.execute("select url from urllist where id=%d" % id)
        return self.cursor.fetchone()[0]

    def query(self, q):
        rows, word_ids = self.get_match_rows(q)
        scores = self.get_scored_list(rows, word_ids)
        ranked_scores = sorted([(score, url) for (url, score) in scores.items( )],\
                reverse=1)
        for (score, url_id) in ranked_scores[0:10]:
            print '%f\t%s' % (score, self.get_url_name(url_id))

    def normalize_scores(self, scores, small_is_better = 0):
        small = 0.00001 
        if small_is_better:
            minscore = min(scores.values())
            return dict([(u, float(minscore) / max(small, l)) for (u, l) \
                in scores.items()])
        else:
            maxscore = max(scores.values())
            if maxscore == 0: maxscore = small
            return dict([(u, float(c) / maxscore) for (u, c) in scores.items( )])

    def frequency_score(self, rows):
        counts = dict([(row[0],0) for row in rows])
        for row in rows: counts[row[0]] += 1
        return self.normalize_scores(counts)

    def location_score(self, rows):
        locations = dict([(row[0],1000000) for row in rows])
        for row in rows:
            loc = sum(row[1:])
            if loc < locations[row[0]]: locations[row[0]]=loc
        return self.normalize_scores(locations, small_is_better=1)

    def distance_score(self, rows):
        if len(rows[0]) <= 2: return dict([(row[0], 1.0) for row in rows])
        min_distance = dict([(row[0], 1000000) for row in rows])
        for row in rows:
            dist = sum([abs(row[i]-row[i-1]) for i in range(2, len(row))])
            if dist < min_distance[row[0]]: min_distance[row[0]]=dist
        return self.normalize_scores(min_distance, small_is_better = 1)

    def inbound_link_score(self, rows):
        unique_urls = set([row[0] for row in rows])
        inbound_count = {}
        for url in unique_urls:
            self.cursor.execute('select count(*) from link where id=%d' % url)
            inbound_count[url] = self.cursor.fetchone()[0]
        return self.normalize_scores(inbound_count)

if __name__ == "__main__":
    search  = Searcher()
    search.query(sys.argv[1])  
