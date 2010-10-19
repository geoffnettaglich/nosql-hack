import sys
import simplejson as json
import urllib
import traceback
import gzip
import fileinput
import re
import getopt
from datetime import datetime

FILTER_PROJECT = 'en'
LIMIT = 10

def main():
    
    try:
        opts, args = getopt.getopt(sys.argv[1:], "p:m:L:")
    except getopt.GetoptError, err:
        print str(err)
        sys.exit(2)
    
    if(not args):
        usage()
        exit(2)

    file = args[0]
    
    filters = dict(
        proj = FILTER_PROJECT,
        limit = LIMIT)
    
    for o, a in opts:
        if o == "-p":
            filters['proj'] = a
        elif o == "-m":
            filters['match'] = a
        elif o == "-L":
            filters['limit'] = a
            
    process_file(file, filters)

def process_file(file, filters):
    # this is used for all records in a file
    timestamp = extract_timestamp(file);
    count = 0;
    processed = 0;
    for line in fileinput.input(file, openhook=fileinput.hook_compressed):
        if(parse(count, line, timestamp, filters)):
            processed += 1

        count += 1;
        
        # cheeky way to limit th number of proccessed items before bailing
        if(0 < filters['limit'] and filters['limit'] < processed):
            break

def parse(count, line, timestamp, filters):
    pts = line.split()  
    processed = False
    try:
        # unicode(urllib.unquote_plus(pts[1]), 'UTF-8').encode('UTF-8')
        row = dict(proj=pts[0],
                url=urllib.unquote_plus(pts[1]),
                cnt=pts[2],
                bytes=pts[3],
                ts=timestamp.isoformat())
                
        if(row_matches(row, filters)):
            print json.dumps(row)
            processed = True
    except:
        sys.stderr.write("%i:%s"%(count, line))
        print "%i:%s"%(count, line)
        #print sys.exc_info()
        
    return processed

def row_matches(row, filters):
    match = filters['proj'] == row['proj']
    
    # only check the 'match' filter if one was supplied
    if(match and ('match' in filters) ):
        match = match and re.search(filters['match'], row['url'], re.I)
        
    return match

# expected format of filename 'pagecount-YYYYmmdd-HHMMSS.gz'
def extract_timestamp(filename):
    [name, sep, ext]  = filename.partition('.')
    [type, sep, time ] = name.partition('-')    
    ts = datetime.strptime(time, '%Y%m%d-%H%M%S')
    return ts

def usage():
    print "usage:"
    print "\t" + sys.argv[0] + " [-p proj (en)] [-l limit (10)] [-m 'regex match'] file.gz"
    
if __name__ == "__main__":
    main()