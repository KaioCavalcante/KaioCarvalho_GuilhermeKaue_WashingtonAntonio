import re
from dateutil.parser import parse as parse_date

PAT_ASIN = re.compile(r'^ASIN:\s+(\S+)')
PAT_TITLE = re.compile(r'^\s{2}title:\s+(.*)')
PAT_GROUP = re.compile(r'^\s{2}group:\s+(\w+)')
PAT_SALESRANK = re.compile(r'^\s{2}salesrank:\s+(\d+|NA)')
PAT_SIMILAR = re.compile(r'^\s{2}similar:\s+(\d+)\s+(.*)')
PAT_CATEGORIES = re.compile(r'^\s{2}categories:\s+(\d+)')
PAT_CATLINE = re.compile(r'^\s{3}\|(.*)$')
PAT_REVHDR = re.compile(r'^\s{2}reviews:')
PAT_REVIEW = re.compile(
    r'^\s{4}(\d{4}-\d{1,2}-\d{1,2})\s+cutomer:\s+(\S+)\s+rating:\s+(\d)\s+votes:\s+(\d+)\s+helpful:\s+(\d+)'
)
PAT_CAT_TOKEN = re.compile(r'(.*?)\[(\d+)\]')

class ProductBlock:
    def __init__(self):
        self.asin = None
        self.title = None
        self.group = None
        self.salesrank = None
        self.similars = []
        self.categories_paths = []
        self.reviews = []

    def valid(self):
        return self.asin is not None

def parse_snap_lines(lines):
    blk = None
    in_reviews = False
    for line in lines:
        line = line.rstrip('\n')

        if line.startswith("Id:"):
            if blk and blk.valid():
                yield blk
            blk = ProductBlock()
            in_reviews = False
            continue

        if not blk:
            continue

        m = PAT_ASIN.match(line)
        if m:
            blk.asin = m.group(1).strip()
            continue

        m = PAT_TITLE.match(line)
        if m:
            blk.title = m.group(1).strip()
            continue

        m = PAT_GROUP.match(line)
        if m:
            blk.group = m.group(1).strip()
            continue

        m = PAT_SALESRANK.match(line)
        if m:
            sr = m.group(1)
            blk.salesrank = int(sr) if sr.isdigit() else None
            continue

        m = PAT_SIMILAR.match(line)
        if m:
            count = int(m.group(1))
            rest = m.group(2).strip()
            asins = rest.split()
            blk.similars.extend(asins[:count])
            continue

        m = PAT_CATEGORIES.match(line)
        if m:
            in_reviews = False
            continue

        m = PAT_CATLINE.match(line)
        if m:
            path = m.group(1)
            tokens = [t for t in path.split('|') if t]
            parsed = []
            for tok in tokens:
                mm = PAT_CAT_TOKEN.match(tok)
                if mm:
                    name = mm.group(1)
                    cid = int(mm.group(2))
                    parsed.append((name, cid))
            if parsed:
                blk.categories_paths.append(parsed)
            continue

        if PAT_REVHDR.match(line):
            in_reviews = True
            continue

        if in_reviews:
            m = PAT_REVIEW.match(line)
            if m:
                d = parse_date(m.group(1)).date()
                cust = m.group(2)
                rating = int(m.group(3))
                votes = int(m.group(4))
                helpful = int(m.group(5))
                blk.reviews.append((d, cust, rating, votes, helpful))
            continue

    if blk and blk.valid():
        yield blk
