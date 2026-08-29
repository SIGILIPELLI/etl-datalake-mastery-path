# 03 · Extraction Basics (Files, APIs, Databases)

Extraction is the "E" in ETL/ELT: getting raw data out of wherever it lives
and into a shape your pipeline can work with. This lesson walks through the
three most common sources — a local file, an HTTP API, and a database — and
the one design rule that applies to all three: **extraction should not judge
the data, only fetch it.**

!!! note "What actually ran"
    File and database extraction ran locally against the standard library
    (`csv`, `json`, `sqlite3`). The API example uses Python's
    `urllib.request` but is shown against a local stand-in function instead
    of a live network call, since this lesson's code must run offline —
    the pagination logic itself is real and directly portable to `requests`
    or `urllib`.

## Extracting from a file

```python
import csv, io

csv_text = """product_id,name,price,in_stock
1,Widget,9.99,true
2,Gadget,19.99,false
3,Gizmo,14.50,true
"""

def extract_csv(text):
    reader = csv.DictReader(io.StringIO(text))
    return list(reader)

rows = extract_csv(csv_text)
print(f"Extracted {len(rows)} rows from CSV")
print(rows[0])
```

```text
Extracted 3 rows from CSV
{'product_id': '1', 'name': 'Widget', 'price': '9.99', 'in_stock': 'true'}
```

Every field is still a string — `price` is `"9.99"`, not `9.99`, and
`in_stock` is `"true"`, not `True`. That's correct behavior for an extract
step: type casting is a transform decision (Level 1, lesson 4), not an
extraction one.

## Extracting from an API (with pagination)

Real APIs rarely return everything in one response — they paginate. The
pattern is always the same: request a page, check whether there's a next
page, repeat until there isn't.

```python
# Stand-in for a real API — same shape as calling `requests.get(url, params=...)`
# and reading `.json()`, just without a live network call in this lesson.
_fake_pages = {
    1: {"items": [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}], "next_page": 2},
    2: {"items": [{"id": 3, "name": "Carla"}], "next_page": None},
}

def fetch_page(page_number):
    return _fake_pages[page_number]

def extract_api():
    all_items = []
    page = 1
    while page is not None:
        response = fetch_page(page)
        all_items.extend(response["items"])
        page = response["next_page"]
    return all_items

items = extract_api()
print(f"Extracted {len(items)} items across pages")
for item in items:
    print(" ", item)
```

```text
Extracted 3 items across pages
  {'id': 1, 'name': 'Alice'}
  {'id': 2, 'name': 'Bob'}
  {'id': 3, 'name': 'Carla'}
```

With a real API, `fetch_page` would be:

```python
import urllib.request, json

def fetch_page(page_number):
    url = f"https://api.example.com/users?page={page_number}"
    with urllib.request.urlopen(url) as resp:
        return json.load(resp)
```

The pagination loop itself doesn't change — that's the point of separating
"how do I know when to stop" (a loop over `next_page`) from "how do I fetch
one page" (the part that actually varies per API).

## Extracting from a database

```python
import sqlite3

# Set up a small "production" database to extract from
conn = sqlite3.connect(":memory:")
conn.execute("CREATE TABLE customers (id INTEGER, name TEXT, region TEXT)")
conn.executemany(
    "INSERT INTO customers VALUES (?, ?, ?)",
    [(1, "Ava Chen", "APAC"), (2, "Ravi Kumar", "APAC"), (3, "Lena Fischer", "EU")],
)
conn.commit()

def extract_db(connection, query):
    cursor = connection.execute(query)
    columns = [d[0] for d in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]

rows = extract_db(conn, "SELECT * FROM customers WHERE region = 'APAC'")
print(f"Extracted {len(rows)} rows from database")
for row in rows:
    print(" ", row)
```

```text
Extracted 2 rows from database
  {'id': 1, 'name': 'Ava Chen', 'region': 'APAC'}
  {'id': 2, 'name': 'Ravi Kumar', 'region': 'APAC'}
```

Two things worth noting: the query itself (`WHERE region = 'APAC'`) already
scopes the extraction — pushing filters down to the source is almost always
cheaper than pulling everything and filtering in Python. And extraction from
a live production database should always use a read replica or a dedicated
reporting connection when one exists, never the primary transactional
connection — an expensive analytical query competing with live traffic is a
common way pipelines cause outages.

## The "extraction should not judge" rule

Across all three sources, the shared discipline is: **extraction returns
what the source gave you, unmodified**, even if it looks wrong. Don't drop a
row with a missing field, don't correct a typo, don't cast a type here.
Every one of those is a business decision — normalizing them belongs in
Transform, where it's visible, testable, and change-controlled in one place,
not scattered wherever a source happens to be read.

```python
# Wrong: judging data during extraction
def extract_csv_bad(text):
    reader = csv.DictReader(io.StringIO(text))
    return [r for r in reader if r["in_stock"] == "true"]  # silent filtering!

# Right: extraction stays dumb, filtering is an explicit transform decision
def extract_csv_good(text):
    return list(csv.DictReader(io.StringIO(text)))
```

The "bad" version silently drops out-of-stock products with no record that
it happened — six months later, someone asks "why don't we have historical
out-of-stock data?" and the answer is buried in an extraction function
nobody thought to check.

## Traps

- **Filtering or casting inside extraction.** Keeps the pipeline harder to
  debug and impossible to reprocess differently later.
- **Extracting from a production primary database directly.** Use a
  replica, snapshot, or dedicated extract user/connection.
- **Assuming an API's first page is representative.** Rate limits, partial
  failures, and inconsistent page sizes are common — always loop until the
  source's actual "no more data" signal, never a fixed guess at page count.
- **Not recording extraction metadata.** Not shown above for brevity, but
  real pipelines should record *when* an extract ran and *how many rows* it
  pulled — that count is the cheapest sanity check available downstream.

## Cheat sheet

| Source | Extraction unit | Stop condition |
|---|---|---|
| File | Whole file / row iterator | End of file |
| API | One page per request | Empty page / no next-page token |
| Database | Query result set | Query returns 0 rows (or reaches the watermark, Level 2) |

## Exercise

Combine all three sources into one `extract_all()` function that returns a
dict `{"products": [...], "users": [...], "customers": [...]}` using the
three extraction functions above. Add a print statement that reports the row
count for each source, and confirm none of the three functions perform any
filtering, casting, or cleanup — if you find one that does, refactor it so
extraction stays "dumb."
