# aver-scripts

Utility scripts for the Aver security management database.

## gender_match.py

Infers gender from first names in the `SecurityPersonnel_combination` table and writes the results to `NameGenderInference` via a MERGE upsert.

### How it works

1. Fetches distinct first names from the database.
2. Normalizes each name (strips prefixes like "Mr./Mrs./Dr.", punctuation, etc.).
3. Looks up the normalized name against a pre-built CSV of ~2,900 names using the [gender-guesser](https://pypi.org/project/gender-guesser/) library.
4. Assigns a gender bucket (`male`, `female`, or `unknown`) with a confidence score.
5. Upserts all results into `dbo.NameGenderInference`.

### Setup

```
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

Create a `.env` file in the project root:

```
DB_CONNECTION_STRING=Driver={ODBC Driver 18 for SQL Server};Server=...;Database=...;
```

### Usage

```
python gender_match.py
```
