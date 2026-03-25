import csv
import re
import os
from collections import Counter

import gender_guesser.detector as gender_detector
import pyodbc
from dotenv import load_dotenv

load_dotenv()

DB_CONNECTION_STRING = os.getenv("DB_CONNECTION_STRING")

if not DB_CONNECTION_STRING:
    raise ValueError("DB_CONNECTION_STRING is missing from .env")


def normalize_first_name(name: str) -> str | None:
    if not name:
        return None

    name = name.strip().lower()

    # remove common prefixes
    name = re.sub(r"^(mr|mrs|ms|miss|dr|officer|sgt|lt|capt)\.?\s+", "", name)

    # replace periods with spaces so "J. Michael" becomes "J Michael"
    name = name.replace(".", " ")

    # split on whitespace
    parts = name.split()
    if not parts:
        return None

    # take first token only
    first = parts[0]

    # keep only letters, apostrophes, hyphens
    first = re.sub(r"[^a-z'-]", "", first)

    return first or None


_GENDER_BUCKET_MAP = {
    "male": ("male", 0.99),
    "mostly_male": ("male", 0.75),
    "female": ("female", 0.99),
    "mostly_female": ("female", 0.75),
    "andy": ("unknown", 0.50),
    "unknown": ("unknown", 0.0),
}


def _load_gender_lookup(csv_path: str) -> dict[str, tuple[str, float, str]]:
    detector = gender_detector.Detector()
    lookup: dict[str, tuple[str, float, str]] = {}

    with open(csv_path, newline="", encoding="utf-8") as f:
        for row in csv.reader(f):
            if not row or not row[0].strip():
                continue
            name = row[0].strip()
            result = detector.get_gender(name.title())
            bucket, confidence = _GENDER_BUCKET_MAP.get(result, ("unknown", 0.0))
            lookup[name.lower()] = (bucket, confidence, "csv_gender_guesser")

    return lookup


_NAME_GENDER_LOOKUP = _load_gender_lookup(
    os.path.join(os.path.dirname(__file__), "distinct_names_export.csv")
)


def infer_gender_from_lookup(normalized_name: str) -> tuple[str, float, str]:
    return _NAME_GENDER_LOOKUP.get(normalized_name, ("unknown", 0.0, "unmatched"))


def fetch_distinct_first_names(cursor) -> list[str]:
    cursor.execute("""
        SELECT DISTINCT FirstName
        FROM dbo.SecurityPersonnel_combination
        WHERE FirstName IS NOT NULL
          AND LTRIM(RTRIM(FirstName)) <> ''
    """)
    return [row[0] for row in cursor.fetchall()]


def upsert_name_gender_inference(cursor, rows_to_upsert: list[tuple]):
    """
    rows_to_upsert columns:
    (FirstNameRaw, FirstNameNormalized, GenderBucket, Confidence, Source)
    """

    # temp staging table
    cursor.execute("""
        IF OBJECT_ID('tempdb..#NameGenderInferenceStage') IS NOT NULL
            DROP TABLE #NameGenderInferenceStage;

        CREATE TABLE #NameGenderInferenceStage (
            FirstNameRaw NVARCHAR(255) NOT NULL,
            FirstNameNormalized NVARCHAR(255) NOT NULL,
            GenderBucket NVARCHAR(20) NOT NULL,
            Confidence DECIMAL(5,4) NOT NULL,
            Source NVARCHAR(50) NOT NULL
        );
    """)

    insert_sql = """
        INSERT INTO #NameGenderInferenceStage
            (FirstNameRaw, FirstNameNormalized, GenderBucket, Confidence, Source)
        VALUES (?, ?, ?, ?, ?)
    """

    cursor.fast_executemany = True
    cursor.executemany(insert_sql, rows_to_upsert)

    cursor.execute("""
        MERGE dbo.NameGenderInference AS target
        USING (
            SELECT
                s.FirstNameRaw,
                s.FirstNameNormalized,
                s.GenderBucket,
                s.Confidence,
                s.Source
            FROM #NameGenderInferenceStage s
        ) AS src
        ON target.FirstNameNormalized = src.FirstNameNormalized
        WHEN MATCHED THEN
            UPDATE SET
                target.FirstNameRaw = src.FirstNameRaw,
                target.GenderBucket = src.GenderBucket,
                target.Confidence = src.Confidence,
                target.Source = src.Source,
                target.UpdatedAt = SYSUTCDATETIME()
        WHEN NOT MATCHED THEN
            INSERT (
                FirstNameRaw,
                FirstNameNormalized,
                GenderBucket,
                Confidence,
                Source,
                CreatedAt,
                UpdatedAt
            )
            VALUES (
                src.FirstNameRaw,
                src.FirstNameNormalized,
                src.GenderBucket,
                src.Confidence,
                src.Source,
                SYSUTCDATETIME(),
                SYSUTCDATETIME()
            );
    """)


def main():
    conn = pyodbc.connect(DB_CONNECTION_STRING)
    conn.autocommit = False

    try:
        cursor = conn.cursor()

        raw_names = fetch_distinct_first_names(cursor)
        print(f"Fetched {len(raw_names)} distinct first names")

        rows_to_upsert = []
        bucket_counter = Counter()

        for raw_name in raw_names:
            normalized = normalize_first_name(raw_name)

            if not normalized:
                bucket, confidence, source = ("unknown", 0.0, "normalization_failed")
                normalized = ""
            else:
                bucket, confidence, source = infer_gender_from_lookup(normalized)

            bucket_counter[bucket] += 1
            rows_to_upsert.append((
                raw_name,
                normalized,
                bucket,
                confidence,
                source
            ))

        upsert_name_gender_inference(cursor, rows_to_upsert)
        conn.commit()

        print("Upsert complete.")
        print("Bucket summary:")
        for bucket, count in bucket_counter.items():
            print(f"  {bucket}: {count}")

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()