#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import re
import sys
from pathlib import Path

import boto3
import pandas as pd
from botocore.client import Config


def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Variable d'environnement manquante: {name}")
    return value


def normalize_endpoint(host_or_url: str) -> str:
    host_or_url = host_or_url.strip()
    if host_or_url.startswith("http://") or host_or_url.startswith("https://"):
        return host_or_url.rstrip("/")
    return f"https://{host_or_url.rstrip('/')}"


def detect_separator(file_path: Path) -> str:
    sample = file_path.read_text(encoding="utf-8", errors="ignore")[:20000]
    candidates = [";", ",", "\t", "|"]
    counts = {sep: sample.count(sep) for sep in candidates}
    return max(counts, key=counts.get)


def slugify_column(name: str) -> str:
    name = str(name).strip().lower()
    replacements = {
        "é": "e", "è": "e", "ê": "e", "ë": "e",
        "à": "a", "â": "a", "ä": "a",
        "î": "i", "ï": "i",
        "ô": "o", "ö": "o",
        "ù": "u", "û": "u", "ü": "u",
        "ç": "c", "œ": "oe", "æ": "ae",
        "'": "",
    }
    for src, dst in replacements.items():
        name = name.replace(src, dst)
    name = re.sub(r"[^a-z0-9]+", "_", name)
    name = re.sub(r"_+", "_", name).strip("_")
    return name or "col"


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    seen: dict[str, int] = {}
    new_cols: list[str] = []

    for col in df.columns:
        base = slugify_column(col)
        if base in seen:
            seen[base] += 1
            new_cols.append(f"{base}_{seen[base]}")
        else:
            seen[base] = 0
            new_cols.append(base)

    df.columns = new_cols
    return df


def cast_numeric_columns(df: pd.DataFrame) -> pd.DataFrame:
    excluded = {"tour", "source_file", "ingested_at_utc"}

    for col in df.columns:
        if col in excluded:
            continue

        if not pd.api.types.is_object_dtype(df[col]) and not pd.api.types.is_string_dtype(df[col]):
            continue

        s = df[col].astype("string")
        cleaned = (
            s.str.strip()
            .str.replace("\u00A0", "", regex=False)
            .str.replace(" ", "", regex=False)
            .str.replace(",", ".", regex=False)
        )

        non_empty = cleaned.dropna()
        non_empty = non_empty[non_empty != ""]
        if len(non_empty) == 0:
            continue

        ratio_numeric = non_empty.str.match(r"^-?\d+(\.\d+)?$").mean()

        if ratio_numeric >= 0.98:
            numeric = pd.to_numeric(cleaned.replace("", pd.NA), errors="coerce")
            if numeric.dropna().mod(1).eq(0).all():
                df[col] = numeric.astype("Int64")
            else:
                df[col] = numeric.astype("Float64")

    return df


def load_csv(file_path: Path, tour: int) -> pd.DataFrame:
    print(f"Lecture tour {tour}: {file_path}")

    if not file_path.exists():
        raise FileNotFoundError(f"Fichier introuvable: {file_path}")

    sep = detect_separator(file_path)

    last_error = None
    for encoding in ("utf-8-sig", "utf-8", "latin1", "cp1252"):
        try:
            df = pd.read_csv(
                file_path,
                sep=sep,
                encoding=encoding,
                dtype=str,
                low_memory=False,
            )
            df = normalize_columns(df)
            df["tour"] = str(tour)
            df["source_file"] = str(file_path.name)
            return df
        except Exception as e:
            last_error = e

    raise RuntimeError(f"Impossible de lire le CSV {file_path}: {last_error}")



def get_s3_client():
    endpoint = normalize_endpoint(require_env("CELLAR_ADDON_HOST"))
    access_key = require_env("CELLAR_ADDON_KEY_ID")
    secret_key = require_env("CELLAR_ADDON_KEY_SECRET")

    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="default",
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
            request_checksum_calculation="WHEN_REQUIRED",
            response_checksum_validation="WHEN_REQUIRED",
        ),
    )


def upload_to_s3(local_file: Path, bucket: str, s3_key: str) -> None:
    client = get_s3_client()
    print(f"Upload vers s3://{bucket}/{s3_key}")

    with open(local_file, "rb") as f:
        data = f.read()  # 👈 important

    client.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=data,  # 👈 bytes, pas file
        ContentType="application/octet-stream",
    )

    print("Upload terminé ✅")

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Convertit les résultats municipales 2026 en parquet puis upload sur Cellar/S3."
    )
    parser.add_argument("--tour1", required=True, help="Chemin du CSV du 1er tour")
    parser.add_argument("--tour2", required=True, help="Chemin du CSV du 2e tour")
    parser.add_argument(
        "--output",
        default="municipales_2026_bureaux_vote_france.parquet",
        help="Chemin du fichier parquet de sortie",
    )
    parser.add_argument(
        "--s3-key",
        default=None,
        help="Clé S3 cible. Si absente, une valeur par défaut est utilisée.",
    )
    parser.add_argument(
        "--no-upload",
        action="store_true",
        help="Génère le parquet sans upload S3",
    )

    args = parser.parse_args()

    tour1_path = Path(args.tour1)
    tour2_path = Path(args.tour2)
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    df1 = load_csv(tour1_path, tour=1)
    df2 = load_csv(tour2_path, tour=2)

    print("Concaténation...")
    df = pd.concat([df1, df2], ignore_index=True, sort=False)

    print("Conversion des colonnes numériques...")
    df = cast_numeric_columns(df)

    df["ingested_at_utc"] = pd.Timestamp.utcnow().isoformat()

    print(f"Écriture du parquet: {output_path}")
    df.to_parquet(
        output_path,
        engine="pyarrow",
        compression="snappy",
        index=False,
    )

    if not args.no_upload:
        bucket = require_env("S3_BUCKET")
        s3_key = args.s3_key or f"elections/municipales_2026/{output_path.name}"
        upload_to_s3(output_path, bucket, s3_key)

    print("Terminé.")
    print(f"Parquet local: {output_path}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as e:
        print(f"Erreur: {e}", file=sys.stderr)
        raise SystemExit(1)
