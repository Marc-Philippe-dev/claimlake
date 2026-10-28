"""Source 2 - the claims handler drops its daily CSV files in the landing zone.

Copies every generated file dated up to --until that is not delivered yet, which
simulates the passing of time (one month at a time, for example).

  python scripts/drop_claims_files.py --data data --landing data/landing --until 2025-03-31
"""

from __future__ import annotations

import argparse
import shutil
from datetime import date
from pathlib import Path


def drop_until(data_dir: Path, landing: Path, until: date) -> int:
    target = landing / "claims"
    target.mkdir(parents=True, exist_ok=True)
    delivered = 0
    for src in sorted((data_dir / "claims").glob("claims_*.csv")):
        day = date.fromisoformat(src.stem.removeprefix("claims_"))
        if day <= until and not (target / src.name).exists():
            shutil.copy2(src, target / src.name)
            delivered += 1
    return delivered


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", default="data", type=Path)
    parser.add_argument("--landing", default="data/landing", type=Path)
    parser.add_argument("--until", required=True, type=date.fromisoformat)
    args = parser.parse_args()
    n = drop_until(args.data, args.landing, args.until)
    print(f"claims handler: {n} new file(s) delivered up to {args.until}")


if __name__ == "__main__":
    main()
