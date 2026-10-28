"""Generate the ClaimsLake source data.

Base data: the public freMTPL2 French motor third-party liability dataset
(OpenML ids 41214 for policies and 41215 for claim amounts). It has no dates,
premiums or claim identifiers, so this script enriches it with SYNTHETIC values:
policy periods in the observation year, a simplified premium, claim dates,
declaration delays, status updates and deliberate data quality anomalies.

Outputs (in --out):
  pas/policies_initial.csv          initial load of the Policy Administration System
  pas/updates/updates_<date>.sql    daily UPDATE scripts that create SCD2 changes
  claims/claims_<date>.csv          daily files sent by the claims handler
  anomalies_manifest.csv            every injected anomaly, to check the pipeline

Usage:
  python scripts/generate_data.py --out data            # real freMTPL2 (needs internet)
  python scripts/generate_data.py --out data --synthetic --n-policies 20000
"""

from __future__ import annotations

import argparse
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

OBS_START = date(2025, 1, 1)
OBS_DAYS = 365

# Old French administrative regions used by freMTPL2, mapped to a sample
# department prefix to build synthetic postal codes.
REGION_TO_DEPT = {
    "R11": "75",
    "R21": "51",
    "R22": "80",
    "R23": "76",
    "R24": "45",
    "R25": "14",
    "R26": "21",
    "R31": "59",
    "R41": "57",
    "R42": "67",
    "R43": "25",
    "R52": "44",
    "R53": "35",
    "R54": "86",
    "R72": "33",
    "R73": "31",
    "R74": "87",
    "R82": "69",
    "R83": "63",
    "R91": "34",
    "R93": "13",
    "R94": "2A",
}
CLAIM_TYPES = ["COLLISION", "BRIS_DE_GLACE", "VOL", "INCENDIE", "CATASTROPHE_NATURELLE", "AUTRE"]
CLAIM_TYPE_WEIGHTS = [0.55, 0.2, 0.08, 0.03, 0.04, 0.10]


def load_fremtpl2() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Download freMTPL2freq and freMTPL2sev from OpenML."""
    from sklearn.datasets import fetch_openml

    freq = fetch_openml(data_id=41214, as_frame=True).frame
    sev = fetch_openml(data_id=41215, as_frame=True).frame
    freq["IDpol"] = freq["IDpol"].astype(int)
    sev["IDpol"] = sev["IDpol"].astype(int)
    for col in ["Area", "VehBrand", "VehGas", "Region"]:
        freq[col] = freq[col].astype(str).str.strip("'")
    return freq, sev


def synthetic_fremtpl2(n: int, rng: np.random.Generator) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Offline substitute with the same columns and plausible distributions."""
    regions = list(REGION_TO_DEPT)
    freq = pd.DataFrame(
        {
            "IDpol": np.arange(1, n + 1),
            "Exposure": np.clip(rng.beta(2.0, 1.2, n), 0.01, 1.0).round(2),
            "Area": rng.choice(list("ABCDEF"), n, p=[0.15, 0.11, 0.28, 0.22, 0.2, 0.04]),
            "VehPower": rng.integers(4, 15, n),
            "VehAge": np.clip(rng.gamma(2.0, 3.5, n), 0, 30).astype(int),
            "DrivAge": np.clip(rng.normal(46, 14, n), 18, 95).astype(int),
            "BonusMalus": np.clip(50 + rng.exponential(8, n), 50, 230).astype(int),
            "VehBrand": rng.choice([f"B{i}" for i in [1, 2, 3, 4, 5, 6, 10, 11, 12, 13, 14]], n),
            "VehGas": rng.choice(["Regular", "Diesel"], n),
            "Density": np.clip(rng.lognormal(6.0, 1.6, n), 1, 27000).astype(int),
            "Region": rng.choice(regions, n),
        }
    )
    lam = 0.08 * freq["Exposure"] * (freq["BonusMalus"] / 60.0)
    freq["ClaimNb"] = rng.poisson(lam)
    ids = np.repeat(freq["IDpol"].to_numpy(), freq["ClaimNb"].to_numpy())
    sev = pd.DataFrame({"IDpol": ids, "ClaimAmount": rng.lognormal(7.2, 1.1, len(ids)).round(2)})
    return freq, sev


def build_policies(freq: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
    exposure = freq["Exposure"].clip(0.01, 1.0).to_numpy()
    days = np.maximum((exposure * OBS_DAYS).round().astype(int), 1)
    start_offset = (rng.random(len(freq)) * (OBS_DAYS - days + 1)).astype(int)
    start = pd.to_datetime(OBS_START) + pd.to_timedelta(start_offset, unit="D")
    end = start + pd.to_timedelta(days, unit="D")

    # Simplified, NON-actuarial premium: enough to compute a plausible loss ratio.
    age_factor = np.where(freq["DrivAge"] < 25, 1.7, np.where(freq["DrivAge"] > 75, 1.2, 1.0))
    premium = (
        420.0
        * (freq["BonusMalus"] / 100.0)
        * (1 + 0.03 * (freq["VehPower"] - 4))
        * age_factor
        * (1 + 0.15 * (freq["Area"].isin(["E", "F"])))
    ).round(2)

    dept = freq["Region"].map(REGION_TO_DEPT).fillna("99")
    postal = dept + pd.Series(rng.integers(0, 1000, len(freq))).astype(str).str.zfill(3).values

    return pd.DataFrame(
        {
            "policy_id": freq["IDpol"].astype(int),
            "start_date": start.date,
            "end_date": end.date,
            "exposure": (days / OBS_DAYS).round(4),
            "area": freq["Area"],
            "region": freq["Region"],
            "postal_code": postal,
            "veh_power": freq["VehPower"].astype(int),
            "veh_age": freq["VehAge"].astype(int),
            "veh_brand": freq["VehBrand"],
            "veh_gas": freq["VehGas"],
            "density": freq["Density"].astype(int),
            "bonus_malus": freq["BonusMalus"].astype(int),
            "driver_age": freq["DrivAge"].astype(int),
            "annual_premium": premium,
            "last_modified": pd.Timestamp("2024-12-31 23:00:00"),
        }
    )


def build_claims(
    policies: pd.DataFrame, sev: pd.DataFrame, rng: np.random.Generator
) -> pd.DataFrame:
    claims = sev.rename(columns={"IDpol": "policy_id", "ClaimAmount": "claim_amount"}).merge(
        policies[["policy_id", "start_date", "end_date"]], on="policy_id", how="inner"
    )
    span = (pd.to_datetime(claims["end_date"]) - pd.to_datetime(claims["start_date"])).dt.days
    offset = (rng.random(len(claims)) * span.clip(lower=1)).astype(int)
    claim_date = pd.to_datetime(claims["start_date"]) + pd.to_timedelta(offset, unit="D")
    delay = np.minimum(rng.geometric(0.25, len(claims)) - 1, 45)
    declared = claim_date + pd.to_timedelta(delay, unit="D")

    out = pd.DataFrame(
        {
            "claim_id": [f"CLM-{i:08d}" for i in range(1, len(claims) + 1)],
            "policy_id": claims["policy_id"].astype("Int64"),
            "claim_date": claim_date.dt.date,
            "declared_date": declared.dt.date,
            "claim_amount": (claims["claim_amount"] * rng.uniform(0.6, 0.9, len(claims))).round(2),
            "claim_type": rng.choice(CLAIM_TYPES, len(claims), p=CLAIM_TYPE_WEIGHTS),
            "status": "OPEN",
            "updated_at": pd.to_datetime(declared) + pd.Timedelta(hours=9),
        }
    )
    # About 60% of claims are settled later: a CLOSED record with the final amount.
    closed = out.sample(frac=0.6, random_state=7).copy()
    settle = rng.integers(10, 90, len(closed))
    closed["status"] = "CLOSED"
    closed["claim_amount"] = claims.loc[closed.index, "claim_amount"].round(2)
    closed["updated_at"] = closed["updated_at"] + pd.to_timedelta(settle, unit="D")
    return pd.concat([out, closed], ignore_index=True)


def inject_anomalies(
    claims: pd.DataFrame, rng: np.random.Generator
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Inject a small, documented share of bad records (each type < 1%)."""
    df = claims.copy()
    manifest = []

    def pick(frac: float) -> pd.Index:
        return df.sample(frac=frac, random_state=int(rng.integers(0, 1_000_000))).index

    idx = pick(0.003)
    df.loc[idx, "claim_amount"] = -df.loc[idx, "claim_amount"]
    manifest += [(df.at[i, "claim_id"], "invalid_amount") for i in idx]

    idx = pick(0.003)
    df.loc[idx, "policy_id"] = 99_000_000 + np.arange(len(idx))
    manifest += [(df.at[i, "claim_id"], "unknown_policy") for i in idx]

    idx = pick(0.003)
    df.loc[idx, "declared_date"] = [d - timedelta(days=5) for d in df.loc[idx, "claim_date"]]
    manifest += [(df.at[i, "claim_id"], "declared_before_occurrence") for i in idx]

    dups = df.loc[pick(0.01)]
    manifest += [(c, "exact_duplicate") for c in dups["claim_id"]]
    df = pd.concat([df, dups], ignore_index=True)

    return df, pd.DataFrame(manifest, columns=["claim_id", "anomaly"])


def write_policy_updates(policies: pd.DataFrame, out_dir: Path, rng: np.random.Generator) -> int:
    """Monthly SQL scripts: bonus-malus revisions and relocations create SCD2 versions."""
    out_dir.mkdir(parents=True, exist_ok=True)
    count = 0
    for month in range(2, 13):
        run_day = date(2025, month, 1)
        sample = policies.sample(frac=0.02, random_state=month)
        # Demo shortcut: last_modified carries the business effective date so that
        # the point-in-time join of 2025 claims shows the SCD2 effect. In a real
        # system last_modified is the technical timestamp and the effective date
        # is a separate business column. Run the scripts in chronological order.
        effective = f"{run_day.isoformat()}T00:00:00"
        lines = [f"-- Policy changes effective {run_day.isoformat()}"]
        for row in sample.itertuples():
            if rng.random() < 0.7:
                new_bm = int(max(50, row.bonus_malus * (0.95 if rng.random() < 0.8 else 1.25)))
                lines.append(
                    f"UPDATE dbo.policies SET bonus_malus = {new_bm}, "
                    f"last_modified = '{effective}' WHERE policy_id = {row.policy_id};"
                )
            else:
                region = rng.choice(list(REGION_TO_DEPT))
                postal = REGION_TO_DEPT[region] + str(rng.integers(0, 1000)).zfill(3)
                lines.append(
                    f"UPDATE dbo.policies SET region = '{region}', postal_code = '{postal}', "
                    f"last_modified = '{effective}' WHERE policy_id = {row.policy_id};"
                )
            count += 1
        (out_dir / f"updates_{run_day.isoformat()}.sql").write_text("\n".join(lines) + "\n")
    return count


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("--out", default="data", type=Path)
    parser.add_argument("--synthetic", action="store_true", help="offline mode, no OpenML download")
    parser.add_argument("--n-policies", type=int, default=20000, help="synthetic mode only")
    parser.add_argument("--sample", type=int, default=0, help="keep only N policies (real data)")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    rng = np.random.default_rng(args.seed)

    if args.synthetic:
        freq, sev = synthetic_fremtpl2(args.n_policies, rng)
    else:
        freq, sev = load_fremtpl2()
        if args.sample:
            freq = freq.sample(n=args.sample, random_state=args.seed)
            sev = sev[sev["IDpol"].isin(freq["IDpol"])]

    policies = build_policies(freq, rng)
    claims, manifest = inject_anomalies(build_claims(policies, sev, rng), rng)

    pas_dir = args.out / "pas"
    pas_dir.mkdir(parents=True, exist_ok=True)
    policies.to_csv(pas_dir / "policies_initial.csv", index=False)
    n_updates = write_policy_updates(policies, pas_dir / "updates", rng)

    claims_dir = args.out / "claims"
    claims_dir.mkdir(parents=True, exist_ok=True)
    file_day = pd.to_datetime(claims["updated_at"]).dt.date
    for day, chunk in claims.groupby(file_day):
        chunk.to_csv(claims_dir / f"claims_{day.isoformat()}.csv", index=False)
    manifest.to_csv(args.out / "anomalies_manifest.csv", index=False)

    print(
        f"policies: {len(policies):,} | claim records: {len(claims):,} "
        f"| claim files: {file_day.nunique()} | policy updates: {n_updates:,} "
        f"| anomalies: {len(manifest):,}"
    )


if __name__ == "__main__":
    main()
