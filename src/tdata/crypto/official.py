"""Official macro/liquidity data that works without FRED: US Treasury (nominal + real yield curves, debt), NY Fed (SOFR, EFFR, reverse repo,
SOMA holdings), BLS (CPI, unemployment, payrolls), World Bank (annual macro for major economies). Rates + Fed liquidity are the main macro drivers for crypto."""
from __future__ import annotations

import io
import json
from datetime import datetime, timezone

import pandas as pd

from ..collector import Collector, Ctx, register
from ..store import now_utc, p, upsert, write

TREAS = "https://home.treasury.gov/resource-center/data-chart-center/interest-rates/daily-treasury-rates.csv/{y}/all"
NYF = "https://markets.newyorkfed.org/api"


def _date(s):
    return pd.to_datetime(s, errors="coerce").astype("datetime64[ms]")


@register
class Official(Collector):
    name = "macro_official"
    group = "macro"
    title = "Official macro & liquidity (Treasury, NY Fed, BLS, World Bank)"
    description = ("Daily Treasury nominal + TIPS real yield curves, SOFR/EFFR, Fed reverse-repo take-up, SOMA holdings (Fed balance sheet), "
                   "US debt, BLS CPI/unemployment/payrolls, World Bank inflation/GDP for major economies. Replaces FRED where it is unreachable.")
    first_run = "1 min"

    def run(self, ctx: Ctx) -> dict:
        http = ctx.http
        n: dict = {}
        this = datetime.now(timezone.utc).year

        for kind, fname in (("daily_treasury_yield_curve", "yield_curve"), ("daily_treasury_real_yield_curve", "real_yield_curve")):
            frames = []
            for y in range(this - (1 if not ctx.full and (p("macro", "official", f"{fname}.parquet")).exists() else 11), this + 1):
                try:
                    r = http.get(TREAS.format(y=y), params={"type": kind, "field_tdr_date_value": y, "page": "", "_format": "csv"}, timeout=60, retries=1)
                    df = pd.read_csv(io.StringIO(r.text))
                    if len(df):
                        frames.append(df)
                except Exception as e:
                    ctx.info(f"{kind} {y}: {type(e).__name__}")
            if frames:
                df = pd.concat(frames, ignore_index=True)
                df = df.rename(columns={c: c.strip().lower().replace(" ", "").replace("mo", "m").replace("yr", "y").replace("month", "m") for c in df.columns})
                df["date"] = pd.to_datetime(df["date"], format="%m/%d/%Y").astype("datetime64[ms]")
                _, tot = upsert(p("macro", "official", f"{fname}.parquet"), df, ["date"], "date")
                n[fname] = tot

        try:
            sofr = []
            for kind, series in (("secured/sofr", "sofr"), ("unsecured/effr", "effr")):
                j = http.json(f"{NYF}/rates/{kind}/search.json", params={"startDate": "2018-04-03", "endDate": datetime.now(timezone.utc).strftime("%Y-%m-%d")}, timeout=90, retries=1)["refRates"]
                d = pd.DataFrame(j)
                d = d[["effectiveDate", "percentRate", "percentPercentile1", "percentPercentile99", "volumeInBillions"]].copy()
                d.columns = ["date", f"{series}_rate", f"{series}_p1", f"{series}_p99", f"{series}_volume_bn"]
                d["date"] = _date(d["date"])
                for c in d.columns[1:]:
                    d[c] = pd.to_numeric(d[c], errors="coerce")
                sofr.append(d.set_index("date"))
            df = pd.concat(sofr, axis=1).reset_index().sort_values("date")
            _, tot = upsert(p("macro", "official", "policy_rates.parquet"), df, ["date"], "date")
            n["policy_rates"] = tot
        except Exception as e:
            ctx.info(f"NY Fed rates: {type(e).__name__} {str(e)[:70]}")

        try:
            start = "2021-01-01"
            ops = []
            y0 = int(start[:4])
            for y in range(y0, this + 1):
                j = http.json(f"{NYF}/rp/reverserepo/propositions/search.json", params={"startDate": f"{y}-01-01", "endDate": f"{y}-12-31"}, timeout=60, retries=1)
                for o in (j.get("repo") or {}).get("operations", []):
                    ops.append({"date": o.get("operationDate"), "accepted_bn": float(o.get("totalAmtAccepted") or 0) / 1e9, "submitted_bn": float(o.get("totalAmtSubmitted") or 0) / 1e9,
                                "participants": o.get("participatingCpty")})
            if ops:
                d = pd.DataFrame(ops)
                d["date"] = _date(d["date"])
                _, tot = upsert(p("macro", "official", "reverse_repo.parquet"), d, ["date"], "date")
                n["reverse_repo"] = tot
        except Exception as e:
            ctx.info(f"NY Fed RRP: {type(e).__name__} {str(e)[:70]}")

        try:
            j = http.json(f"{NYF}/soma/summary.json", timeout=90, retries=1)["soma"]["summary"]
            d = pd.DataFrame(j)
            d["date"] = _date(d.pop("asOfDate"))
            for c in d.columns:
                if c != "date":
                    d[c] = pd.to_numeric(d[c], errors="coerce") / 1e9
            write(d.sort_values("date"), p("macro", "official", "soma_holdings_bn.parquet"))
            n["soma"] = len(d)
        except Exception as e:
            ctx.info(f"SOMA: {type(e).__name__} {str(e)[:70]}")

        try:
            rows, page = [], 1
            while True:
                j = http.json("https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v2/accounting/od/debt_to_penny",
                              params={"filter": "record_date:gte:2018-01-01", "sort": "record_date", "page[size]": 10000, "page[number]": page}, timeout=60, retries=1)
                rows += j["data"]
                if page >= int(j["meta"]["total-pages"]):
                    break
                page += 1
            d = pd.DataFrame(rows)[["record_date", "debt_held_public_amt", "intragov_hold_amt", "tot_pub_debt_out_amt"]]
            d["date"] = _date(d.pop("record_date"))
            for c in d.columns:
                if c != "date":
                    d[c] = pd.to_numeric(d[c], errors="coerce") / 1e12
            d.columns = ["debt_held_public_tn", "intragov_tn", "total_debt_tn", "date"]
            _, tot = upsert(p("macro", "official", "us_debt.parquet"), d, ["date"], "date")
            n["us_debt"] = tot
        except Exception as e:
            ctx.info(f"Fiscal data: {type(e).__name__} {str(e)[:70]}")

        try:
            ids = {"CUSR0000SA0": "cpi_u_sa", "CUSR0000SA0L1E": "core_cpi_sa", "LNS14000000": "unemployment_rate", "CES0000000001": "nonfarm_payrolls_k"}
            body = {"seriesid": list(ids), "startyear": str(this - 9), "endyear": str(this)}
            j = http.post("https://api.bls.gov/publicAPI/v1/timeseries/data/", json=body, timeout=60, retries=1).json()
            recs = []
            for s in j["Results"]["series"]:
                for o in s["data"]:
                    if o["period"].startswith("M") and o["period"] != "M13":
                        try:
                            v = float(o["value"])
                        except ValueError:  # '-' = not yet published
                            continue
                        recs.append({"date": pd.Timestamp(int(o["year"]), int(o["period"][1:]), 1), "series": ids[s["seriesID"]], "value": v})
            if recs:
                d = pd.DataFrame(recs).pivot(index="date", columns="series", values="value").reset_index()
                d["date"] = d["date"].astype("datetime64[ms]")
                _, tot = upsert(p("macro", "official", "bls_monthly.parquet"), d, ["date"], "date")
                n["bls_months"] = tot
        except Exception as e:
            ctx.info(f"BLS: {type(e).__name__} {str(e)[:70]}")

        try:
            ind = {"FP.CPI.TOTL.ZG": "inflation_cpi_pct", "NY.GDP.MKTP.KD.ZG": "gdp_growth_pct", "NY.GDP.PETR.RT.ZS": "oil_rents_pct_gdp", "NY.GDP.MKTP.CD": "gdp_usd", "BN.CAB.XOKA.GD.ZS": "current_account_pct_gdp"}
            recs = []
            for code, name in ind.items():
                j = http.json(f"https://api.worldbank.org/v2/country/USA;CHN;JPN;DEU;GBR;FRA;IND;BRA;CAN;KOR;AUS;CHE/indicator/{code}", params={"format": "json", "per_page": 2000, "date": "2005:2026"}, timeout=60, retries=1)
                for o in j[1] or []:
                    if o["value"] is not None:
                        recs.append({"country": o["countryiso3code"], "year": int(o["date"]), "indicator": name, "value": float(o["value"])})
            if recs:
                write(pd.DataFrame(recs), p("macro", "official", "worldbank_annual.parquet"))
                n["worldbank_rows"] = len(recs)
        except Exception as e:
            ctx.info(f"World Bank: {type(e).__name__} {str(e)[:70]}")
        return n
