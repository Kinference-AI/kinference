#!/usr/bin/env python3
"""Refit a kinference profile's latency-predictor coefficients from latency.csv.

kinference appends one row per pick to the file that
KINFERENCE_OBSERVATIONS_CSV names: the predicted TTFT, its terms, the
observed TTFT and the predictor config that produced the prediction. For
each profile in the file, this script fits the predictor's coefficients to
the observed TTFTs, checks the fit on held-out rows, and prints values to
paste into the profile.

The router predicts (crates/kinference-router/src/picker.rs):

    predicted = base_ms
              + a * U + c * U * T
              + inflight
              + rate * max(0, q_waiting - q_inflight)
              + cache_pressure_penalty_ms * kv_cache_usage
              + q_running_penalty_peak_ms * tent(E)
              + forward_pass_ms * E

where U is the request's uncached blocks, T its total blocks,
E = q_running + q_inflight, a and c are the attention_uncached_* knobs,
rate is queue_ms_per_entry (saturated_cold_ms_per_entry when the
cold-saturation gate fires), and inflight is the remaining prefill of the
worker's in-flight picks. Every knob enters linearly, so one robust
non-negative least-squares fit covers them jointly. The in-flight term
scales with a and c; the fit rescales its recorded value by the change in
the prefill estimate of an average request.

The CSV doesn't record each in-flight pick's progress, so
expected_overlap_alpha and inflight_concurrent_capacity can't be refit;
the report gives a diagnostic for alpha instead.

Usage:
    analyze-latency.py latency.csv [more.csv ...]
    analyze-latency.py latency.csv --profile default --routing

Needs pandas and numpy.
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class Knob:
    name: str
    digits: int
    unit: str


# Fit order. `base_ms` is also the name of the CSV column that records the
# base term, which always equals the knob.
KNOBS = [
    Knob("base_ms", 0, "ms"),
    Knob("attention_uncached_block_ms", 3, "ms/block"),
    Knob("attention_uncached_per_every_block_ms", 5, "ms/block^2"),
    Knob("queue_ms_per_entry", 1, "ms/entry"),
    Knob("saturated_cold_ms_per_entry", 0, "ms/entry"),
    Knob("cache_pressure_penalty_ms", 0, "ms"),
    Knob("forward_pass_ms", 2, "ms/request"),
    Knob("q_running_penalty_peak_ms", 0, "ms"),
]
KNOB_BY_NAME = {k.name: k for k in KNOBS}

# Config columns that identify one predictor-config snapshot.
CONFIG_COLS = [k.name for k in KNOBS] + [
    "q_running_penalty_peak_qr",
    "q_running_penalty_falloff_qr",
    "cache_pressure_saturated_threshold",
    "inflight_concurrent_capacity",
    "expected_overlap_alpha",
]

REQUIRED_COLS = [
    "unix_ms", "name", "status", "predicted_ms", "base_ms", "q_ms",
    "q_rate_ms_per_entry", "prefill_ms", "cache_ms", "observed_ms",
    "residual_ms", "overlap_blocks", "total_blocks", "q_waiting",
    "q_inflight", "q_running", "kv_cache_usage", "compl_per_s", "kv_blocks",
    "queue_ms_per_entry", "cache_pressure_penalty_ms",
    "saturated_cold_ms_per_entry", "cache_pressure_saturated_threshold",
    "attention_uncached_block_ms", "attention_uncached_per_every_block_ms",
    "q_running_ms", "decode_contention_ms", "q_running_penalty_peak_ms",
    "q_running_penalty_peak_qr", "q_running_penalty_falloff_qr",
    "inflight_concurrent_capacity", "forward_pass_ms",
    "expected_overlap_alpha",
]

# Least share of rows on which a term must be nonzero for its knob to be fit.
MIN_EXERCISED = 0.02

# Below this many uncached blocks in the longest prompt, prefill is too small
# to separate from noise.
MIN_PREFILL_BLOCKS = 64


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------

def load(paths: list[str]) -> pd.DataFrame:
    frames = []
    for path in paths:
        with open(path) as f:
            first = f.readline()
        if not first.startswith("unix_ms,"):
            sys.exit(f"{path}: no header row; this script reads the CSV that "
                     "kinference writes with a header (52 columns).")
        df = pd.read_csv(path, dtype={"request_id": str, "x_request_id": str,
                                      "inflight_overlap_blocks": str,
                                      "name": str, "endpoint": str})
        missing = [c for c in REQUIRED_COLS if c not in df.columns]
        if missing:
            sys.exit(f"{path}: missing columns {missing}; the CSV is from a "
                     "kinference version this script doesn't read.")
        for col in ("workerset", "profile"):
            if col not in df.columns:
                df[col] = "(unrecorded)"
        df["source"] = path
        frames.append(df)
    df = pd.concat(frames, ignore_index=True)
    for col in ("workerset", "profile", "x_request_id",
                "inflight_overlap_blocks"):
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str)
    if "replication_fired" in df.columns:
        df["replication_fired"] = (df["replication_fired"].astype(str)
                                   .str.lower() == "true")
    df = df.sort_values("unix_ms", kind="stable").reset_index(drop=True)
    return add_terms(df)


def tent(e: np.ndarray, peak_qr, falloff_qr) -> np.ndarray:
    """The q_running penalty's shape, 0..1, as `q_running_penalty_ms` clamps it."""
    e = np.asarray(e, dtype=float)
    peak = np.maximum(np.asarray(peak_qr, dtype=float), 1.0)
    fall = np.maximum(np.asarray(falloff_qr, dtype=float), peak + 1.0)
    rising = e / peak
    falling = (fall - e) / (fall - peak)
    out = np.where(e <= peak, rising, falling)
    return np.where((e <= 0) | (e >= fall), 0.0, out)


def add_terms(df: pd.DataFrame) -> pd.DataFrame:
    """Add the per-row inputs of each predictor term."""
    df["ts"] = pd.to_datetime(df["unix_ms"], unit="ms", utc=True)
    df["U"] = (df["total_blocks"] - df["overlap_blocks"]).clip(lower=0)
    df["T"] = df["total_blocks"]
    # The predictor counts the worker's tracked dispatches, which are the
    # entries of inflight_overlap_blocks. q_inflight is a different counter:
    # picks not yet observed as first-token.
    df["dispatches"] = df["inflight_overlap_blocks"].map(
        lambda s: len([p for p in s.split(";") if p]))
    df["E"] = df["q_running"] + df["dispatches"]
    df["unknown_q"] = (df["q_waiting"] - df["dispatches"]).clip(lower=0)
    # The recorded rate differs from queue_ms_per_entry only when the
    # cold-saturation gate fired.
    df["gate_fired"] = (df["q_rate_ms_per_entry"]
                        - df["queue_ms_per_entry"]).abs() > 0.05
    # The gate's conditions other than "saturated rate > normal rate", so the
    # fit can price gated entries separately.
    df["gate_eligible"] = ((df["compl_per_s"] == 0)
                           & (df["kv_cache_usage"]
                              >= df["cache_pressure_saturated_threshold"])
                           & (df["q_waiting"] > 0))
    df["inflight_ms"] = (df["q_ms"]
                         - df["unknown_q"] * df["q_rate_ms_per_entry"]
                         ).clip(lower=0)
    df["tent"] = tent(df["E"], df["q_running_penalty_peak_qr"],
                      df["q_running_penalty_falloff_qr"])
    return df


def reconstruct(df: pd.DataFrame) -> pd.Series:
    """predicted_ms rebuilt from each row's own config and inputs."""
    return (df["base_ms"]
            + df["attention_uncached_block_ms"] * df["U"]
            + df["attention_uncached_per_every_block_ms"] * df["U"] * df["T"]
            + df["inflight_ms"]
            + df["unknown_q"] * df["q_rate_ms_per_entry"]
            + df["cache_pressure_penalty_ms"] * df["kv_cache_usage"]
            + df["q_running_penalty_peak_ms"] * df["tent"]
            + df["forward_pass_ms"] * df["E"])


# ---------------------------------------------------------------------------
# Fitting
# ---------------------------------------------------------------------------

def nnls(a: np.ndarray, b: np.ndarray) -> np.ndarray:
    """Lawson-Hanson non-negative least squares: argmin |ax - b|, x >= 0."""
    m, n = a.shape
    x = np.zeros(n)
    passive = np.zeros(n, dtype=bool)
    tol = 10 * np.finfo(float).eps * np.abs(a).sum(axis=0).max() * max(m, n)
    w = a.T @ b
    for _ in range(30 * n):
        if passive.all() or w[~passive].max() <= tol:
            break
        free = np.flatnonzero(~passive)
        passive[free[np.argmax(w[free])]] = True
        while True:
            z = np.zeros(n)
            z[passive] = np.linalg.lstsq(a[:, passive], b, rcond=None)[0]
            if (z[passive] > tol).all():
                break
            bad = passive & (z <= tol)
            step = np.min(x[bad] / (x[bad] - z[bad]))
            x = x + step * (z - x)
            passive &= x > tol
            if not passive.any():
                z = np.zeros(n)
                break
        x = z
        w = a.T @ (b - a @ x)
    return x


def robust_scale(r: np.ndarray) -> float:
    s = 1.4826 * np.median(np.abs(r - np.median(r)))
    return float(s) if s > 0 else float(np.std(r)) or 1.0


def huber_nnls(a: np.ndarray, b: np.ndarray,
               delta: float | None = None) -> np.ndarray:
    """Non-negative Huber regression by iteratively reweighted NNLS.

    Huber rather than squared loss because TTFT tails run to minutes under
    overload and would otherwise set every coefficient.
    """
    norms = np.sqrt((a ** 2).mean(axis=0))
    norms[norms == 0] = 1.0
    a = a / norms
    x = nnls(a, b)
    for _ in range(50):
        r = b - a @ x
        d = delta if delta is not None else 1.345 * robust_scale(r)
        w = np.sqrt(np.where(np.abs(r) <= d, 1.0, d / np.maximum(np.abs(r), 1e-9)))
        x_new = nnls(a * w[:, None], b * w)
        if np.allclose(x_new, x, rtol=1e-6, atol=1e-9):
            x = x_new
            break
        x = x_new
    return x / norms


def huber_loss(r: np.ndarray, delta: float) -> float:
    ar = np.abs(r)
    return float(np.where(ar <= delta, 0.5 * ar ** 2,
                          delta * (ar - 0.5 * delta)).mean())


@dataclass
class Design:
    """The predictor's terms for one profile's rows, one column per knob."""
    x: pd.DataFrame
    # Recorded in-flight cost on rows whose config had no prefill estimate
    # to rescale.
    offset: np.ndarray


def design(d: pd.DataFrame, peak_qr: int, falloff_qr: int,
           mean_u: float, mean_ut: float) -> Design:
    # The in-flight term is the remaining prefill of other requests, priced
    # with the row's a0 and c0. Rescale it as a * mean_u + c * mean_ut over
    # a0 * mean_u + c0 * mean_ut, which is linear in a and c.
    ref = (d["attention_uncached_block_ms"] * mean_u
           + d["attention_uncached_per_every_block_ms"] * mean_ut).to_numpy()
    inflight = d["inflight_ms"].to_numpy()
    has_ref = ref > 0
    safe = np.where(has_ref, ref, 1.0)
    inflight_a = np.where(has_ref, inflight * mean_u / safe, 0.0)
    inflight_c = np.where(has_ref, inflight * mean_ut / safe, 0.0)
    gate = d["gate_eligible"].to_numpy()
    uq = d["unknown_q"].to_numpy(dtype=float)
    u = d["U"].to_numpy(dtype=float)
    t = d["T"].to_numpy(dtype=float)
    e = d["E"].to_numpy(dtype=float)
    x = pd.DataFrame({
        "base_ms": np.ones(len(d)),
        "attention_uncached_block_ms": u + inflight_a,
        "attention_uncached_per_every_block_ms": u * t + inflight_c,
        "queue_ms_per_entry": np.where(gate, 0.0, uq),
        "saturated_cold_ms_per_entry": np.where(gate, uq, 0.0),
        "cache_pressure_penalty_ms": d["kv_cache_usage"].to_numpy(dtype=float),
        "forward_pass_ms": e,
        "q_running_penalty_peak_ms": tent(e, peak_qr, falloff_qr),
    }, index=d.index)
    return Design(x, np.where(has_ref, 0.0, inflight))


def fit(des: Design, y: np.ndarray, free: list[str], fixed: dict[str, float],
        delta: float | None = None) -> dict[str, float]:
    """Fit the `free` knobs with the others held at `fixed`."""
    held = [k for k in des.x.columns if k not in free]
    b = y - des.offset - sum((des.x[k].to_numpy() * fixed[k] for k in held),
                             np.zeros(len(y)))
    coef = dict(fixed)
    if free:
        sol = huber_nnls(des.x[free].to_numpy(), b, delta)
        coef.update(zip(free, sol))
    return coef


def predict(des: Design, coef: dict[str, float]) -> np.ndarray:
    return des.offset + sum(des.x[k].to_numpy() * coef[k] for k in des.x.columns)


# ---------------------------------------------------------------------------
# Per-profile analysis
# ---------------------------------------------------------------------------

@dataclass
class Verdict:
    knob: str
    current: float
    estimate: float
    lo: float
    hi: float
    action: str  # "keep" or "change"
    reason: str


def fmt_knob(name: str, v: float) -> str:
    return f"{v:.{KNOB_BY_NAME[name].digits}f}"


def spearman(a, b) -> float:
    """Rank correlation; pandas' own needs scipy."""
    return float(pd.Series(np.asarray(a, dtype=float)).rank().corr(
        pd.Series(np.asarray(b, dtype=float)).rank()))


def metrics(observed: np.ndarray, predicted: np.ndarray) -> dict[str, float]:
    r = observed - predicted
    return {
        "median residual": float(np.median(r)),
        "median |residual|": float(np.median(np.abs(r))),
        "p90 |residual|": float(np.quantile(np.abs(r), 0.9)),
        "rank correlation": spearman(predicted, observed),
    }


def exercised(col: np.ndarray) -> tuple[bool, str]:
    nonzero = float((col != 0).mean())
    if np.ptp(col) == 0:
        return False, "constant in this data"
    if nonzero < MIN_EXERCISED:
        return False, f"nonzero on only {100 * nonzero:.1f}% of rows"
    return True, ""


def analyze_profile(name: str, d: pd.DataFrame, args) -> None:
    print(f"\n{'=' * 78}\nProfile {name!r}\n{'=' * 78}")
    d = d.reset_index(drop=True)
    workers = d["name"].nunique()
    sets = ", ".join(sorted(d["workerset"].unique()))
    print(f"{len(d)} rows, {workers} workers, worker sets: {sets}")
    print(f"{d['ts'].iloc[0]:%Y-%m-%d %H:%M} to {d['ts'].iloc[-1]:%Y-%m-%d %H:%M} UTC")

    current = d.iloc[-1]
    snapshots = d.groupby(CONFIG_COLS, sort=False).size()
    if len(snapshots) > 1:
        print(f"{len(snapshots)} predictor configs appear in these rows; "
              "'current' below means the latest. All rows feed the fit.")

    recon_err = (reconstruct(d) - d["predicted_ms"]).abs()
    bad = float((recon_err > 2.0).mean())
    if bad > 0.01:
        print(f"\nWARNING: on {100 * bad:.1f}% of rows this script's predictor "
              "doesn't match the recorded predicted_ms (worst off by "
              f"{recon_err.max():.0f} ms). The CSV may come from a kinference "
              "version with a different predictor; treat the refit with care.")

    obs = d["observed_ms"].to_numpy(dtype=float)
    print("\nObserved TTFT and current prediction (ms):")
    print(f"  {'':12s}{'p10':>9s}{'p50':>9s}{'p90':>9s}{'p99':>9s}")
    for label, s in (("observed", d["observed_ms"]),
                     ("predicted", d["predicted_ms"])):
        q = s.quantile([0.1, 0.5, 0.9, 0.99])
        print(f"  {label:12s}" + "".join(f"{v:>9.0f}" for v in q))

    mean_u = float(d["U"].mean())
    mean_ut = float((d["U"] * d["T"]).mean())
    cur = {k.name: float(current[k.name]) for k in KNOBS}
    cur_peak_qr = int(current["q_running_penalty_peak_qr"])
    cur_falloff_qr = int(current["q_running_penalty_falloff_qr"])

    # Which knobs does this data exercise at all?
    base_des = design(d, cur_peak_qr, cur_falloff_qr, mean_u, mean_ut)
    unexercised: dict[str, str] = {}
    for k in KNOBS[1:]:
        ok, why = exercised(base_des.x[k.name].to_numpy())
        if not ok:
            unexercised[k.name] = why
    if "attention_uncached_block_ms" not in unexercised and d["U"].max() < MIN_PREFILL_BLOCKS:
        why = (f"prompts have at most {int(d['U'].max())} uncached blocks, "
               "too few to measure prefill cost")
        unexercised["attention_uncached_block_ms"] = why
        unexercised["attention_uncached_per_every_block_ms"] = why
    free_all = [k.name for k in KNOBS if k.name not in unexercised]

    # Choose the q_running tent's shape by grid search; its peak is linear.
    peak_qr, falloff_qr = cur_peak_qr, cur_falloff_qr
    if "q_running_penalty_peak_ms" not in unexercised:
        delta = 1.345 * robust_scale(obs - d["predicted_ms"].to_numpy())
        best = None
        for pq in range(1, 9):
            for fq in range(pq + 1, pq + 17):
                des = design(d, pq, fq, mean_u, mean_ut)
                coef = fit(des, obs, free_all, cur, delta)
                loss = huber_loss(obs - predict(des, coef), delta)
                if best is None or loss < best[0]:
                    best = (loss, pq, fq, coef["q_running_penalty_peak_ms"])
        if best[3] > 0:
            peak_qr, falloff_qr = best[1], best[2]
    des = design(d, peak_qr, falloff_qr, mean_u, mean_ut)

    # Joint fit of every exercised knob, with bootstrap intervals.
    est = fit(des, obs, free_all, cur)
    rng = np.random.default_rng(args.seed)
    boots = []
    for _ in range(args.bootstrap):
        idx = rng.integers(0, len(d), len(d))
        sub = Design(des.x.iloc[idx], des.offset[idx])
        boots.append(fit(sub, obs[idx], free_all, cur))

    verdicts = {}
    for k in KNOBS:
        n = k.name
        if n in unexercised:
            verdicts[n] = Verdict(n, cur[n], np.nan, np.nan, np.nan, "keep",
                                  f"not exercised: {unexercised[n]}")
            continue
        vals = np.array([b[n] for b in boots]) if boots else np.array([est[n]])
        lo, hi = np.quantile(vals, [0.1, 0.9])
        if lo <= cur[n] <= hi:
            verdicts[n] = Verdict(n, cur[n], est[n], lo, hi, "keep",
                                  "current value is within the interval")
        else:
            verdicts[n] = Verdict(n, cur[n], est[n], lo, hi, "change", "")

    # Held-out check on random 10-minute blocks, so every load regime in the
    # data lands on both sides of the split.
    blocks = d["unix_ms"] // 600_000
    ublocks = blocks.unique()
    test_blocks = ublocks[rng.random(len(ublocks)) < args.holdout]
    test = blocks.isin(test_blocks).to_numpy()
    if test.all() or not test.any():
        test = np.arange(len(d)) >= int(len(d) * (1 - args.holdout))
    split = Split(des, obs, ~test, test)
    m_cur = metrics(obs[test], d["predicted_ms"].to_numpy()[test])

    candidates = [n for n, v in verdicts.items() if v.action == "change"]
    free, best = select_knobs(split, candidates, cur, m_cur)
    for n in (n for n in candidates if n not in free):
        v = verdicts[n]
        v.action = "keep"
        v.reason = (f"a joint fit puts it at {fmt_knob(n, v.lo)} .. "
                    f"{fmt_knob(n, v.hi)}, but changing it doesn't improve "
                    "held-out error")

    final = fit(des, obs, free, cur)
    final_boots = []
    for _ in range(args.bootstrap if free else 0):
        idx = rng.integers(0, len(d), len(d))
        final_boots.append(fit(Design(des.x.iloc[idx], des.offset[idx]),
                               obs[idx], free, cur))

    print(f"\nHeld-out check: {int(test.sum())} rows in random 10-minute "
          f"blocks ({100 * args.holdout:.0f}% of blocks), predicted by a fit "
          "on the other rows.")
    print("Residual = observed - predicted; positive means the predictor "
          "was too optimistic.")
    print_metrics({"current": m_cur, "recommended": best})

    print("\nCoefficients. The recommended values are refit with every other "
          "knob at its current value; the interval is their 10th-90th "
          f"percentile over {args.bootstrap} bootstrap resamples.")
    print(f"  {'knob':40s}{'current':>11s}{'recommended':>13s}  interval")
    for v in verdicts.values():
        n = v.knob
        if v.action == "change":
            new = fmt_knob(n, final[n])
            lo, hi = np.quantile([b[n] for b in final_boots], [0.1, 0.9])
            interval = f"{fmt_knob(n, lo)} .. {fmt_knob(n, hi)}"
        else:
            new, interval = "keep", ""
        print(f"  {n:40s}{fmt_knob(n, v.current):>11s}{new:>13s}  {interval}")
        if v.action == "keep":
            print(f"  {'':42s}{v.reason}")
    tent_changed = (final["q_running_penalty_peak_ms"] > 0
                    and (peak_qr, falloff_qr) != (cur_peak_qr, cur_falloff_qr))
    if tent_changed:
        print(f"  {'q_running_penalty_peak_qr':40s}{cur_peak_qr:>11d}{peak_qr:>13d}")
        print(f"  {'q_running_penalty_falloff_qr':40s}{cur_falloff_qr:>11d}{falloff_qr:>13d}")
    if ("saturated_cold_ms_per_entry" in free
            and final["saturated_cold_ms_per_entry"] <= final["queue_ms_per_entry"]):
        print("  The cold-saturation rate is no higher than queue_ms_per_entry, "
              "so the gate won't fire with these values.")

    if "base_ms" in free and final["base_ms"] > 2 * cur["base_ms"]:
        print("  base_ms adds the same amount to every worker, so it doesn't "
              "change which worker the picker chooses. It does change "
              "predicted TTFT and how much latency replication_threshold_pct "
              "allows. A large base_ms often stands in for load the other "
              "terms miss; see below.")

    if not free:
        print("\nNo knob change improves held-out error; keep the current "
              "config. See below for what the predictor can't capture.")
    else:
        print(f"\nPaste into the Helm values under config.profiles.{name}:")
        print("  predictor:")
        for k in KNOBS:
            note = (f"  # was {fmt_knob(k.name, cur[k.name])}"
                    if k.name in free else "")
            print(f"    {k.name}: {fmt_knob(k.name, final[k.name])}{note}")
        if tent_changed:
            print(f"    q_running_penalty_peak_qr: {peak_qr}  # was {cur_peak_qr}")
            print(f"    q_running_penalty_falloff_qr: {falloff_qr}  # was {cur_falloff_qr}")

    report_gaps(d, split, free_all, free, cur, best)
    if args.routing:
        report_routing(d)


@dataclass
class Split:
    des: Design
    obs: np.ndarray
    train: np.ndarray
    test: np.ndarray

    def part(self, mask: np.ndarray, extra: dict[str, np.ndarray]) -> Design:
        x = self.des.x[mask].copy()
        for col, v in extra.items():
            x[col] = v[mask]
        return Design(x, self.des.offset[mask])

    def predict_test(self, free: list[str], fixed: dict[str, float],
                     extra: dict[str, np.ndarray] = {}) -> np.ndarray:
        """Test-row predictions from a fit of `free` on the training rows."""
        fixed = {**fixed, **{col: 0.0 for col in extra}}
        coef = fit(self.part(self.train, extra), self.obs[self.train],
                   free, fixed)
        return predict(self.part(self.test, extra), coef)

    def score(self, free: list[str], fixed: dict[str, float],
              extra: dict[str, np.ndarray] = {}) -> dict[str, float]:
        return metrics(self.obs[self.test], self.predict_test(free, fixed, extra))


def better(m: dict[str, float], than: dict[str, float]) -> bool:
    """Lower held-out median |residual| by 2%, without losing rank
    correlation, which is what the picker uses."""
    return (m["median |residual|"] < 0.98 * than["median |residual|"]
            and m["rank correlation"] >= than["rank correlation"] - 0.01)


def select_knobs(split: Split, candidates: list[str], cur: dict[str, float],
                 m_cur: dict[str, float]) -> tuple[list[str], dict[str, float]]:
    """The candidates worth changing, and their held-out metrics.

    Starts from all of them and drops each one whose removal doesn't make
    held-out error worse, since knobs often only help together. If even the
    full set loses to the current config, adds knobs one at a time instead.
    """
    free = list(candidates)
    best = split.score(free, cur) if free else m_cur
    if free and better(best, m_cur):
        while len(free) > 1:
            trials = [(split.score([k for k in free if k != n], cur), n)
                      for n in free]
            m, n = min(trials, key=lambda t: t[0]["median |residual|"])
            if better(best, m):
                break
            free.remove(n)
            best = m
        return free, best

    free, best = [], m_cur
    rest = list(candidates)
    while rest:
        trials = [(split.score(free + [n], cur), n) for n in rest]
        m, n = min(trials, key=lambda t: t[0]["median |residual|"])
        if not better(m, best):
            break
        free.append(n)
        rest.remove(n)
        best = m
    return free, best


def print_metrics(cols: dict[str, dict[str, float]]) -> None:
    print(f"  {'':22s}" + "".join(f"{c:>16s}" for c in cols))
    for key in next(iter(cols.values())):
        f = "{:>16.2f}" if key == "rank correlation" else "{:>16.0f}"
        print(f"  {key:22s}" + "".join(f.format(m[key]) for m in cols.values()))


def report_gaps(d: pd.DataFrame, split: Split, free_all: list[str],
                free: list[str], cur: dict[str, float],
                best: dict[str, float]) -> None:
    """What no knob can fix: structural gaps in the predictor, sized on the
    held-out rows."""
    print("\nWhat the predictor can't capture (no knob changes these):")

    # Every waiting request, not only those kinference didn't route.
    waiting = d["q_waiting"].to_numpy(dtype=float)
    if (waiting > 0).mean() >= MIN_EXERCISED:
        extra = {"all_waiting": waiting}
        m = split.score(free + ["all_waiting"], cur, extra)
        coef = fit(split.part(split.train | split.test, extra), split.obs,
                   free + ["all_waiting"], {**cur, "all_waiting": 0.0})
        print(f"\n  Waiting requests. queue_ms_per_entry only prices waiting "
              "requests that kinference didn't route: q_waiting minus its "
              f"tracked dispatches, nonzero on "
              f"{100 * (d['unknown_q'] > 0).mean():.1f}% of rows, while "
              f"q_waiting > 0 on {100 * (waiting > 0).mean():.0f}%. Tracked "
              "dispatches cost only their remaining prefill.")
        if better(m, best):
            print("  A per-entry cost on every waiting request fits at "
                  f"{coef['all_waiting']:.0f} ms/entry and would change "
                  "held-out error to:")
            print_metrics({"recommended": best, "with that term": m})
        else:
            print("  A per-entry cost on every waiting request doesn't "
                  "improve held-out error.")

    # A per-worker offset learned from the training rows.
    pred = split.predict_test(free, cur)
    train_r = pd.Series(split.obs[split.train] - predict(
        split.part(split.train, {}), fit(split.part(split.train, {}),
                                         split.obs[split.train], free, cur)))
    names = d["name"].to_numpy()
    offsets = train_r.groupby(names[split.train]).median()
    shifted = pred + pd.Series(names[split.test]).map(offsets).fillna(0).to_numpy()
    m = metrics(split.obs[split.test], shifted)
    counts = pd.Series(names[split.train]).value_counts()
    med = offsets[counts[counts >= 30].index]
    if better(m, best):
        print(f"\n  Per-worker speed. Each worker's median residual ranges "
              f"from {med.min():+.0f} to {med.max():+.0f} ms, and the "
              "predictor has no per-worker term; check node and GPU "
              "placement. A per-worker offset would change held-out error to:")
        print_metrics({"recommended": best, "with offsets": m})
    else:
        print("\n  Per-worker speed. Workers' median residuals range from "
              f"{med.min():+.0f} to {med.max():+.0f} ms, but a per-worker "
              "offset doesn't improve held-out error.")

    credited = d["overlap_blocks"] > d["kv_blocks"]
    peers = (d["dispatches"] > 0) & ~credited
    if credited.sum() >= 30 and peers.sum() >= 30:
        r = pd.Series(d["observed_ms"].to_numpy() - predict(
            split.des, fit(split.des, split.obs, free, cur)), index=d.index)
        gap = r[credited].median() - r[peers].median()
        alpha = d["expected_overlap_alpha"].iloc[-1]
        print(f"\n  expected_overlap_alpha ({alpha:.2f}) can't be refit: the "
              "CSV doesn't record in-flight progress. Rows where in-flight "
              f"picks raised the expected cache overlap have a median residual "
              f"{gap:+.0f} ms from other rows with in-flight picks.")
        if gap > 200:
            print("  That credit looks too generous; try a lower alpha.")


def report_routing(d: pd.DataFrame) -> None:
    """Routing diagnostics: load spread and the replication override."""
    print("\nRouting (these depend on picker knobs, not the predictor):")
    last = d.iloc[-1]
    print(f"  picker: top_k_min={int(last['top_k_min'])} "
          f"top_k_fraction={last['top_k_fraction']:.2f} "
          f"tie_tolerance_ms={last['tie_tolerance_ms']:.0f}")
    print(f"\n  {'worker':44s}{'picks':>7s}{'p50':>8s}{'p95':>8s}{'max':>8s}"
          f"{'peak/s':>8s}")
    sec = d["ts"].dt.floor("s")
    for w, g in d.groupby("name"):
        o = g["observed_ms"]
        peak = int(g.groupby(sec[g.index]).size().max())
        print(f"  {w[-44:]:44s}{len(g):>7d}{o.median():>8.0f}"
              f"{o.quantile(0.95):>8.0f}{o.max():>8.0f}{peak:>8d}")
    share = d["name"].value_counts(normalize=True)
    print(f"  Busiest worker took {100 * share.iloc[0]:.0f}% of picks; an even "
          f"split would be {100 / len(share):.0f}%.")

    if "replication_fired" in d.columns:
        fired = d["replication_fired"]
        print(f"\n  Replication override fired on {int(fired.sum())} of "
              f"{len(d)} picks (replication_threshold_pct="
              f"{last['replication_threshold_pct']:.0f}, "
              f"replication_warm_target={int(last['replication_warm_target'])}).")
        if fired.sum() >= 10:
            gap = (d.loc[fired, "residual_ms"].median()
                   - d.loc[~fired, "residual_ms"].median())
            print(f"  Replicated picks have a median residual {gap:+.0f} ms "
                  "from other picks; a large positive gap means the cold "
                  "worker was slower than predicted.")


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("csv", nargs="+", help="latency CSV(s) from kinference")
    p.add_argument("--profile", help="analyze only this profile")
    p.add_argument("--holdout", type=float, default=0.3,
                   help="share of the latest rows held out for the check "
                        "(default 0.3)")
    p.add_argument("--bootstrap", type=int, default=50,
                   help="bootstrap resamples for intervals (default 50)")
    p.add_argument("--seed", type=int, default=0)
    p.add_argument("--min-rows", type=int, default=200,
                   help="skip profiles with fewer usable rows (default 200)")
    p.add_argument("--routing", action="store_true",
                   help="also print routing diagnostics")
    args = p.parse_args()
    if not 0 < args.holdout < 1:
        p.error("--holdout must be between 0 and 1")

    df = load(args.csv)
    ok = df[df["status"] == 200]
    print(f"{len(df)} rows; {len(df) - len(ok)} with a non-200 status are "
          "left out because their observed_ms isn't a TTFT.")
    profiles = sorted(ok["profile"].unique())
    if args.profile:
        if args.profile not in profiles:
            sys.exit(f"profile {args.profile!r} not in the CSV; it has "
                     f"{profiles}")
        profiles = [args.profile]
    for name in profiles:
        d = ok[ok["profile"] == name]
        if len(d) < args.min_rows:
            print(f"\nProfile {name!r}: only {len(d)} rows; skipped "
                  f"(--min-rows {args.min_rows}).")
            continue
        analyze_profile(name, d, args)
    return 0


if __name__ == "__main__":
    sys.exit(main())
