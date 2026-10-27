"""Tests for analyze-latency.py. Run with `python3 -m unittest` from analysis/."""

import contextlib
import importlib.util
import io
import os
import re
import sys
import tempfile
import unittest

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
_spec = importlib.util.spec_from_file_location(
    "analyze_latency", os.path.join(HERE, "analyze-latency.py"))
al = importlib.util.module_from_spec(_spec)
sys.modules["analyze_latency"] = al
_spec.loader.exec_module(al)

CURRENT = {
    "base_ms": 400.0,
    "attention_uncached_block_ms": 2.3,
    "attention_uncached_per_every_block_ms": 0.03,
    "queue_ms_per_entry": 50.0,
    "saturated_cold_ms_per_entry": 10000.0,
    "cache_pressure_penalty_ms": 0.0,
    "forward_pass_ms": 5.0,
    "q_running_penalty_peak_ms": 0.0,
}
TRUE = {**CURRENT, "base_ms": 250.0, "attention_uncached_block_ms": 1.5,
        "attention_uncached_per_every_block_ms": 0.004, "forward_pass_ms": 20.0}


def synthetic_csv(path: str, n: int = 4000, seed: int = 1) -> None:
    """Rows priced by the router's formula with CURRENT, observed with TRUE."""
    rng = np.random.default_rng(seed)
    total = rng.integers(1, 2000, n)
    overlap = (total * rng.uniform(0, 1, n) * (rng.random(n) < 0.6)).astype(int)
    dispatches = rng.integers(0, 4, n)
    q_running = rng.integers(0, 12, n)
    q_waiting = rng.integers(0, 3, n) * (rng.random(n) < 0.3)
    u, e = total - overlap, q_running + dispatches
    unknown = np.maximum(q_waiting - dispatches, 0)
    inflight_true = dispatches * rng.uniform(0, 400, n)

    def price(c, inflight):
        return (c["base_ms"] + c["attention_uncached_block_ms"] * u
                + c["attention_uncached_per_every_block_ms"] * u * total
                + inflight + c["queue_ms_per_entry"] * unknown
                + c["forward_pass_ms"] * e)

    # The script rescales the recorded in-flight term by the change in an
    # average request's prefill estimate; record it as the router would.
    def avg_prefill(c):
        return (c["attention_uncached_block_ms"] * u.mean()
                + c["attention_uncached_per_every_block_ms"] * (u * total).mean())
    inflight_rec = inflight_true * avg_prefill(CURRENT) / avg_prefill(TRUE)
    predicted = price(CURRENT, inflight_rec)
    observed = price(TRUE, inflight_true) * rng.lognormal(0, 0.05, n)
    observed[rng.random(n) < 0.03] *= 8  # heavy tail

    rows = {
        "unix_ms": 1_790_000_000_000 + np.arange(n) * 5_000,
        "name": [f"w{i % 4}" for i in range(n)],
        "endpoint": "10.0.0.1:8000", "status": 200, "upstream_error": "",
        "predicted_ms": predicted.round(1),
        "base_ms": CURRENT["base_ms"],
        "q_ms": (inflight_rec + CURRENT["queue_ms_per_entry"] * unknown).round(1),
        "q_rate_ms_per_entry": CURRENT["queue_ms_per_entry"],
        "prefill_ms": 0.0, "cache_ms": 0.0,
        "observed_ms": observed.round().astype(int),
        "residual_ms": (observed - predicted).round(1),
        "overlap_blocks": overlap, "total_blocks": total,
        "q_waiting": q_waiting, "q_inflight": dispatches, "q_running": q_running,
        "q_swapped": 0, "kv_cache_usage": 0.2, "compl_per_s": 1.0,
        "kv_blocks": overlap, "q_workload": 0, "request_id": "1",
        "q_ms_constant": 0.0, "q_ms_workload": 0.0,
        "inflight_overlap_blocks": [";".join(["0"] * k) for k in dispatches],
        "expected_overlap_alpha": 1.0,
        "cache_pressure_saturated_threshold": 0.9,
        "top_k_min": 1, "top_k_fraction": 0.0, "inflight_tracking": "true",
        "tie_tolerance_ms": 0.0, "x_request_id": "",
        "q_running_ms": 0.0, "decode_contention_ms": CURRENT["forward_pass_ms"] * e,
        "q_running_penalty_peak_qr": 2, "q_running_penalty_falloff_qr": 5,
        "inflight_concurrent_capacity": 5,
        "replication_threshold_pct": 0.0, "replication_warm_target": 1,
        "replication_warm_threshold_fraction": 0.5,
        "replication_fired": "false", "workerset": "ns/set", "profile": "p",
    }
    for k, v in CURRENT.items():
        rows.setdefault(k, v)
    pd.DataFrame(rows).to_csv(path, index=False)


class AnalyzeLatencyTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        cls.path = os.path.join(cls.tmp.name, "latency.csv")
        synthetic_csv(cls.path)

    @classmethod
    def tearDownClass(cls):
        cls.tmp.cleanup()

    def test_reconstruct_matches_recorded_prediction(self):
        """The script's predictor reproduces predicted_ms from each row."""
        d = al.load([self.path])
        err = (al.reconstruct(d) - d["predicted_ms"]).abs()
        self.assertLess(err.max(), 1.0)

    def test_fit_recovers_true_coefficients(self):
        """A joint fit lands near the coefficients that generated observed_ms."""
        d = al.load([self.path])
        mean_u = d["U"].mean()
        mean_ut = (d["U"] * d["T"]).mean()
        des = al.design(d, 2, 5, mean_u, mean_ut)
        free = ["base_ms", "attention_uncached_block_ms",
                "attention_uncached_per_every_block_ms", "forward_pass_ms"]
        coef = al.fit(des, d["observed_ms"].to_numpy(float), free, CURRENT)
        for k in free:
            self.assertAlmostEqual(coef[k] / TRUE[k], 1.0, delta=0.1, msg=k)

    def test_report_recommends_the_changed_knobs(self):
        """End to end, the paste block changes the knobs that are wrong."""
        out = io.StringIO()
        argv = sys.argv
        sys.argv = ["analyze-latency.py", self.path, "--bootstrap", "10",
                    "--routing"]
        try:
            with contextlib.redirect_stdout(out):
                al.main()
        finally:
            sys.argv = argv
        text = out.getvalue()
        self.assertNotIn("WARNING", text)
        paste = text.split("Paste into the Helm values under config.profiles.p:")[1]
        values = dict(re.findall(r"^\s+(\w+): ([\d.]+)", paste, re.M))
        for k in ("base_ms", "attention_uncached_block_ms",
                  "attention_uncached_per_every_block_ms", "forward_pass_ms"):
            self.assertAlmostEqual(float(values[k]) / TRUE[k], 1.0, delta=0.1,
                                   msg=k)


if __name__ == "__main__":
    unittest.main()
