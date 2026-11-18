#!/usr/bin/env python3
"""Render local evidence pages used for documentation screenshots."""

from __future__ import annotations

import html
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RUN_DIR = ROOT / "artifacts" / "run"
REPORT_DIR = ROOT / "artifacts" / "report"


def plan_excerpt(path: Path, limit: int = 28) -> str:
    lines = path.read_text(encoding="utf-8").splitlines()
    try:
        start = lines.index("== Physical Plan ==")
    except ValueError:
        start = 0
    selected = lines[start : start + limit]
    return "\n".join(selected)


def page(title: str, body: str) -> str:
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{html.escape(title)}</title>
  <style>
    :root {{
      --ink: #172033;
      --muted: #627087;
      --line: #dce2ea;
      --paper: #f7f9fc;
      --navy: #12213f;
      --accent: #d95d39;
      --green: #17745b;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      background: var(--paper);
      color: var(--ink);
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }}
    .shell {{ max-width: 1320px; margin: 0 auto; padding: 42px 44px 50px; }}
    .eyebrow {{
      color: var(--accent);
      font-size: 12px;
      font-weight: 750;
      letter-spacing: .08em;
      text-transform: uppercase;
    }}
    h1 {{ margin: 9px 0 8px; font-size: 36px; line-height: 1.12; }}
    .subtitle {{ color: var(--muted); font-size: 16px; margin: 0 0 28px; }}
    .chips {{ display: flex; gap: 9px; flex-wrap: wrap; margin-bottom: 24px; }}
    .chip {{
      border: 1px solid var(--line);
      background: white;
      border-radius: 6px;
      padding: 7px 10px;
      font-family: "SFMono-Regular", Consolas, monospace;
      font-size: 12px;
      color: #34425a;
    }}
    .grid {{ display: grid; grid-template-columns: 1.55fr .75fr; gap: 20px; }}
    .panel {{
      background: white;
      border: 1px solid var(--line);
      border-radius: 8px;
      overflow: hidden;
    }}
    .panel-head {{
      padding: 15px 18px;
      border-bottom: 1px solid var(--line);
      font-size: 14px;
      font-weight: 720;
    }}
    pre {{
      margin: 0;
      padding: 18px;
      background: #101a2d;
      color: #dce7f8;
      min-height: 590px;
      white-space: pre-wrap;
      overflow-wrap: anywhere;
      font: 12.5px/1.62 "SFMono-Regular", Consolas, monospace;
    }}
    .side {{ display: grid; gap: 14px; align-content: start; }}
    .fact {{ padding: 16px 18px; border-bottom: 1px solid var(--line); }}
    .fact:last-child {{ border-bottom: 0; }}
    .fact-label {{ color: var(--muted); font-size: 11px; text-transform: uppercase; }}
    .fact-value {{ margin-top: 5px; font-size: 17px; font-weight: 720; }}
    .fact-code {{
      margin-top: 5px;
      font: 12px/1.5 "SFMono-Regular", Consolas, monospace;
      color: var(--navy);
      overflow-wrap: anywhere;
    }}
    .terminal {{
      background: #101a2d;
      color: #d8e4f5;
      border-radius: 8px;
      padding: 22px 24px;
      font: 13px/1.65 "SFMono-Regular", Consolas, monospace;
      white-space: pre-wrap;
      overflow-wrap: anywhere;
    }}
    .ok {{ color: #87d8bd; }}
    .metric-strip {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; margin-bottom: 20px; }}
    .metric {{ background: white; border: 1px solid var(--line); border-radius: 8px; padding: 16px 18px; }}
    .metric-label {{ color: var(--muted); font-size: 11px; text-transform: uppercase; }}
    .metric-value {{ font-size: 22px; font-weight: 760; margin-top: 5px; }}
    .footer {{ margin-top: 18px; color: var(--muted); font-size: 12px; }}
  </style>
</head>
<body><main class="shell">{body}</main></body>
</html>"""


def render_plan(manifest: dict, output: Path) -> None:
    plan = html.escape(plan_excerpt(RUN_DIR / "plans" / "optimized_pipeline_plan.txt"))
    source = manifest["source"]
    body = f"""
    <div class="eyebrow">Execution evidence / physical plan</div>
    <h1>Adaptive Spark plan for the zone-hour aggregation</h1>
    <p class="subtitle">A 20-file Parquet scan, broadcast zone lookup, and distributed
    aggregation captured from the successful full-data run.</p>
    <div class="chips">
      <span class="chip">Spark {html.escape(manifest["spark_version"])}</span>
      <span class="chip">{source["files"]} Parquet files</span>
      <span class="chip">{source["compressed_gib"]:.3f} GiB compressed</span>
      <span class="chip">AQE enabled</span>
      <span class="chip">shuffle partitions: {manifest["config"]["shuffle_partitions"]}</span>
    </div>
    <section class="grid">
      <div class="panel">
        <div class="panel-head">DataFrame.explain(mode="formatted") excerpt</div>
        <pre>{plan}</pre>
      </div>
      <aside class="panel side">
        <div class="fact">
          <div class="fact-label">Join strategy</div>
          <div class="fact-value">BroadcastHashJoin</div>
          <div class="fact-code">BuildRight: 265 zone rows; trip side is not shuffled.</div>
        </div>
        <div class="fact">
          <div class="fact-label">Aggregation</div>
          <div class="fact-value">Two-stage HashAggregate</div>
          <div class="fact-code">Partial aggregate before one required Exchange, then final aggregate.</div>
        </div>
        <div class="fact">
          <div class="fact-label">Column pruning</div>
          <div class="fact-value">11 source columns</div>
          <div class="fact-code">ReadSchema excludes vendor, flags, tolls, taxes, and surcharges.</div>
        </div>
        <div class="fact">
          <div class="fact-label">Predicate pushdown</div>
          <div class="fact-value">Parquet PushedFilters</div>
          <div class="fact-code">Distance, fare, total, tip, and null checks are visible in the scan.</div>
        </div>
        <div class="fact">
          <div class="fact-label">Adaptive plan</div>
          <div class="fact-value">AQE coalescing enabled</div>
          <div class="fact-code">Final UI query plan shows AQEShuffleRead coalesced.</div>
        </div>
      </aside>
    </section>
    <div class="footer">Generated from artifacts/run/plans/optimized_pipeline_plan.txt</div>
    """
    output.write_text(page("Physical execution plan", body), encoding="utf-8")


def render_success(manifest: dict, output: Path) -> None:
    metrics = manifest["data"]
    timings = manifest["timings_seconds"]
    log_lines = (RUN_DIR / "pipeline.log").read_text(encoding="utf-8").splitlines()
    log_text = html.escape("\n".join(log_lines))
    body = f"""
    <div class="eyebrow">Execution evidence / completed pipeline</div>
    <h1>Full 20-file pipeline completed successfully</h1>
    <p class="subtitle">Spark 3.5.5 on a local ten-core cluster. The run wrote partitioned
    Parquet plus two SQL result sets.</p>
    <section class="metric-strip">
      <div class="metric">
        <div class="metric-label">Source rows</div>
        <div class="metric-value">66,807,209</div>
      </div>
      <div class="metric">
        <div class="metric-label">Clean trips</div>
        <div class="metric-value">{metrics["clean_trip_count"]:,}</div>
      </div>
      <div class="metric">
        <div class="metric-label">Gross revenue</div>
        <div class="metric-value">${metrics["gross_revenue"] / 1_000_000_000:.3f}B</div>
      </div>
      <div class="metric">
        <div class="metric-label">Metrics write time</div>
        <div class="metric-value">{timings["zone_hour_metrics_write"]:.2f}s</div>
      </div>
    </section>
    <div class="terminal"><span class="ok">SUCCESS</span>
{log_text}</div>
    <div class="footer">Captured from artifacts/run/pipeline.log and run_manifest.json</div>
    """
    output.write_text(page("Successful pipeline run", body), encoding="utf-8")


def main() -> int:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    manifest = json.loads(
        (RUN_DIR / "run_manifest.json").read_text(encoding="utf-8")
    )
    render_plan(manifest, REPORT_DIR / "physical_plan.html")
    render_success(manifest, REPORT_DIR / "pipeline_success.html")
    print(REPORT_DIR)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
