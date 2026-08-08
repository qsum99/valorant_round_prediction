"""
report_generator.py
-------------------
Generates standalone HTML post-match reports and updates the reports index hub.
"""

import json
import re
from datetime import datetime
from pathlib import Path
from report_template import REPORT_HTML_TEMPLATE


class ReportGenerator:
    """Generates standalone interactive HTML post-match reports."""

    def __init__(self, base_dir: Path | None = None):
        if base_dir is None:
            # Default to valorant-round-prediction/reports
            self.output_dir = Path(__file__).parent.parent / "reports"
        else:
            self.output_dir = Path(base_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def generate(self, match_data: dict, output_dir: Path | str | None = None, dry_run: bool = False) -> str | None:
        """
        Builds the HTML report string and writes to disk.
        Returns the absolute file path of the generated report.
        """
        if dry_run:
            print("\n[ReportGenerator DRY-RUN] Match Data JSON:")
            print(json.dumps(match_data, indent=2))
            return None

        target_dir = Path(output_dir) if output_dir else self.output_dir
        target_dir.mkdir(parents=True, exist_ok=True)

        match_meta = match_data.get("match", {})
        map_name = self._sanitize(match_meta.get("map", "Valorant"))
        outcome = self._sanitize(match_meta.get("outcome", "match"))
        date_str = match_meta.get("date") or datetime.now().strftime("%Y-%m-%d")
        match_id = self._sanitize(str(match_meta.get("match_id", "report"))[:8])

        filename = f"match_{map_name}_{outcome}_{date_str}_{match_id}.html"
        file_path = target_dir / filename

        # Inject JSON into the static HTML template
        json_blob = json.dumps(match_data, indent=2)
        html_content = REPORT_HTML_TEMPLATE.replace("{match_data_json}", json_blob)

        with open(file_path, "w", encoding="utf-8") as f:
            f.write(html_content)

        # Update index hub
        self._update_index_hub(target_dir, match_data, filename)

        return str(file_path.resolve())

    def _sanitize(self, s: str) -> str:
        return re.sub(r"[^\w\-]", "_", str(s).strip())

    def _update_index_hub(self, target_dir: Path, match_data: dict, report_filename: str):
        """Maintains an index.html listing all generated reports."""
        index_file = target_dir / "index.html"
        history_file = target_dir / ".report_history.json"

        history = []
        if history_file.exists():
            try:
                with open(history_file, "r", encoding="utf-8") as f:
                    history = json.load(f)
            except Exception:
                history = []

        match_meta = match_data.get("match", {})
        final_score = match_meta.get("final_score", [0, 0])
        score_str = f"{final_score[0]} - {final_score[1]}"
        acc = match_meta.get("model_accuracy")
        acc_str = f"{acc*100:.1f}%" if acc is not None else "--"

        entry = {
            "filename": report_filename,
            "date": match_meta.get("date") or datetime.now().strftime("%Y-%m-%d %H:%M"),
            "map": match_meta.get("map", "Valorant"),
            "outcome": match_meta.get("outcome", "Unknown").upper(),
            "score": score_str,
            "agent": match_meta.get("local_agent") or match_meta.get("local_player_name") or "Agent",
            "accuracy": acc_str,
            "rounds": len(match_data.get("rounds", [])),
        }

        # Prepend latest entry and remove duplicate filenames
        history = [e for e in history if e.get("filename") != report_filename]
        history.insert(0, entry)

        try:
            with open(history_file, "w", encoding="utf-8") as f:
                json.dump(history, f, indent=2)
        except Exception:
            pass

        # Build index.html
        rows_html = []
        for h in history:
            is_vic = h["outcome"] == "VICTORY"
            badge_color = "#00e5cc" if is_vic else "#ff4655"
            bg_color = "rgba(0, 229, 204, 0.1)" if is_vic else "rgba(255, 70, 85, 0.1)"
            rows_html.append(f"""
              <tr>
                <td>{h['date']}</td>
                <td><strong>{h['map']}</strong></td>
                <td><span style="background:{bg_color}; color:{badge_color}; padding:2px 8px; border-radius:4px; font-weight:700; font-family:'Rajdhani', sans-serif;">{h['outcome']}</span></td>
                <td><strong>{h['score']}</strong></td>
                <td>{h['agent']}</td>
                <td>{h['accuracy']}</td>
                <td>{h['rounds']}</td>
                <td><a href="{h['filename']}" style="color:#00e5cc; text-decoration:none; font-weight:600;">View Report →</a></td>
              </tr>
            """)

        index_html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>Valorant Match Reports Hub</title>
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600&family=Rajdhani:wght@600;700&display=swap" rel="stylesheet">
  <style>
    body {{
      background: #0f1923;
      color: #ecedee;
      font-family: 'Inter', sans-serif;
      padding: 32px 20px;
      display: flex;
      justify-content: center;
    }}
    .container {{
      max-width: 1000px;
      width: 100%;
    }}
    h1 {{
      font-family: 'Rajdhani', sans-serif;
      font-size: 32px;
      letter-spacing: 0.08em;
      margin-bottom: 20px;
      color: #00e5cc;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      background: #1a2332;
      border: 1px solid rgba(255,255,255,0.08);
      border-radius: 8px;
      overflow: hidden;
      font-size: 13px;
    }}
    th {{
      background: #242f3d;
      font-family: 'Rajdhani', sans-serif;
      font-size: 12px;
      letter-spacing: 0.1em;
      color: #8fa3b1;
      padding: 12px 14px;
      text-align: left;
    }}
    td {{
      padding: 12px 14px;
      border-bottom: 1px solid rgba(255,255,255,0.04);
      color: #8fa3b1;
    }}
    tr:hover td {{
      background: #2c394b;
    }}
  </style>
</head>
<body>
  <div class="container">
    <h1>VALORANT MATCH REPORTS HUB</h1>
    <table>
      <thead>
        <tr>
          <th>DATE</th>
          <th>MAP</th>
          <th>OUTCOME</th>
          <th>SCORE</th>
          <th>AGENT</th>
          <th>MODEL ACC</th>
          <th>ROUNDS</th>
          <th>REPORT</th>
        </tr>
      </thead>
      <tbody>
        {"".join(rows_html)}
      </tbody>
    </table>
  </div>
</body>
</html>
"""
        with open(index_file, "w", encoding="utf-8") as f:
            f.write(index_html)
