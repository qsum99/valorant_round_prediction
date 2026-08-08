import { AppWindow } from "../AppWindow";
import { kWindowNames } from "../consts";

class DesktopController extends AppWindow {
  private ws: WebSocket | null = null;
  private latestReportUrl: string = "";
  private reportsHubUrl: string = "";

  constructor() {
    super(kWindowNames.desktop);
    this.initUI();
    this.connectBackend();
  }

  private initUI() {
    const btnLatest = document.getElementById("btnOpenLatestReport");
    const btnHub = document.getElementById("btnOpenReportsHub");

    if (btnLatest) {
      btnLatest.addEventListener("click", () => {
        if (this.latestReportUrl) {
          overwolf.utils.openUrlInDefaultBrowser(this.latestReportUrl);
        }
      });
    }

    if (btnHub) {
      btnHub.addEventListener("click", () => {
        if (this.reportsHubUrl) {
          overwolf.utils.openUrlInDefaultBrowser(this.reportsHubUrl);
        } else {
          // Fallback to local file path
          const fallback = "file:///" + encodeURI("c:/valorant project/valorant-round-prediction/reports/index.html");
          overwolf.utils.openUrlInDefaultBrowser(fallback);
        }
      });
    }
  }

  private connectBackend() {
    const statusText = document.getElementById("statusText");
    const statusDot = document.getElementById("statusDot");
    const connectionBadge = document.getElementById("connectionBadge");

    try {
      this.ws = new WebSocket("ws://127.0.0.1:8765");

      this.ws.onopen = () => {
        if (statusText) statusText.textContent = "Backend Connected (ws://localhost:8765)";
        if (statusDot) statusDot.className = "status-dot";
        if (connectionBadge) connectionBadge.className = "status-badge";
      };

      this.ws.onclose = () => {
        if (statusText) statusText.textContent = "Backend Disconnected";
        if (statusDot) statusDot.className = "status-dot disconnected";
        if (connectionBadge) connectionBadge.className = "status-badge disconnected";
        setTimeout(() => this.connectBackend(), 3000);
      };

      this.ws.onerror = () => {
        if (statusText) statusText.textContent = "Backend Disconnected";
        if (statusDot) statusDot.className = "status-dot disconnected";
        if (connectionBadge) connectionBadge.className = "status-badge disconnected";
      };

      this.ws.onmessage = (event) => {
        try {
          const msg = JSON.parse(event.data);
          this.handleMessage(msg);
        } catch (e) {
          console.error("Error parsing WS message:", e);
        }
      };
    } catch (e) {
      console.warn("WebSocket init error:", e);
      setTimeout(() => this.connectBackend(), 3000);
    }
  }

  private handleMessage(msg: any) {
    if (msg.type === "match_end" || msg.type === "match_report") {
      const url = msg.report_url || (msg.report_file ? `file:///${msg.report_file.replace(/\\/g, "/")}` : "");
      if (url) {
        this.latestReportUrl = url;
        const btnLatest = document.getElementById("btnOpenLatestReport");
        const reportText = document.getElementById("latestReportText");

        if (btnLatest) btnLatest.style.display = "inline-flex";
        if (reportText) {
          const outcome = (msg.outcome || "Match").toUpperCase();
          const score = msg.score_won !== undefined ? `(${msg.score_won} - ${msg.score_lost})` : "";
          reportText.textContent = `Latest Match: ${outcome} ${score} — Full Interactive Report Ready!`;
          reportText.style.color = msg.outcome === "victory" ? "#00e5cc" : "#ff4655";
        }
      }
    }
  }
}

new DesktopController();

