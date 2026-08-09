import {
  OWGames,
  OWGameListener,
  OWGamesEvents,
  OWHotkeys
} from "@overwolf/overwolf-api-ts";

import { AppWindow } from "../AppWindow";
import { kHotkeys, kWindowNames, kGamesFeatures } from "../consts";

import WindowState = overwolf.windows.WindowStateEx;

// The window displayed in-game while a game is running.
// Sets up Ctrl+F as the minimize/restore hotkey.
class InGame extends AppWindow {
  private static _instance: InGame;
  private _gameEventsListener: OWGamesEvents;
  private _gameListener: OWGameListener;
  private _eventsLog: HTMLElement | null;
  private _infoLog: HTMLElement | null;
  private _logBuffer: any[] = [];
  private _logFilePath: string;

  private constructor() {
    super(kWindowNames.inGame);

    this._eventsLog = document.getElementById('eventsLog');
    this._infoLog = document.getElementById('infoLog');

    this.setToggleHotkeyBehavior();
    this.setToggleHotkeyText();

    this._logFilePath = `${overwolf.io.paths.documents}\\valorant_game_events.json`;
    console.log("Game logs will be saved to: " + this._logFilePath);
  }

  public static instance() {
    if (!this._instance) {
      this._instance = new InGame();
    }

    return this._instance;
  }

  public async run() {
    try {
      const inGameState = await this.getWindowState();
      if (inGameState && (inGameState as any).window_id) {
        (overwolf.windows as any).setWindowFlags(
          (inGameState as any).window_id,
          ["InputPassThrough"],
          (res: any) => console.log("InputPassThrough set:", res)
        );
      }
    } catch (e) {
      console.warn("Could not set InputPassThrough flag:", e);
    }

    this.initGameEventsListener();
  }

  private async initGameEventsListener() {
    const gameClassId = await this.getCurrentGameClassId();
    if (gameClassId && kGamesFeatures.has(gameClassId)) {
      this.startGameEvents(gameClassId);
    }

    this._gameListener = new OWGameListener({
      onGameStarted: (info) => {
        if (info && info.classId && kGamesFeatures.has(info.classId)) {
          this.startGameEvents(info.classId);
        }
      },
      onGameEnded: () => {
        this.stopGameEvents();
      }
    });
    this._gameListener.start();
  }

  private startGameEvents(classId: number) {
    if (this._gameEventsListener) return;

    const gameFeatures = kGamesFeatures.get(classId);
    if (gameFeatures && gameFeatures.length) {
      console.log("Starting OWGamesEvents listener for classId:", classId);
      this._gameEventsListener = new OWGamesEvents(
        {
          onInfoUpdates: this.onInfoUpdates.bind(this),
          onNewEvents: this.onNewEvents.bind(this)
        },
        gameFeatures
      );
      this._gameEventsListener.start();
      this.saveLog({ type: 'system', message: 'Session started', timestamp: Date.now() });
    }
  }

  private stopGameEvents() {
    if (this._gameEventsListener) {
      console.log("Stopping OWGamesEvents listener");
      this._gameEventsListener.stop();
      this._gameEventsListener = null;
    }
  }

  private onInfoUpdates(info) {
    this.logLine(this._infoLog, info, false);
    this.saveLog({ type: 'info', data: info });
  }

  private onNewEvents(e) {
    const shouldHighlight = e.events.some(event => {
      switch (event.name) {
        case 'kill':
        case 'death':
        case 'assist':
        case 'level':
        case 'matchStart':
        case 'match_start':
        case 'matchEnd':
        case 'match_end':
          return true;
      }
      return false;
    });
    this.logLine(this._eventsLog, e, shouldHighlight);
    this.saveLog({ type: 'event', data: e });
  }

  // Displays the toggle minimize/restore hotkey in the window header if element exists
  private async setToggleHotkeyText() {
    try {
      const gameClassId = await this.getCurrentGameClassId();
      const hotkeyText = await OWHotkeys.getHotkeyText(kHotkeys.toggle, gameClassId);
      const hotkeyElem = document.getElementById('hotkey');
      if (hotkeyElem) {
        hotkeyElem.textContent = hotkeyText;
      }
    } catch (e) {
      console.warn("Could not set hotkey text:", e);
    }
  }

  // Sets toggleInGameWindow as the behavior for the Ctrl+F hotkey
  private async setToggleHotkeyBehavior() {
    const toggleInGameWindow = async (
      hotkeyResult: overwolf.settings.hotkeys.OnPressedEvent
    ): Promise<void> => {
      console.log(`pressed hotkey for ${hotkeyResult.name}`);
      try {
        const inGameState = await this.getWindowState();
        const stateStr = (inGameState as any).window_state_ex || (inGameState as any).window_state;

        if (stateStr === WindowState.NORMAL || stateStr === WindowState.MAXIMIZED) {
          this.currWindow.minimize();
        } else {
          this.currWindow.restore();
        }
      } catch (e) {
        console.error("Error toggling in-game window:", e);
      }
    };

    OWHotkeys.onHotkeyDown(kHotkeys.toggle, toggleInGameWindow);
  }

  private logLine(logElem: HTMLElement | null, data: any, highlight: boolean) {
    if (!logElem) return;
    const line = document.createElement('pre');
    line.textContent = JSON.stringify(data);

    if (highlight) {
      line.className = 'highlight';
    }

    const shouldAutoScroll =
      logElem.scrollTop + logElem.offsetHeight >= logElem.scrollHeight - 10;

    logElem.appendChild(line);

    if (shouldAutoScroll) {
      logElem.scrollTop = logElem.scrollHeight;
    }
  }

  private async getCurrentGameClassId(): Promise<number | null> {
    const info = await OWGames.getRunningGameInfo();
    return (info && info.isRunning && info.classId) ? info.classId : null;
  }

  private saveLog(entry: any) {
    this._logBuffer.push({
      timestamp: Date.now(),
      ...entry
    });

    overwolf.io.writeFileContents(
      this._logFilePath,
      JSON.stringify(this._logBuffer, null, 2),
      overwolf.io.enums.eEncoding.UTF8,
      false,
      (res) => {
        if (res.success) {
          console.log("Successfully wrote log to " + this._logFilePath);
        } else {
          console.error("Failed to write log to " + this._logFilePath + ". Error: " + res.error);
        }
      }
    );
  }
}

InGame.instance().run();
