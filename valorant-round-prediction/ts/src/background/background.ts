import {
  OWGames,
  OWGameListener,
  OWGamesEvents,
  OWWindow
} from '@overwolf/overwolf-api-ts';

import { kWindowNames, kGameClassIds, kGamesFeatures } from "../consts";
import { GameStateManager } from "./GameState";

import RunningGameInfo = overwolf.games.RunningGameInfo;
import AppLaunchTriggeredEvent = overwolf.extensions.AppLaunchTriggeredEvent;

// The background controller holds all of the app's background logic - hence its name. it has
// many possible use cases, for example sharing data between windows, or, in our case,
// managing which window is currently presented to the user. To that end, it holds a dictionary
// of the windows available in the app.
// Our background controller implements the Singleton design pattern, since only one
// instance of it should exist.
class BackgroundController {
  private static _instance: BackgroundController;
  private _windows: Record<string, OWWindow> = {};
  private _gameListener: OWGameListener;
  private _gameEventsListener: OWGamesEvents;

  private constructor() {
    // Populating the background controller's window dictionary
    this._windows[kWindowNames.desktop] = new OWWindow(kWindowNames.desktop);
    this._windows[kWindowNames.inGame] = new OWWindow(kWindowNames.inGame);

    // When a a supported game game is started or is ended, toggle the app's windows
    this._gameListener = new OWGameListener({
      onGameStarted: this.toggleWindows.bind(this),
      onGameEnded: this.toggleWindows.bind(this)
    });

    overwolf.extensions.onAppLaunchTriggered.addListener(
      e => this.onAppLaunchTriggered(e)
    );
  };

  private async startDataCollection(classId: number) {
    this.stopDataCollection();

    const gameFeatures = kGamesFeatures.get(classId);
    if (gameFeatures && gameFeatures.length) {
      this._gameEventsListener = new OWGamesEvents(
        {
          onInfoUpdates: (info) => GameStateManager.instance().handleInfoUpdate(info),
          onNewEvents: (e) => GameStateManager.instance().handleNewEvents(e)
        },
        gameFeatures
      );
      this._gameEventsListener.start();
    }
  }

  private stopDataCollection() {
    if (this._gameEventsListener) {
      this._gameEventsListener.stop();
      this._gameEventsListener = null;
    }
  }

  // Implementing the Singleton design pattern
  public static instance(): BackgroundController {
    if (!BackgroundController._instance) {
      BackgroundController._instance = new BackgroundController();
    }

    return BackgroundController._instance;
  }

  // When running the app, start listening to games' status and decide which window should
  // be launched first, based on whether a supported game is currently running
  public async run() {
    this.ensureBackendRunning();
    this._gameListener.start();

    const currWindowName = (await this.isSupportedGameRunning())
      ? kWindowNames.inGame
      : kWindowNames.desktop;

    this._windows[currWindowName].restore();

    if (currWindowName === kWindowNames.inGame) {
      const info = await OWGames.getRunningGameInfo();
      if (info && info.isRunning) {
        this.startDataCollection(info.classId);
      }
    }
  }

  private ensureBackendRunning() {
    try {
      const socket = new WebSocket('ws://127.0.0.1:8765');
      socket.onopen = () => {
        console.log('[Background] Backend server is running.');
        socket.close();
      };
      socket.onerror = () => {
        console.log('[Background] Backend server not detected. Auto-launching python backend/server.py...');
        this.launchBackendProcess();
      };
    } catch (e) {
      this.launchBackendProcess();
    }
  }

  private launchBackendProcess() {
    try {
      if ((overwolf.utils as any) && (overwolf.utils as any).openProcess) {
        (overwolf.utils as any).openProcess(
          {
            path: 'C:\\Users\\Someshwar Kumbar\\AppData\\Local\\Programs\\Python\\Python314\\pythonw.exe',
            args: '"C:\\valorant project\\valorant-round-prediction\\backend\\server.py"',
            flags: 0
          },
          (res: any) => {
            console.log('[Background] openProcess result:', res);
          }
        );
      }
    } catch (err) {
      console.error('[Background] Failed to auto-launch backend:', err);
    }
  }

  private async onAppLaunchTriggered(e: AppLaunchTriggeredEvent) {
    console.log('onAppLaunchTriggered():', e);

    if (!e || e.origin.includes('gamelaunchevent')) {
      return;
    }

    if (await this.isSupportedGameRunning()) {
      this._windows[kWindowNames.desktop].close();
      this._windows[kWindowNames.inGame].restore();
    } else {
      this._windows[kWindowNames.desktop].restore();
      this._windows[kWindowNames.inGame].close();
    }
  }

  private toggleWindows(info: RunningGameInfo) {
    if (!info || !this.isSupportedGame(info)) {
      return;
    }

    if (info.isRunning) {
      this._windows[kWindowNames.desktop].close();
      this._windows[kWindowNames.inGame].restore();
      this.startDataCollection(info.classId);
    } else {
      this._windows[kWindowNames.desktop].restore();
      this._windows[kWindowNames.inGame].close();
      this.stopDataCollection();
    }
  }

  private async isSupportedGameRunning(): Promise<boolean> {
    const info = await OWGames.getRunningGameInfo();

    return info && info.isRunning && this.isSupportedGame(info);
  }

  // Identify whether the RunningGameInfo object we have references a supported game
  private isSupportedGame(info: RunningGameInfo) {
    return kGameClassIds.includes(info.classId);
  }
}

BackgroundController.instance().run();
