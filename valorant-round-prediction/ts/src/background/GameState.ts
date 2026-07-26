import { OWGamesEvents } from "@overwolf/overwolf-api-ts";

export interface Player {
    name: string;
    playerId: string;
    character: string;
    rank: number;
    isTeammate: boolean;
    isLocal: boolean;
    team?: 'Blue' | 'Red'; // Generic team identifier if available
}

export interface TeamData {
    score: number;
    aliveCount: number;
    credits: number; // Estimated
    loadoutValue: number; // Estimated
    agents: string[];
}

export interface RoundSnapshot {
    matchId: string;
    mapName: string;
    roundNum: number;
    phase: string;
    teams: {
        blue: TeamData;
        red: TeamData;
    };
    timestamp: number;
}

export class GameStateManager {
    private static _instance: GameStateManager;

    private _matchId: string = '';
    private _mapName: string = '';
    private _currentRound: number = 0;
    private _phase: string = '';
    private _logFilePath: string;

    // Storage for collected rows
    private _datasetBuffer: RoundSnapshot[] = [];

    private _teams: { blue: TeamData, red: TeamData } = {
        blue: { score: 0, aliveCount: 5, credits: 800, loadoutValue: 0, agents: [] },
        red: { score: 0, aliveCount: 5, credits: 800, loadoutValue: 0, agents: [] }
    };

    private constructor() {
        this._logFilePath = `${overwolf.io.paths.documents}\\valorant_round_data.json`;
    }

    public static instance(): GameStateManager {
        if (!GameStateManager._instance) {
            GameStateManager._instance = new GameStateManager();
        }
        return GameStateManager._instance;
    }

    // --- Event Handlers ---

    public handleInfoUpdate(info: any) {
        if (!info) return;

        if (info.match_info) {
            this.updateMatchInfo(info.match_info);
        }
        if (info.game_info) {
            this.updateGameInfo(info.game_info);
        }
    }

    public handleNewEvents(e: any) {
        if (!e || !e.events) return;

        for (const event of e.events) {
            if (event.name === 'kill') {
                // handle kill
            } else if (event.name === 'match_start') {
                this.resetMatch();
            } else if (event.name === 'match_end') {
                this.flushData();
            }
        }
    }

    // --- Updaters ---

    private updateMatchInfo(info: any) {
        if (info.map) this._mapName = info.map;
        if (info.match_id) this._matchId = info.match_id;

        // Pseudo code for parsing roster strings to populate team info
        // In reality, we need to map players to teams.
        // For this prototype, we'll rely on global scores provided by Overwolf if available.
        if (info.score0 !== undefined) this._teams.blue.score = parseInt(info.score0); // Assuming 0 is blue
        if (info.score1 !== undefined) this._teams.red.score = parseInt(info.score1);
    }

    private updateGameInfo(info: any) {
        if (info.phase) {
            this._phase = info.phase;
            console.log(`[GameState] Phase updated: ${this._phase}`);
        }
        if (info.round_num) {
            const newRound = parseInt(info.round_num);
            if (newRound !== this._currentRound) {
                this._currentRound = newRound;
                console.log(`[GameState] Round updated to ${this._currentRound}. Capturing snapshot.`);
                this.captureSnapshot();
            }
        }
    }

    private resetMatch() {
        this._currentRound = 0;
        this._teams = {
            blue: { score: 0, aliveCount: 5, credits: 800, loadoutValue: 0, agents: [] },
            red: { score: 0, aliveCount: 5, credits: 800, loadoutValue: 0, agents: [] }
        };
        this._datasetBuffer = [];
    }

    // --- Data Collection ---

    private captureSnapshot() {
        const snapshot: RoundSnapshot = {
            matchId: this._matchId,
            mapName: this._mapName,
            roundNum: this._currentRound,
            phase: this._phase,
            teams: JSON.parse(JSON.stringify(this._teams)),
            timestamp: Date.now()
        };

        this._datasetBuffer.push(snapshot);
        console.log(`[GameState] Captured snapshot for Round ${this._currentRound}`);

        // Optional: Write on every snapshot or just at match end
        this.flushData();
    }

    private flushData() {
        if (this._datasetBuffer.length === 0) return;

        overwolf.io.writeFileContents(
            this._logFilePath,
            JSON.stringify(this._datasetBuffer, null, 2),
            overwolf.io.enums.eEncoding.UTF8,
            false, // Override? No, we probably want to append or read-modify-write in real app. For now overwrite is safer.
            // Actually, 'false' means NO prepend? No, 'false' is 'isAppend'.
            // Wait, checking docs... writeFileContents(path, content, encoding, USE_BOM, callback) in some versions?
            // Let's check common usage. 
            // In 'in_game.ts': writeFileContents(path, content, encoding, false, callback)
            (res) => {
                if (res.success) {
                    console.log("Saved dataset to " + this._logFilePath);
                } else {
                    console.error("Failed to save dataset: " + res.error);
                }
            }
        );
    }
}
