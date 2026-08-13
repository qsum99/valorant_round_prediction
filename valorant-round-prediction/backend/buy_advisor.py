"""
buy_advisor.py — VCT-Accurate Buy Recommendation Engine
========================================================
Models VALORANT credit mechanics exactly (win bonus, loss-streak bonus,
plant bonus, per-player floors, overtime reset), classifies team buy states
the way VCT IGLs do (eco / force / full / bonus / anti-eco / broken), applies
round-context rules (post-pistol, bonus round, match point), and runs the
remaining decisions through Model A as a 2-round expected-value simulation.

Runs at shopping phase before anyone buys.
"""

import copy
import logging
import pandas as pd

log = logging.getLogger("valo-backend")

# ── Credit mechanics (Valorant) ─────────────────────────────────────────────
WIN_BONUS       = 3_000   # all players on the winning team
LOSS_BONUS_BASE = 1_900   # first loss
LOSS_BONUS_STEP = 500     # +500 per consecutive loss
LOSS_BONUS_CAP  = 2_900   # capped at 3+ consecutive losses
PLAYER_CAP      = 9_000   # per-player credit cap
PLANT_BONUS     = 300     # spike planter only
START_MONEY     = 800

# ── Per-player economy floors (VCT) ─────────────────────────────────────────
ECO_FLOOR        = 2_000   # below this: clean eco, nothing that threatens a buy
FULL_BUY_FLOOR   = 3_900   # rifle + heavy shield
FULL_BUY_COMPLETE = 4_500  # rifle + heavy + utility
DEFENSE_SHIFT    = 500     # defenders need ~500 less (no exec utility)

# ── Team-state thresholds ───────────────────────────────────────────────────
BROKEN_SPREAD  = 5_000     # max-min spread that signals a broken economy
TEAM_ECO_TOTAL = 5 * ECO_FLOOR              # 10,000
TEAM_FULL_TOTAL = 5 * FULL_BUY_FLOOR        # 19,500 (matches model training)

# ── Scenario economy model ──────────────────────────────────────────────────
# SCENARIO_MONEY: pre-buy team credits the Model A snapshot sees (in-training
# distribution — rich team vs poor team). SCENARIO_SPEND: planned per-player
# spend used to project the NEXT round from the team's real credits.
SCENARIO_MONEY = {
    "eco":      10_000,
    "force":    14_000,
    "half_buy": 17_500,
    "full_buy": 22_500,
}
SCENARIO_SPEND = {
    "eco":      0,
    "force":    2_600,   # Spectre + heavy
    "half_buy": 3_300,   # rifle + light, or SMG + heavy + util
    "full_buy": 4_200,   # rifle + heavy + util
}
TYPICAL_SPEND = {
    "eco": 0, "force": 2_600, "half_buy": 3_300, "full_buy": 4_200,
    "anti_eco": 2_600, "bonus": 300, "broken": 2_000, "pistol": 0,
}


def loss_bonus(streak: int) -> int:
    """Credits per player after `streak` consecutive losses (0 if none)."""
    if streak <= 0:
        return 0
    return min(LOSS_BONUS_BASE + LOSS_BONUS_STEP * (streak - 1), LOSS_BONUS_CAP)


def player_buy_state(money: int, side: str) -> str:
    """Per-player classification: eco | force | full (side-aware)."""
    if money < ECO_FLOOR:
        return "eco"
    floor = FULL_BUY_FLOOR - (DEFENSE_SHIFT if side == "defense" else 0)
    return "full" if money >= floor else "force"


def classify_team(moneys, side, prev_won=None, round_number=0, last_buy_rec=None):
    """
    Classify a team's buy state from the per-player credit distribution,
    following VCT team-level rules:
      eco | force | full_buy | bonus | anti_eco | broken

    prev_won / last_buy_rec describe the team's previous round (needed for
    post-pistol and bonus-round detection); pass None for the enemy team when
    unknown — money-based classification is then used.
    """
    per = [player_buy_state(m, side) for m in moneys]
    fulls = sum(1 for s in per if s == "full")
    ecos  = sum(1 for s in per if s == "eco")

    if prev_won is not None and round_number in (2, 14):
        return "anti_eco" if prev_won else "eco"

    if prev_won and isinstance(last_buy_rec, dict):
        prev_rec = last_buy_rec.get("recommendation")
        if prev_rec in ("anti_eco", "force", "half_buy", "bonus"):
            if fulls < 4:
                return "bonus"

    mx, mn = max(moneys), min(moneys)
    if (mx - mn) > BROKEN_SPREAD and mn < ECO_FLOOR and mx >= FULL_BUY_FLOOR:
        return "broken"

    if ecos >= 4:
        return "eco"
    if fulls >= 4:
        return "full_buy"
    if fulls >= 3 and ecos <= 1:
        return "full_buy"
    if fulls == 0:
        return "force"
    if ecos >= 2:
        return "eco"
    return "force"


class BuyAdvisor:
    """Runs buy scenarios through Model A and applies VCT economy rules."""

    def __init__(self, model_a, a_features: list[str]):
        self.model_a = model_a
        self.a_features = a_features

    def recommend(
        self,
        pre_snap: dict,
        ally_moneys,
        enemy_moneys,
        local_side: str,
        round_number: int,
        score_won: int,
        score_lost: int,
        ally_streak: int = 0,
        enemy_streak: int = 0,
        plant_last: bool = False,
        last_buy_rec: dict | None = None,
    ) -> dict:
        """
        Generate a buy recommendation for the current round.

        ally_moneys / enemy_moneys: per-player credit lists (5 entries each).
        ally_streak / enemy_streak: consecutive losses before this round.
        plant_last:                 did we plant the spike last round?
        """
        ally_moneys   = self._norm_moneys(ally_moneys)
        enemy_moneys  = self._norm_moneys(enemy_moneys)
        ally_money    = sum(ally_moneys)
        enemy_money   = sum(enemy_moneys)
        enemy_side    = "defense" if local_side == "attack" else "attack"

        if round_number in (1, 13):
            return self._pistol_result(round_number, score_won, score_lost)

        if round_number > 24:
            return self._overtime_result(pre_snap, local_side, score_won, score_lost)

        prev_won = None
        if round_number >= 2:
            prev_won = ally_streak == 0
        enemy_prev_won = (not prev_won) if prev_won is not None else None

        ally_state  = classify_team(ally_moneys, local_side, prev_won, round_number, last_buy_rec)
        enemy_state = classify_team(enemy_moneys, enemy_side, enemy_prev_won, round_number, None)

        if ally_state in ("anti_eco", "bonus"):
            return self._special_result(
                ally_state, ally_moneys, enemy_moneys, enemy_state,
                local_side, round_number, score_won, score_lost,
                ally_streak, enemy_streak, plant_last,
            )

        if ally_state == "broken":
            return self._broken_result(
                ally_moneys, enemy_moneys, enemy_state,
                local_side, round_number, score_won, score_lost,
            )

        scenarios = {}
        for buy_type in ("eco", "force", "half_buy", "full_buy"):
            this_round_prob = self._simulate_scenario(
                pre_snap, SCENARIO_MONEY[buy_type], local_side
            )
            spend = SCENARIO_SPEND[buy_type]

            ally_next_win  = self._project_team(ally_moneys,  spend, won=True,  streak_after=0, plant=False)
            enemy_next_win = self._project_team(enemy_moneys, TYPICAL_SPEND[enemy_state], won=False,
                                                streak_after=enemy_streak + 1, plant=False)
            ally_next_loss  = self._project_team(ally_moneys,  spend, won=False, streak_after=ally_streak + 1,
                                                 plant=plant_last)
            enemy_next_loss = self._project_team(enemy_moneys, TYPICAL_SPEND[enemy_state], won=True,
                                                 streak_after=0, plant=False)

            next_prob_if_win = self._simulate_next_round(
                pre_snap, sum(ally_next_win), sum(enemy_next_win),
                local_side, round_number, score_won + 1, score_lost,
            )
            next_prob_if_loss = self._simulate_next_round(
                pre_snap, sum(ally_next_loss), sum(enemy_next_loss),
                local_side, round_number, score_won, score_lost + 1,
            )

            two_round_ev = (
                this_round_prob * next_prob_if_win +
                (1 - this_round_prob) * next_prob_if_loss
            )
            scenarios[buy_type] = {
                "this_round": round(this_round_prob * 100, 1),
                "next_round_ev": round(two_round_ev * 100, 1),
                "two_round_ev": round(two_round_ev * 100, 1),
            }

        best = max(scenarios, key=lambda k: scenarios[k]["two_round_ev"])

        # ── VCT affordability gate ───────────────────────────────────────────
        # The recommendation must match what the team can actually buy:
        # a team that already has full-buy credits never saves, and a team
        # with nothing to buy never forces. Model EV only decides the
        # ambiguous middle band (2,000-3,899 /player).
        if ally_state == "full_buy":
            best = "full_buy"
        elif not (score_won >= 12 or score_lost >= 12) and \
             all(m < ECO_FLOOR for m in ally_moneys):
            best = "eco"

        # Match point in EITHER direction: no round left to save for — spend all.
        if (score_won >= 12 or score_lost >= 12) and best == "eco":
            best = "force"

        urgency, context = self._assess_situation(
            score_won, score_lost, round_number, ally_money, enemy_money,
            enemy_state, plant_last, best,
        )
        reason = self._generate_reason(
            best, scenarios, ally_money, enemy_money, ally_state, enemy_state,
            ally_streak, plant_last, ally_moneys, local_side,
        )

        return {
            "recommendation": best,
            "reason": reason,
            "scenarios": scenarios,
            "urgency": urgency,
            "context": context,
            "plan": self._plan_for(best, ally_moneys, local_side),
            "current_buy": ally_state,
            "ally_money": ally_money,
            "enemy_money": enemy_money,
            "enemy_buy": enemy_state,
        }

    # ═════════════════════════════════════════════════════════════════════════
    # Economy helpers
    # ═════════════════════════════════════════════════════════════════════════

    @staticmethod
    def _norm_moneys(moneys):
        if isinstance(moneys, (int, float)):
            total = int(moneys)
            per = total // 5
            return [per] * 4 + [total - per * 4]
        out = [max(0, int(m)) for m in (moneys or [])]
        while len(out) < 5:
            out.append(START_MONEY)
        return out[:5]

    def _project_team(self, moneys, spend, won, streak_after, plant=False):
        """Project per-player credits after this round using real mechanics."""
        out = []
        for m in moneys:
            bonus = WIN_BONUS if won else loss_bonus(streak_after)
            out.append(min(PLAYER_CAP, max(0, int(m) - spend + bonus)))
        if not won and plant and out:
            i = out.index(min(out))
            out[i] = min(PLAYER_CAP, out[i] + PLANT_BONUS)
        return out

    # ═════════════════════════════════════════════════════════════════════════
    # Model A simulation helpers
    # ═════════════════════════════════════════════════════════════════════════

    def _predict_ally_prob(self, snap: dict) -> float:
        try:
            x = pd.DataFrame([snap])[self.a_features].astype(float)
            att_prob = float(self.model_a.predict_proba(x)[0][1])
            local_side = int(snap.get("local_team_side", 1))
            ally_prob = att_prob if local_side == 1 else (1.0 - att_prob)
            return max(0.01, min(0.99, ally_prob))
        except Exception as e:
            log.warning(f"BuyAdvisor prediction failed: {e}")
            return 0.5

    def _apply_side_money(self, snap, side, money):
        if side == "attack":
            snap["att_money"] = money
            snap["att_full_buy"] = int(money >= TEAM_FULL_TOTAL)
            snap["att_eco"] = int(money < TEAM_ECO_TOTAL)
        else:
            snap["def_money"] = money
            snap["def_full_buy"] = int(money >= TEAM_FULL_TOTAL)
            snap["def_eco"] = int(money < TEAM_ECO_TOTAL)

    def _simulate_scenario(self, base_snap, sim_ally_money, local_side) -> float:
        snap = copy.deepcopy(base_snap)
        self._apply_side_money(snap, local_side, sim_ally_money)
        snap["economy_diff"] = snap["att_money"] - snap["def_money"]
        return self._predict_ally_prob(snap)

    def _simulate_next_round(
        self, base_snap, next_ally_money, next_enemy_money,
        local_side, current_round, next_score_won, next_score_lost,
    ) -> float:
        snap = copy.deepcopy(base_snap)
        next_round = current_round + 1
        projected_side = local_side
        if next_round == 13:
            projected_side = "defense" if local_side == "attack" else "attack"
            snap["local_team_side"] = 0 if projected_side == "defense" else 1

        self._apply_side_money(snap, projected_side, next_ally_money)
        enemy_side = "defense" if projected_side == "attack" else "attack"
        self._apply_side_money(snap, enemy_side, next_enemy_money)
        snap["economy_diff"] = snap["att_money"] - snap["def_money"]

        snap["score_won"] = next_score_won
        snap["score_lost"] = next_score_lost
        snap["score_diff"] = next_score_won - next_score_lost
        snap["round_number"] = next_round

        return self._predict_ally_prob(snap)

    # ═════════════════════════════════════════════════════════════════════════
    # Situation assessment & plans
    # ═════════════════════════════════════════════════════════════════════════

    def _assess_situation(self, score_won, score_lost, round_number,
                          ally_money, enemy_money, enemy_state, plant_last, best) -> tuple[str, str]:
        deficit = score_lost - score_won

        if score_lost >= 12:
            return "high", f"Enemy match point ({score_lost}-{score_won}) — spend everything, there is no next round."
        if score_won >= 12:
            return "high", f"Your match point ({score_won}-{score_lost}) — spend everything, close it out."
        if deficit >= 5:
            return "high", f"Down {score_won}-{score_lost}. Must win to stay in the match."
        if deficit >= 2:
            return "medium", f"Trailing {score_won}-{score_lost}. Need to find momentum."
        if round_number in (2, 14) and best in ("anti_eco", "eco"):
            return "medium", f"Post-pistol round. Enemy projecting: {enemy_state.upper()}."
        if enemy_state in ("eco", "broken"):
            return "low", f"Enemy on {enemy_state.replace('_', ' ')} — punish the weak buy."
        if plant_last:
            return "low", f"Planted last round — the planter carries +{PLANT_BONUS} credits into projections."
        if deficit <= 0:
            return "low", f"{'Leading' if deficit < 0 else 'Tied'} {score_won}-{score_lost}. Play standard."
        return "low", ""

    def _plan_for(self, best, moneys, side) -> str:
        if best == "eco":
            return "Buy nothing over ~300. Save for the rifle next round."
        if best == "force":
            return "Spectre/Bulldog + shield (~2,600). Keep balance above 2,000 for next round."
        if best == "half_buy":
            return "Rifle + light shield, or SMG + heavy + util (~3,300)."
        if best == "full_buy":
            return "Rifle + heavy shield + util (~4,200). Five rifles beat four rifles and a Classic."
        return ""

    # ═════════════════════════════════════════════════════════════════════════
    # Reason generation
    # ═════════════════════════════════════════════════════════════════════════

    def _generate_reason(self, best, scenarios, ally_money, enemy_money,
                         ally_state, enemy_state, ally_streak, plant_last,
                         ally_moneys, local_side) -> str:
        this = scenarios[best]["this_round"]
        ev = scenarios[best]["two_round_ev"]
        econ_diff = ally_money - enemy_money
        lb = loss_bonus(ally_streak + 1)

        if best == "full_buy":
            if ally_state == "full_buy":
                floor = FULL_BUY_FLOOR - (DEFENSE_SHIFT if local_side == "defense" else 0)
                rifles = sum(1 for m in ally_moneys if m >= floor)
                return (f"{rifles}/5 players have rifle money (≥{floor:,} each). "
                        f"Never save with full-buy credits.")
            if econ_diff > 5_000:
                return (f"Economy advantage (+{econ_diff:,}). Full buy wins {this:.0f}% "
                        f"this round — the VCT play vs {enemy_state.replace('_', ' ')}.")
            return (f"Full buy gives {this:.0f}% this round and {ev:.0f}% over 2 rounds. "
                    f"Best value on the board.")
        if best == "eco":
            parts = [f"Save. Losing costs +{lb:,}/player, enough for a rifle next round."]
            if enemy_state == "full_buy":
                parts.append("Enemy full buy — Sheriff headshots one-tap full armor, don't walk into eco corners.")
            if plant_last:
                parts.append(f"Planter carries +{PLANT_BONUS} into next round.")
            return " ".join(parts)
        if best == "force":
            return (f"Force buy: {this:.0f}% now, {ev:.0f}% over 2 rounds. "
                    f"Only viable because your loss bonus reaches +{lb:,}/player on a loss.")
        if best == "half_buy":
            return (f"Half buy: {this:.0f}% this round vs {scenarios['eco']['this_round']:.0f}% eco, "
                    f"{ev:.0f}% over 2 rounds — spend but keep the next rifle guaranteed.")
        return "Standard buy recommended."

    # ═════════════════════════════════════════════════════════════════════════
    # Deterministic round states (VCT rules — no EV needed)
    # ═════════════════════════════════════════════════════════════════════════

    def _special_result(self, ally_state, ally_moneys, enemy_moneys, enemy_state,
                        local_side, round_number, score_won, score_lost,
                        ally_streak, enemy_streak, plant_last) -> dict:
        if ally_state == "anti_eco":
            next_after = self._project_team(ally_moneys, TYPICAL_SPEND["anti_eco"],
                                            won=False, streak_after=1)
            return {
                "recommendation": "anti_eco",
                "reason": "Won the pistol — never save. Press the advantage with an anti-eco buy.",
                "plan": "Spectre/Bulldog + Heavy Shield (~2,600). Support buys utility and drops a rifle on round 3.",
                "scenarios": None,
                "urgency": "low",
                "context": f"Post-pistol win. Enemy is on {enemy_state.replace('_', ' ')} "
                           f"— even a loss leaves ~{next_after[0]:,}/player for a rifle.",
                "current_buy": "anti_eco",
                "ally_money": sum(ally_moneys),
                "enemy_money": sum(enemy_moneys),
                "enemy_buy": enemy_state,
            }

        next_if_loss = self._project_team(ally_moneys, TYPICAL_SPEND["bonus"],
                                          won=False, streak_after=ally_streak + 1,
                                          plant=plant_last)
        return {
            "recommendation": "bonus",
            "reason": "Won with a weak loadout — keep your weapons, do NOT re-buy rifles.",
            "plan": "Save credits, play close angles, break their economy with kills.",
            "scenarios": None,
            "urgency": "low",
            "context": f"Bonus round. Even if you lose, you enter the next round with "
                       f"~{min(next_if_loss):,}/player for a full rifle buy.",
            "current_buy": "bonus",
            "ally_money": sum(ally_moneys),
            "enemy_money": sum(enemy_moneys),
            "enemy_buy": enemy_state,
        }

    def _broken_result(self, ally_moneys, enemy_moneys, enemy_state,
                       local_side, round_number, score_won, score_lost) -> dict:
        rich = sum(1 for m in ally_moneys if m >= FULL_BUY_FLOOR)
        return {
            "recommendation": "broken",
            "reason": "Broken economy — richest players rifle, the rest save. Never all-force.",
            "plan": (f"Hero buy: {rich} player{'s' if rich != 1 else ''} full buy, "
                     f"{5 - rich} save — drop rifles after the round."),
            "scenarios": None,
            "urgency": "medium",
            "context": f"Money spread over {BROKEN_SPREAD:,} credits. "
                       f"Enemy projecting: {enemy_state.replace('_', ' ')}.",
            "current_buy": "broken",
            "ally_money": sum(ally_moneys),
            "enemy_money": sum(enemy_moneys),
            "enemy_buy": enemy_state,
        }

    def _pistol_result(self, round_number, score_won, score_lost) -> dict:
        half = "first" if round_number == 1 else "second"
        return {
            "recommendation": "pistol",
            "reason": f"Pistol round ({half} half) — economy is fixed at 800 credits.",
            "plan": "Ghost/Classic + one ability. Kill money is doubled on pistols.",
            "scenarios": None,
            "urgency": "low",
            "context": f"Round {round_number} pistol. Everyone starts with 800 credits.",
            "current_buy": "pistol",
            "ally_money": 5 * START_MONEY,
            "enemy_money": 5 * START_MONEY,
            "enemy_buy": "pistol",
        }

    def _overtime_result(self, pre_snap, local_side, score_won, score_lost) -> dict:
        prob = self._simulate_scenario(pre_snap, 25_000, local_side)
        return {
            "recommendation": "full_buy",
            "reason": "Overtime — both teams hard-reset to 5,000 each. Full buy every round.",
            "plan": "Rifle + heavy + util (~4,200). Spend every credit.",
            "scenarios": {
                "full_buy": {
                    "this_round": round(prob * 100, 1),
                    "next_round_ev": 50.0,
                    "two_round_ev": round(prob * 50, 1),
                }
            },
            "urgency": "high",
            "context": f"Overtime round. Score: {score_won}-{score_lost}.",
            "current_buy": "full_buy",
            "ally_money": 25_000,
            "enemy_money": 25_000,
            "enemy_buy": "full_buy",
        }
