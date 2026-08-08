"""
buy_advisor.py — Buy Recommendation Engine
============================================
Simulates eco / force / full-buy scenarios through Model A,
projects next-round economy, computes 2-round expected value,
and returns a recommendation with reasoning.

Runs at shopping phase before anyone buys.
"""

import copy
import logging
import pandas as pd

log = logging.getLogger("valo-backend")

# ── Economy constants (Valorant 2024+) ───────────────────────────────────────
ECO_THRESHOLD      = 10_000   # team total below this = eco
FORCE_BUY_THRESHOLD = 20_000  # team total below this = force
FULL_BUY_THRESHOLD  = 20_000  # team total at or above = full buy

# Simulated team-total money after a buy decision (5 players combined)
SIM_MONEY = {
    "eco":      4_000,   # classic only, save everything
    "force":   14_000,   # spectre + light shield ≈ 2,800 each
    "full_buy": 24_000,  # vandal + heavy + util ≈ 4,800 each
}

# Next-round economy projections (team total)
NEXT_ECON = {
    #               if_win    if_loss
    "eco":      {"win": 24_000, "loss": 20_000},  # saved money + bonus
    "force":    {"win": 15_000, "loss": 14_000},  # partial save + bonus
    "full_buy": {"win": 15_000, "loss":  9_500},  # win bonus or loss bonus only
}

# Win/loss bonus per player
WIN_BONUS_TEAM  = 15_000   # 3,000 × 5
LOSS_BONUS_TEAM =  9_500   # 1,900 × 5 base


def classify_buy(team_money: int, round_number: int) -> str:
    """Classify the current buy type from team economy."""
    if round_number == 1 or round_number == 13:
        return "pistol"
    if team_money < ECO_THRESHOLD:
        return "eco"
    if team_money < FORCE_BUY_THRESHOLD:
        return "force"
    return "full_buy"


class BuyAdvisor:
    """Runs buy simulations through Model A and recommends the best option."""

    def __init__(self, model_a, a_features: list[str]):
        self.model_a = model_a
        self.a_features = a_features

    def recommend(
        self,
        pre_snap: dict,
        ally_money: int,
        enemy_money: int,
        local_side: str,       # raw string: "attack" or "defense"
        round_number: int,
        score_won: int,
        score_lost: int,
    ) -> dict:
        """
        Generate a buy recommendation for the current round.

        Args:
            pre_snap:     the full pre-round feature snapshot dict
            ally_money:   our team's total economy
            enemy_money:  enemy team's total economy
            local_side:   "attack" or "defense" (raw string, NOT encoded)
            round_number: current round (1-based)
            score_won:    our wins so far
            score_lost:   enemy wins so far

        Returns:
            dict with recommendation, reason, scenarios, urgency, context
        """

        current_buy = classify_buy(ally_money, round_number)

        # ── Special case: pistol round ──────────────────────────────────────
        if current_buy == "pistol":
            return self._pistol_result(round_number, score_won, score_lost)

        # ── Special case: overtime (round > 24) ─────────────────────────────
        if round_number > 24:
            return self._overtime_result(
                pre_snap, local_side, score_won, score_lost
            )

        # ── Simulate 3 buy scenarios ────────────────────────────────────────
        scenarios = {}
        for buy_type, sim_money in SIM_MONEY.items():
            this_round_prob = self._simulate_scenario(
                pre_snap, sim_money, local_side
            )
            # Project next-round economy and probability
            next_win_money  = NEXT_ECON[buy_type]["win"]
            next_loss_money = NEXT_ECON[buy_type]["loss"]

            next_prob_if_win = self._simulate_next_round(
                pre_snap, next_win_money, enemy_money,
                local_side, round_number, score_won + 1, score_lost,
            )
            next_prob_if_loss = self._simulate_next_round(
                pre_snap, next_loss_money, enemy_money,
                local_side, round_number, score_won, score_lost + 1,
            )

            # 2-round expected value
            two_round_ev = (
                (this_round_prob * next_prob_if_win) +
                ((1 - this_round_prob) * next_prob_if_loss)
            )

            scenarios[buy_type] = {
                "this_round": round(this_round_prob * 100, 1),
                "next_round_ev": round(
                    (this_round_prob * next_prob_if_win +
                     (1 - this_round_prob) * next_prob_if_loss) * 100, 1
                ),
                "two_round_ev": round(two_round_ev * 100, 1),
            }

        # ── Pick the best option ────────────────────────────────────────────
        best = max(scenarios, key=lambda k: scenarios[k]["two_round_ev"])

        # ── Match context & urgency ─────────────────────────────────────────
        urgency, context = self._assess_situation(
            score_won, score_lost, round_number, ally_money, enemy_money, best
        )

        # ── Override: match point → never eco ───────────────────────────────
        if score_lost >= 12 and best == "eco":
            best = "force"

        # ── Generate reason ─────────────────────────────────────────────────
        reason = self._generate_reason(
            best, scenarios, ally_money, enemy_money, current_buy, context
        )

        return {
            "recommendation": best,
            "reason": reason,
            "scenarios": scenarios,
            "urgency": urgency,
            "context": context,
            "current_buy": current_buy,
            "ally_money": ally_money,
            "enemy_money": enemy_money,
        }

    # ═════════════════════════════════════════════════════════════════════════
    # Internal simulation helpers
    # ═════════════════════════════════════════════════════════════════════════

    def _predict_ally_prob(self, snap: dict) -> float:
        """Run Model A and return ally win probability."""
        try:
            x = pd.DataFrame([snap])[self.a_features].astype(float)
            att_prob = float(self.model_a.predict_proba(x)[0][1])
            local_side = int(snap.get("local_team_side", 1))
            ally_prob = att_prob if local_side == 1 else (1.0 - att_prob)
            return max(0.01, min(0.99, ally_prob))
        except Exception as e:
            log.warning(f"BuyAdvisor prediction failed: {e}")
            return 0.5

    def _simulate_scenario(
        self, base_snap: dict, sim_ally_money: int, local_side: str
    ) -> float:
        """
        Simulate a buy scenario by swapping ally money in the snapshot.
        Handles attack vs defense correctly:
          - If we're attack → swap att_money
          - If we're defense → swap def_money
        """
        snap = copy.deepcopy(base_snap)

        if local_side == "attack":
            snap["att_money"] = sim_ally_money
            snap["att_full_buy"] = int(sim_ally_money >= FULL_BUY_THRESHOLD)
            snap["att_eco"] = int(sim_ally_money < ECO_THRESHOLD)
        else:
            snap["def_money"] = sim_ally_money
            snap["def_full_buy"] = int(sim_ally_money >= FULL_BUY_THRESHOLD)
            snap["def_eco"] = int(sim_ally_money < ECO_THRESHOLD)

        # Recalculate derived features
        snap["economy_diff"] = snap["att_money"] - snap["def_money"]

        return self._predict_ally_prob(snap)

    def _simulate_next_round(
        self,
        base_snap: dict,
        next_ally_money: int,
        enemy_money: int,
        local_side: str,
        current_round: int,
        next_score_won: int,
        next_score_lost: int,
    ) -> float:
        """Project the next round's state and predict win probability."""
        snap = copy.deepcopy(base_snap)

        # Half-time side switch at round 13
        next_round = current_round + 1
        projected_side = local_side
        if next_round == 13:
            projected_side = "defense" if local_side == "attack" else "attack"
            snap["local_team_side"] = 0 if projected_side == "defense" else 1

        # Update money based on which side we're on
        if projected_side == "attack":
            snap["att_money"] = next_ally_money
            snap["def_money"] = enemy_money
        else:
            snap["def_money"] = next_ally_money
            snap["att_money"] = enemy_money

        snap["att_full_buy"] = int(snap["att_money"] >= FULL_BUY_THRESHOLD)
        snap["def_full_buy"] = int(snap["def_money"] >= FULL_BUY_THRESHOLD)
        snap["att_eco"] = int(snap["att_money"] < ECO_THRESHOLD)
        snap["def_eco"] = int(snap["def_money"] < ECO_THRESHOLD)
        snap["economy_diff"] = snap["att_money"] - snap["def_money"]

        # Update scores and round
        snap["score_won"] = next_score_won
        snap["score_lost"] = next_score_lost
        snap["score_diff"] = next_score_won - next_score_lost
        snap["round_number"] = next_round

        return self._predict_ally_prob(snap)

    # ═════════════════════════════════════════════════════════════════════════
    # Situation assessment
    # ═════════════════════════════════════════════════════════════════════════

    def _assess_situation(
        self, score_won, score_lost, round_number,
        ally_money, enemy_money, best_buy
    ) -> tuple[str, str]:
        """Return (urgency, context) based on match state."""
        deficit = score_lost - score_won

        # Match point for enemy
        if score_lost >= 12:
            return "high", f"Match point! Enemy at {score_lost} wins — must win this round."

        # Match point for us
        if score_won >= 12:
            return "medium", f"Match point for your team ({score_won}-{score_lost}). Close it out."

        # Large deficit
        if deficit >= 5:
            return "high", f"Down {score_won}-{score_lost}. Must win to stay in the match."

        # Moderate deficit
        if deficit >= 2:
            return "medium", f"Trailing {score_won}-{score_lost}. Need to find momentum."

        # Half-time transition
        if round_number == 13:
            return "medium", "Half-time — sides switch. Economy resets context."

        # Anti-eco detection
        if enemy_money < 8_000:
            return "low", "Enemy on eco — high confidence expected with full buy."

        # Leading or tied
        if deficit <= 0:
            return "low", f"{'Leading' if deficit < 0 else 'Tied'} {score_won}-{score_lost}. Play standard."

        return "low", ""

    # ═════════════════════════════════════════════════════════════════════════
    # Reason generation
    # ═════════════════════════════════════════════════════════════════════════

    def _generate_reason(
        self, best, scenarios, ally_money, enemy_money, current_buy, context
    ) -> str:
        """Generate a one-line human-readable reason for the recommendation."""
        eco_this  = scenarios["eco"]["this_round"]
        force_this = scenarios["force"]["this_round"]
        full_this = scenarios["full_buy"]["this_round"]

        eco_ev = scenarios["eco"]["two_round_ev"]
        full_ev = scenarios["full_buy"]["two_round_ev"]
        force_ev = scenarios["force"]["two_round_ev"]

        econ_diff = ally_money - enemy_money

        if best == "full_buy":
            if econ_diff > 5_000:
                return (f"Economy advantage (+{econ_diff:,}). "
                        f"Full buy wins {full_this:.0f}% this round.")
            else:
                gain = full_this - eco_this
                return (f"Full buy gives +{gain:.0f}% this round. "
                        f"Best 2-round value at {full_ev:.0f}%.")

        elif best == "eco":
            next_gain = eco_ev - full_ev
            this_cost = full_this - eco_this
            return (f"Save for next round. Costs {this_cost:.0f}% now "
                    f"but gains {next_gain:.0f}% over 2 rounds.")

        elif best == "force":
            return (f"Force buy balances risk: {force_this:.0f}% this round, "
                    f"{force_ev:.0f}% over 2 rounds.")

        return "Standard buy recommended."

    # ═════════════════════════════════════════════════════════════════════════
    # Special case results
    # ═════════════════════════════════════════════════════════════════════════

    def _pistol_result(self, round_number, score_won, score_lost) -> dict:
        """Pistol round — no buy choice, just flag it."""
        half = "first" if round_number == 1 else "second"
        return {
            "recommendation": "pistol",
            "reason": f"Pistol round ({half} half) — economy is fixed.",
            "scenarios": None,
            "urgency": "low",
            "context": f"Round {round_number} pistol. Everyone starts with 800 credits.",
            "current_buy": "pistol",
            "ally_money": 4_000,
            "enemy_money": 4_000,
        }

    def _overtime_result(self, pre_snap, local_side, score_won, score_lost) -> dict:
        """Overtime — economy resets, always full buy."""
        prob = self._simulate_scenario(pre_snap, 25_000, local_side)
        return {
            "recommendation": "full_buy",
            "reason": "Overtime — economy resets to 5,000 each. Full buy always.",
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
        }
