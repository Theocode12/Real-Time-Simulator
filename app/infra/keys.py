def tournament_state(tournament_id: str) -> str:
    return f"tournament:{tournament_id}:state"


def tournament_active_matches(tournament_id: str) -> str:
    return f"tournament:{tournament_id}:active_matches"


def mens_tennis_lock() -> str:
    return "tournament:tennis:mens:lock"


def live_game(game_id: str, prefix: str) -> str:
    return f"{prefix.rstrip(':')}:{game_id}"


def support_game(game_id: str) -> str:
    return f"support:game:{game_id}"


def support_votes(game_id: str) -> str:
    return f"support:game:{game_id}:votes"
