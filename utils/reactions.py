from typing import Any


def react_info_to_chart(react_info: list[dict[str, Any]]) -> str:
    if not react_info:
        return "No reactions yet."

    reactions = sorted(
        react_info,
        key=lambda reaction: reaction["Total"],
        reverse=True,
    )

    max_reacts = max(int(reaction["Total"]) for reaction in reactions)
    bar_width = 12
    lines: list[str] = []

    for reaction in reactions:
        total = int(reaction["Total"])

        if reaction["EmojiID"]:
            emoji = f"<:{reaction['EmojiName']}:{reaction['EmojiID']}>"
        else:
            emoji = str(reaction["EmojiName"])

        filled = round((total / max_reacts) * bar_width) if max_reacts else 0
        bar = "\u2588" * filled + "\u2591" * (bar_width - filled)

        lines.append(f"{emoji}  `{bar}`  `{total}`")

    return "\n".join(lines)


def react_leaderboard_to_chart(leaderboard_info: list[dict[str, int]]) -> str:
    if not leaderboard_info:
        return "No reactions yet."

    leaderboard = sorted(
        leaderboard_info,
        key=lambda entry: int(entry["Total"]),
        reverse=True,
    )

    max_reactions = max(int(entry["Total"]) for entry in leaderboard)
    bar_width = 12
    lines: list[str] = []

    for entry in leaderboard:
        author_id = int(entry["AuthorID"])
        total = int(entry["Total"])
        filled = round((total / max_reactions) * bar_width) if max_reactions else 0
        bar = "\u2588" * filled + "\u2591" * (bar_width - filled)

        lines.append(f"`{bar}`  `{total}`  <@{author_id}>")

    return "\n".join(lines)
