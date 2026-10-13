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
            emoji = reaction["EmojiName"]

        filled = (
            round((total / max_reacts) * bar_width)
            if max_reacts
            else 0
        )

        bar = "█" * filled + "░" * (bar_width - filled)

        lines.append(f"{emoji}  `{bar}`  `{total}`")

    return "\n".join(lines)