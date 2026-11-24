# RimsaBot

### Reaction tracking

The bot remembers every reaction people add or remove, and can turn that into
quick text leaderboards:

- **`/react info [user]`** — a bar chart of all the reactions someone has
  received, most-received first. Defaults to you.
- **`/react leaderboard <emoji>`** — who receives a specific emoji the most.
- **`/react set_max_message_age <seconds>`** — how far back the bot should
  look; anything older than this doesn't count. Defaults to 6 hours.

Reactions on your own messages don't count. Everything lives in a MySQL
database that the bot sets up on its own.

### Game server control

Start, stop, and check a DatHost game server without leaving Discord:

- **`/dathost start_server`** — starts or restarts it and replies with the
  `connect host:port` line to paste in.
- **`/dathost stop_server`** — shuts it down.
- **`/dathost check_server`** — how many players are on and which map.

The bot's status also shows the player count and map (or "server offline") at
all times, refreshed every 15 seconds.

### Restream

`/restream start user:@someone` takes a member's Go Live screen share from the
voice channel they're streaming in and plays it in your voice channel instead.

- **`/restream start <user>`** — begin relaying that member's stream into the
  voice channel you're currently in.
- **`/restream status`** — plain-language progress ("Waiting for the stream to
  be opened", "Playback is running"), or how the last relay ended.
- **`/restream stop`** — end it and tidy up. Anyone can start, check, or stop
  a relay.

Bots can't watch a Go Live, so the relay uses two extra Discord accounts: one
watches the source stream and one shows it in your channel.
### Ping

`/ping` — the bot's latency.

## Commands

All commands are slash commands. Settings changes require Manage Server; every
`/restream` command is open to all members. `/dathost` and `/restream` are only
available in the server they're configured for; the rest work anywhere the bot
is added.

| Command | Requires | What it does |
|---|---|---|
| `/ping` | — | Show latency. |
| `/react info [user]` | — | Reactions a member has received. |
| `/react leaderboard <emoji>` | — | Who receives an emoji the most. |
| `/react set_max_message_age <seconds>` | Manage Server | Change how far back reaction tracking looks. |
| `/dathost start_server` | Manage Server | Start the game server. |
| `/dathost stop_server` | Manage Server | Stop the game server. |
| `/dathost check_server` | — | Player count and map. |
| `/restream start <user>` | — | Relay a member's screen share into your channel. |
| `/restream status` | — | How the relay is doing. |
| `/restream stop` | — | End the relay. |

## Getting started

You'll need:

- **Python 3.10 or newer.**
- **A MySQL database** for reaction tracking.
- **A Discord bot** (application and token) added to your server.
- **A DatHost account** if you want the game server commands.
- **Two spare Discord accounts** if you want the restream — they must be
  different from each other and from anyone who streams. With neither
  configured, `/restream` just checks the request and reports what it would
  do.

Then:

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
Copy-Item config_template.json config.json
```

Fill in `config.json`:

| Key | For | What to put |
|---|---|---|
| `DISCORD_TOKEN` | bot | Your bot's token. |
| `MYSQL_SERVER`, `MYSQL_DB`, `MYSQL_USERNAME`, `MYSQL_PASSWORD` | reactions | Database login. Tables are created automatically. |
| `RESTREAM_GUILD_ID` | restream | Server where `/restream` should appear. |
| `WATCHER_TOKEN` | restream | Account that watches the source stream. |
| `SENDER_TOKEN` | restream | Account that shows the stream in your channel. |
| `DATHOST_USERNAME`, `DATHOST_PASSWORD`, `DATHOST_SERVER_ID` | game server | DatHost API credentials. |
| `DATHOST_GUILD_ID` | game server | Server where `/dathost` should appear. |

