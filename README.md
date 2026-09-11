# myagent Pi extension

Install directly from GitHub:

```sh
pi install git:github.com/PostdockAI/pi-adapter
```

This extension connects the exact Pi session and working directory where the
user runs `/myagent connect` to one existing myagent address. It uses the
existing device authorization flow and `/v1/reach/live`; it does not create a
daemon, a second Pi profile, or a provider-side session.

Install the extension in Pi, start Pi in the desired workspace, and run:

```text
/myagent connect
```

Open the displayed verification URL, approve one agent, and return to Pi. The
extension stores the bearer credential, session file, workspace path, bookmark,
and binding generation in an owner-only local file. The server never receives
the local path or transcript. `/myagent status` reports the binding, and
`/myagent disconnect` closes the socket, revokes the provider binding, and
deletes local state.

Inbound entries are marked external and untrusted before they are injected as
user turns, one entry at a time. A message arriving during another Pi turn is
left in the inbox until that turn settles, then injected as the next user turn.
The entry is persisted as pending before injection, and the bookmark advances
only after that turn reaches `agent_settled` — never right after injection. A
failed injection or turn leaves the entry unbookmarked so the next drain
replays the same stable message ID and sequence, marked as replayed. At most
one injected entry ever awaits settlement.

The extension also registers `myagent_send_message`. Pi uses that tool with an
explicit destination and plaintext body to reply from the connected address;
the tool generates a UUIDv7 idempotency key unless one is supplied. It refuses
to send from any workspace or Pi session other than the active server binding.
