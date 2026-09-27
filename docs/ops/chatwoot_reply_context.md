# Chatwoot reply context — visible quote safety switch

Controls how an inbound WhatsApp reply to a Chatwoot message shows the
replied-to context to the operator.

```env
CHATWOOT_REPLY_CONTEXT_VISIBLE_QUOTE_MODE=fallback_only
```

Native reply metadata (`in_reply_to` / `in_reply_to_external_id`) is sent to
Chatwoot **only through the REST API**, best-effort. `altegio_bot` never writes
to the Chatwoot database.

## Modes

### `fallback_only` (default)

- Same-conversation native replies (a target with a Chatwoot message id in the
  destination conversation) rely on Chatwoot's **native reply preview**; the bot
  does not add a visible body quote, so the context is not duplicated.
- A visible body quote is still added in fallback cases:
  - bot/automation targets whose native target cannot be proven,
  - cross-conversation targets,
  - missing targets.
- Visible fallback quotes intentionally show a shortened, single-line preview
  (~100 chars) of the replied-to message, so replies to long bot/automation
  messages stay readable and closer to native messenger behavior.

### `always` (safety fallback)

- Always prepends a visible body quote, even when native metadata is also sent.
- May duplicate the context if Chatwoot's native preview also renders.
- Use this if native reply previews disappear (e.g. after a Chatwoot upgrade),
  so operators never lose visible context.

## Troubleshooting

**Symptom:** incoming WhatsApp replies to Chatwoot operator messages arrive
without visible context / native preview.

**Action:**

1. Set:

   ```env
   CHATWOOT_REPLY_CONTEXT_VISIBLE_QUOTE_MODE=always
   ```

2. Recreate the affected services:

   ```bash
   docker compose -p altegio_bot \
     -f docker-compose.yml \
     -f docker-compose.chatwoot-internal.yml \
     up -d --force-recreate altegio-api altegio-whatsapp-inbox-worker altegio-inbox-worker
   ```

3. Re-test a WhatsApp native Reply to an operator message.

4. Expected fallback behavior — the body now starts with a visible quote:

   ```text
   ↩️ Ответ на сообщение:
   «<operator message text>»

   <client reply text>
   ```

## Not the removed unsafe DB path

This switch is purely message-body formatting through the Chatwoot REST API. It
must not be confused with the removed direct-database normalization. Do **not**:

- restore the removed Chatwoot database URL / DSN setting;
- connect to or write to Chatwoot's Postgres database;
- normalize the message `content_attributes` column directly in the database;
- treat a particular database JSON storage shape of `content_attributes` as a
  success criterion.

## Reactions to automatic messages — how the native target is proven

A bot/automation send has no Chatwoot message of its own: it exists there only as
the **private mirror note** posted by `ChatwootClient.mirror_outbound_as_note`. A
reaction to an automatic message can be rendered as a native Chatwoot reply only
once that note has been *proven*.

### Trust model

The proof is a record of an action we took ourselves, never a search result.

**Primary — durable registry (new notes).** When the note is posted and Chatwoot
answers with a valid message id, `ChatwootHybridProvider` stores a row in
`chatwoot_outbound_mirrors` (module `altegio_bot.chatwoot_mirror_registry`):

| Column | Role |
| --- | --- |
| `provider_message_id` | exact Meta wamid — globally unique per message; idempotency key AND lookup key |
| `chatwoot_message_id` | the private note Chatwoot actually created |
| `chatwoot_conversation_id` | the conversation it landed in — the isolation boundary |
| `marker_version` | marker contract version in force when written |
| `chatwoot_route`, `chatwoot_inbox_id`, `tenant_provider`, `company_id` | routing provenance for ops — descriptive, never the gate |

The reaction path resolves the target with **one indexed read** on
`(provider_message_id, chatwoot_conversation_id, marker_version)`. No history is
walked, so conversation length is irrelevant and the cost is O(1).

Properties that matter:

- the write is idempotent (`ON CONFLICT DO NOTHING` on the wamid). The first
  successful Chatwoot response wins; a replay cannot overwrite it, and the unique
  constraint is what makes a global conflict impossible rather than merely
  unlikely;
- it runs in its **own short transaction**. The mirror is a background task that
  races the Outbox row's own `provider_message_id` commit, so the write must not
  assume that row exists yet and must never lock it. There is no foreign key to
  `outbox_messages`;
- `outbox_messages.chatwoot_message_id` is deliberately **not** reused: it already
  means the operator-relay message id, and an accidentally populated value must
  never look like proof;
- a missing, foreign-conversation, stale-version or not-yet-committed link is a
  miss. Nothing is guessed.

**Fallback — legacy bounded scan (old notes).** `find_outbound_mirror_note`
accepts a message only when **every** condition holds at once:

| Condition | Why |
| --- | --- |
| listed by the destination conversation, and its own `conversation_id` matches | no cross-conversation `in_reply_to` |
| `message_type` is outgoing | an inbound message is never a mirror note |
| `private` is exactly `true` | a public message is not a mirror note |
| marker equals `whatsapp_outbound_mirror_v1` | version-pinned contract |
| `whatsapp_provider_message_id` equals the reaction target wamid exactly | the reaction must hit *that* message |
| id is a positive integer | an unusable id is not a target |

Nothing is matched by body text, `template_code`, `created_at`, time proximity,
"the last message" or result order.

### Early termination and the scope of the uniqueness check

The scan **stops at the page that proves the target**. A note on the first page
costs exactly one request regardless of how much older history the conversation
has — this is the point: a long history must not be able to veto a target that was
already proven, which is what an earlier version of this lookup did.

Consequently uniqueness is checked over **the pages actually walked**: two distinct
proven ids seen before the scan stops are refused (fail closed), and a duplicate
further back is never looked for. Global uniqueness is therefore **not** claimed by
the scan. For new notes it comes from the registry's unique key on the wamid.

### Bounds

Two independent limits, both named in `chatwoot_client.py`:

- `_MIRROR_NOTE_MAX_PAGES` = 10 pages (~200 messages) — how far back the walk may
  reach. It bounds how DEEP a target can still be found, and it no longer vetoes a
  target already found: a hit is returned before the budget is consulted;
- `_MIRROR_NOTE_TOTAL_DEADLINE_SEC` = 5 s — **one wall-clock deadline for the whole
  scan**, covering every page, checked before each request. Each request is
  additionally capped at `_MIRROR_NOTE_PAGE_TIMEOUT_SEC` = 2 s, and its effective
  timeout is the smaller of that and the budget remaining, which makes the total
  bound strict rather than nominal.

The deadline exists because the scan runs inline while the `WhatsAppEvent` row is
locked `FOR UPDATE` and events are processed serially. Ten independent 15 s client
timeouts would be 150 s of held lock — unacceptable for a best-effort cosmetic
improvement, and it would delay every following event. Hitting the deadline returns
a miss and the reaction is delivered with the visible quote, so the next event is
processed normally.

### Page order and the cursor

Chatwoot 4.17 filters the next page by `id < before`, orders by `created_at DESC`,
takes a page, and then **reverses** it before returning. So:

- the array is in **ascending chronological order**. It is **not** newest-first;
- the chronological boundary of a page is `page[0]` — the oldest message on it;
- the next cursor is `page[0]`'s id, taken **positionally**.

`min(Message.id)` is explicitly **not** used. The two coincide only while ids happen
to increase with `created_at`; a backdated or imported message breaks that, and a
`min(id)` cursor would then name a message that is not the boundary and silently
skip the history between them — including, in the worst case, the page holding the
marker. The page is never re-sorted locally, because reordering the server's answer
throws away the only ordering information the response carries. If the boundary
element has no usable positive integer id, the page fails closed rather than the
cursor being taken from some other element.

### Known upstream limitation

Chatwoot pages by `id` while ordering by `created_at`, and no id cursor can express
that ordering. A message backdated with an id **above** the first page's boundary is
in neither the newest-by-`created_at` page nor any `id <` page, so the scan cannot
reach it.

This is not papered over with a local heuristic. It is the reason new mirror notes
are resolved from the durable registry, whose correctness does not depend on
pagination at all. For older notes the scan fails closed, and the reaction shows the
visible quote. Chatwoot is not upgraded or patched to work around it.

### Fail-closed fallback

All of these produce the visible quote — a short single-line preview of the original
text plus the emoji, and no `in_reply_to`:

- no recorded link and no scan match;
- a recorded link for another conversation, another wamid or another marker version;
- two distinct proven matches inside the region the scan walked;
- an HTTP status other than 200, or a transport error, on **any** page;
- a body that is not JSON, or not a recognizable messages payload, on any page (a
  malformed page is never read as "no more messages");
- a page whose boundary yields no usable cursor;
- a cursor that would not strictly decrease, which is what a replayed page looks
  like;
- the page budget running out;
- the overall wall-clock deadline expiring;
- a database error on the registry read.

None of these can fail the reaction, the WhatsAppEvent or the Meta send: the
reaction is always delivered, only its native preview is lost.

Logs carry the conversation id, whether a cursor was in use, page/message counts and
a stable reason code (`transport_error`, `http_status`, `malformed_json`,
`malformed_payload`, `deadline_exceeded`, `page_budget_exhausted`,
`no_pagination_cursor`, `cursor_not_advancing`, `ambiguous_matches`,
`not_in_history`). No wamid, phone, message body, URL, token or response body is ever
logged. `content_attributes.whatsapp_reaction_native_source` records which proof was
used: `target_chatwoot_message`, `mirror_registry` or `mirror_scan`.

### No historical backfill, and no Chatwoot database access

Notes created before the registry have no link; notes created before the marker have
no evidence at all. Both keep the visible quote and nothing is backfilled. An
accidentally populated `chatwoot_message_id` / `chatwoot_conversation_id` on a bot
Outbox row remains no evidence.

`altegio_bot` never connects to Chatwoot's database. Every Chatwoot read and write
goes through the REST API; the durable link lives in `altegio_bot`'s own PostgreSQL
(`chatwoot_outbound_mirrors`, Alembic revision `c4e9a1b78d52`, whose downgrade simply
returns the reaction path to the bounded scan).

## Closed 24h window — Click-to-Chat link in the private note

The `window_closed` operator note (mode `private_note_only`, both at the first
check and when the window closes between prepare and claim) ends with one short
named Markdown link:

```text
💬 [Dem Kunden auf WhatsApp schreiben](https://wa.me/4917630316130?text=…)
```

- number as bare international digits, no `+`, spaces, brackets or hyphens;
- text percent-encoded with `quote(text, safe="")`, so spaces, newlines, `&`,
  `?`, `#`, `%`, quotes, Unicode and emoji round-trip exactly;
- the length check applies to the finished ASCII URL after encoding: up to 2000
  characters keeps `?text=`, above that the plain `wa.me` URL is used and the
  text is **never** truncated or partially inserted;
- an unusable phone leaves the note without a link and logs a stable reason;
- the URL, its query string and the original text are never logged.

2000 is a conservative internal compatibility ceiling, **not** an official Meta
limit — Meta publishes no maximum for Click-to-Chat.

**The link does not bypass Meta's customer service window.** It only opens
WhatsApp and fills the composer; nothing is sent automatically. The message is
then sent by the operator from their own WhatsApp account, so that send may not
appear in the Chatwoot audit trail. `Originalnachricht` stays visible as before,
and no other failure note or the reopen-template behaviour is affected.
