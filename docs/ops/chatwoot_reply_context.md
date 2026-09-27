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

## Reactions to automatic messages — the native marker contract

A bot/automation send has no Chatwoot message of its own: it exists there only as
the **private mirror note** posted by `ChatwootClient.mirror_outbound_as_note`.
So a reaction to an automatic message can be rendered as a native Chatwoot reply
only once that note has been *proven*.

### What is written

The mirror note carries exactly two technical `content_attributes`, through the
REST API only:

```json
{
  "altegio_bot_message_kind": "whatsapp_outbound_mirror_v1",
  "whatsapp_provider_message_id": "<exact Meta wamid>"
}
```

The wamid reaches the client as its own named argument from
`ChatwootHybridProvider`; no internal meta dict is forwarded. The version suffix
is part of the contract — bump it if the shape ever changes, and the old value
stops being accepted.

### What is accepted as proof

`ChatwootClient.find_outbound_mirror_note(conversation_id, wamid)` accepts a
message only when **every** condition holds at once:

| Condition | Why |
| --- | --- |
| listed by the destination conversation, and its own `conversation_id` matches | no cross-conversation `in_reply_to` |
| `message_type` is outgoing | an inbound message is never a mirror note |
| `private` is exactly `true` | a public message is not a mirror note |
| marker equals `whatsapp_outbound_mirror_v1` | version-pinned contract |
| `whatsapp_provider_message_id` equals the reaction target wamid exactly | the reaction must hit *that* message |
| exactly one match across the whole walked history, id a positive integer | ambiguity is never resolved by picking one |

Nothing is matched by body text, `template_code`, `created_at`, "the last
message" or result order.

### Bounded pagination

Chatwoot serves one page of a conversation per request — `_MIRROR_NOTE_PAGE_SIZE`
= 20 messages, newest first — so a mirror note in a busy conversation is not on
the first page. The lookup therefore walks backwards:

1. the first request carries no query cursor;
2. each following request carries `before=<smallest usable message id of the
   previous page>`;
3. the cursor must **strictly decrease**;
4. matches are collected across every page and deduplicated by Chatwoot message
   id, so one row served on both sides of a page boundary stays one match, while
   two distinct ids with the same marker stay an ambiguity;
5. the walk ends when a page comes back shorter than the page size — an empty
   page included — which is the proof that the walked history ended;
6. only then may a single match be trusted. The lookup never returns early on
   the first hit.

The walk is bounded by `_MIRROR_NOTE_MAX_PAGES` = 10 pages (~200 messages). The
budget is deliberately small: the lookup is best-effort, runs inline on the
inbound reaction path, and each page is one more Chatwoot round trip. There is no
unbounded network scan and no possibility of an infinite loop.

**This is a bounded recent-window proof, not a full-history search.** In a
conversation with more than roughly 200 messages newer than the mirror note the
budget runs out before the end of history is proven, and the reaction keeps the
visible quote. Raising `_MIRROR_NOTE_MAX_PAGES` widens the window at the cost of
more Chatwoot requests per reaction.

### Fail-closed fallback

All of these produce the visible quote instead — a short single-line preview of
the original text plus the emoji, and no `in_reply_to`:

- zero matches, or two or more distinct matches;
- an HTTP status other than 200, or a transport error, on **any** page;
- a body that is not JSON, or not a recognizable messages payload, on any page
  (a malformed page is never read as "no more messages");
- a full page that yields no usable id for the next cursor;
- a cursor that would not strictly decrease, which is what a replayed page looks
  like;
- the page budget running out before the end of history is proven — **even when
  exactly one match was already collected**;
- a conversation mismatch.

Page-level logs carry only the conversation id, whether a cursor was in use, the
page/message counts and a stable reason code (`transport_error`, `http_status`,
`malformed_json`, `malformed_payload`, `page_budget_exhausted`,
`no_pagination_cursor`, `cursor_not_advancing`, `match_count`). No wamid, phone,
message body, URL, token or response body is ever logged.

Example of the resulting body:

```text
↩️ Ответ на сообщение:
«Ваша запись завтра в 10:00»

👍
```

Reaction removal keeps the same context and shows `Реакция удалена в WhatsApp` in
place of the emoji. A failing lookup costs the native link, never the reaction
itself, and never the Meta send: the Chatwoot mirror stays best-effort.

### No historical backfill

Mirror notes created before this change carry no marker, so reactions to them
keep the visible quote. There is **no migration and no backfill**. An
accidentally populated `chatwoot_message_id` / `chatwoot_conversation_id` on a
bot Outbox row remains no evidence: only the proven marker can make a bot target
native.

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
