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
  - bot/automation targets without a native Chatwoot id,
  - cross-conversation targets,
  - missing targets.

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
