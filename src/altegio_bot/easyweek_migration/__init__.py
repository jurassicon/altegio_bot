"""One-off Altegio → EasyWeek cutover of FUTURE ACTIVE bookings (PR-11.1).

Scope, in one sentence: fill the EasyWeek schedule of Karlsruhe (Altegio
``758285``) and Rastatt (Altegio ``1271200``) with the bookings that have not
happened yet, so that switching the branches over does not lose anybody's
appointment.

What this package deliberately does **not** do:

* it does not migrate history — past, completed and cancelled bookings are never
  created in EasyWeek;
* it does not write visit counters through the API. Historical counters arrive
  via EasyWeek's own customer import, the canary proved EasyWeek keeps that
  baseline, and PR-11 stores the ``visits_total`` EasyWeek states afterwards;
* it does not touch Durlach, which does not exist in Altegio at all;
* it does not create customers, guess a mapping, or send a customer anything.

The modules split along the lines the safety argument needs:

``manifest``      explicit, verifiable location/staff/service mapping (no PII)
``altegio_source``  the SOURCE OF TRUTH — the Altegio API, not our local tables
``cutover``       one immutable UTC instant + explicit timezone/DST handling
``customers``     exact phone match against an operator-supplied EasyWeek export
``classify``      ready / already_migrated / blocked / skipped, fail-closed
``gates``         everything that must be true before the first mutation
``write_client``  the ONLY module allowed to mutate EasyWeek
``ledger``        durable idempotency and the uncertain-result contract
``report``        machine-readable, PII-free reconciliation output
``rollback``      read-only by default, run-scoped, refuses hand-edited targets
``cli``           inventory / dry-run / apply / reconcile / rollback

Nothing in this package imports ``MessageJob``, ``OutboxMessage`` or any review
path. A migrated booking is a schedule row, not a conversation, and the cutover
must not produce a single customer message.
"""
