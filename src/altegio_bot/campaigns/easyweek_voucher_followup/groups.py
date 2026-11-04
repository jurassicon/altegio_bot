"""Which of the three §46.3 audience groups one original slot belongs to.

The three groups, and the one answer that is not a group
-------------------------------------------------------
* ``read`` — a read receipt arrived for this slot's own message;
* ``delivered_unread`` — a delivery receipt arrived and no read receipt did;
* ``confirmed_failure`` — PR-22.1 holds a confirmed failure fact about this
  slot's one attempt.

Everything else is ``followup_evidence_unproven``, and the list of "everything
else" is the reason this module exists. Accepted by Meta and never heard of
again, a send that timed out, ``send_unknown``, a slot whose attempt was never
completed, a local parameter refusal that never left the process, a CREATE or
PAY error, a slot that was never attempted at all: none of them is a group. In
particular none of them is "did not read". §46.3 is blunt about this and the
Ops screen repeats it: no read receipt does not prove nobody read the message —
a device can be configured never to send one — so "delivered, read not
confirmed" is the strongest claim the second group may make.

Precedence
----------
Read outranks delivered, and both outrank a failure fact, however late it
arrived. A ``failed`` callback that lands after a delivery is a contradiction in
the provider's own account of one message; PR-22.1 already refuses to bind such
a fact to the slot, and this module refuses it a second time from the other
side, so that one path's bug cannot quietly promote somebody into the failure
group. A person whose device showed the voucher is not somebody whose voucher
failed to arrive.

What a status timestamp is, and is not
--------------------------------------
``delivered_at`` and ``read_at`` on the slot are the MAILING's own record. They
are applied by a transition matched on the exact message id, deliberately
outside the §42 fence, because dropping a status about a message already sent
would corrupt the history of a mailing that really happened.

They are not proof for a NEW send. §46.3 asks a status for a verified inbound
context with an exact sender and message id, and the transition records neither
the signature nor the route — the ingress strips Meta's signature header out of
the stored copy, so no later reader can recover it. PR-22.2 therefore writes
that proof down beside the transition, and a slot with a status but no proof is
``unproven`` here: a historical row, or a callback whose route did not match.

Identity, never proximity
-------------------------
A group is only ever read off facts tied to this slot by exact identity: its own
``provider_message_id``, its own single attempt row, and the sender and recipient
digests that attempt froze before the request left. Nothing here matches on a
phone number, on a batch-wide timestamp or on how close two events happen to be.
A slot whose attempt froze no route, or whose attempt names a different message
than the slot does, proves nothing and is reported as such.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

from altegio_bot.campaigns.easyweek_voucher_followup.failure_policy import FailureFact
from altegio_bot.campaigns.easyweek_voucher_followup.identity import (
    EVIDENCE_UNPROVEN,
    FOLLOWUP_GROUP_DELIVERED,
    FOLLOWUP_GROUP_FAILED,
    FOLLOWUP_GROUP_READ,
    SENDER_UNPROVEN,
    SOURCE_ATTEMPT_UNPROVEN,
    STATUS_EVIDENCE_UNPROVEN,
)

__all__ = ["GroupVerdict", "SourceEvidence", "resolve_group"]


@dataclass(frozen=True)
class SourceEvidence:
    """Everything about one original slot a group decision may rest on.

    A value object rather than the ORM rows, for the same reason
    :class:`~altegio_bot.campaigns.easyweek_voucher_followup.failure_policy.FailureFact`
    is one: the decision is then stateless, testable in one line, and cannot
    reach for a phone number, a rendered body or a voucher code that is not its
    business.
    """

    item_id: int
    slot: int
    status: str
    send_attempt_count: int
    send_attempted_at: datetime | None
    delivered_at: datetime | None
    read_at: datetime | None
    item_message_id: str | None

    # The one attempt's frozen route and identity, or ``None`` where no attempt
    # row exists at all.
    attempt_message_id: str | None = None
    attempt_sender_digest: str | None = None
    attempt_recipient_digest: str | None = None

    # Every CONFIRMED failure fact bound to this slot. Plural on purpose: the
    # policy has to see all of them, because one inadmissible reason among
    # several is a veto rather than something to average away.
    confirmed_failures: tuple[FailureFact, ...] = field(default_factory=tuple)

    # Whether §46.3's proof exists for each positive status: a callback whose
    # signature the ingress verified, naming this slot's exact message id, on
    # the route the attempt froze. The slot's own ``delivered_at`` / ``read_at``
    # are the MAILING's record and are not that proof — they are written by a
    # transition that records neither the signature nor the route, and the
    # signature is unrecoverable afterwards.
    delivered_receipt_proven: bool = False
    read_receipt_proven: bool = False


@dataclass(frozen=True)
class GroupVerdict:
    """The group, or the safe reason why this slot has none."""

    group: str | None
    reason: str | None = None

    @property
    def proven(self) -> bool:
        return self.group is not None


def _attempt_is_proven(evidence: SourceEvidence) -> bool:
    """Is this slot's single attempt a usable, self-consistent record?

    Four things, all required:

    * the attempt really happened — a claim that was written and a counter that
      says one send was made;
    * the attempt froze both halves of its route, so the message is known to
      have gone from our sender to the addressee's number as it stood then. A
      historical attempt that froze nothing is honest about it and simply proves
      no route, which is not enough for a follow-up;
    * the attempt and the slot agree about the message identifier whenever both
      name one. A disagreement means two different messages are being treated as
      one, which is exactly the substitution §46.1 forbids.
    """
    if evidence.send_attempted_at is None or int(evidence.send_attempt_count or 0) < 1:
        return False
    if evidence.attempt_sender_digest is None or evidence.attempt_recipient_digest is None:
        return False
    if (
        evidence.item_message_id is not None
        and evidence.attempt_message_id is not None
        and evidence.item_message_id != evidence.attempt_message_id
    ):
        return False
    return True


def resolve_group(evidence: SourceEvidence, *, expected_sender_digest: str | None = None) -> GroupVerdict:
    """The one group this slot belongs to, or why it belongs to none.

    ``expected_sender_digest`` is the branch's currently confirmed sender. A
    slot whose attempt went out from a different line is not part of this send
    geography, and §46.1 asks for that to be an exclusion with a reason rather
    than a silent inclusion under today's sender.
    """
    if not _attempt_is_proven(evidence):
        return GroupVerdict(None, SOURCE_ATTEMPT_UNPROVEN)
    if expected_sender_digest is not None and evidence.attempt_sender_digest != expected_sender_digest:
        return GroupVerdict(None, SENDER_UNPROVEN)

    # Read first, and unconditionally — but only on §46.3's proof. A read is
    # the strongest evidence there is and nothing arriving later can unsay it;
    # what makes it evidence is the verified signature and the matching route,
    # not the timestamp the transition left behind.
    if evidence.read_at is not None:
        if evidence.read_receipt_proven:
            return GroupVerdict(FOLLOWUP_GROUP_READ)
        # The mailing's record says read and this phase cannot prove it. Never
        # demoted to "delivered, not read": that would state something weaker
        # than what the record says and stronger than what is proven.
        return GroupVerdict(None, STATUS_EVIDENCE_UNPROVEN)

    if evidence.delivered_at is not None:
        if evidence.delivered_receipt_proven:
            return GroupVerdict(FOLLOWUP_GROUP_DELIVERED)
        return GroupVerdict(None, STATUS_EVIDENCE_UNPROVEN)

    if evidence.confirmed_failures:
        # The group, not yet the eligibility: whether the FAILURE REASON admits
        # a follow-up at all is the versioned policy's decision, taken where
        # every other eligibility check is taken.
        return GroupVerdict(FOLLOWUP_GROUP_FAILED)

    # Accepted-only, unknown, unsent, locally refused, never attempted. Honest
    # ignorance, and never a group.
    return GroupVerdict(None, EVIDENCE_UNPROVEN)
