"""Operator-only controlled EasyWeek voucher mutation canary (INTEGRATION_PLAN §35).

Nothing in this package sends a customer message, creates a campaign row, opens
campaign execution or changes a readiness fence. It exists to answer, once and
under an operator's hand, what a real EasyWeek voucher sale actually does — and
to be able to undo it.
"""
