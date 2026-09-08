"""Provider-neutral campaign data contracts."""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime

from altegio_bot.models.models import PROVIDER_ALTEGIO


@dataclass(frozen=True)
class ClientSnapshot:
    """Detached client identity used while a source CRM is queried."""

    id: int | None
    company_id: int
    altegio_client_id: int | None
    display_name: str | None
    phone_e164: str | None
    wa_opted_out: bool
    provider: str = PROVIDER_ALTEGIO


@dataclass
class ClientCandidate:
    """Provider-neutral candidate plus the current campaign diagnostics."""

    client: ClientSnapshot
    total_records_in_period: int
    confirmed_records_in_period: int
    lash_records_in_period: int
    confirmed_lash_records_in_period: int
    service_titles_in_period: list[str]
    records_before_period: int
    records_after_period: int = field(default=0)
    local_client_found: bool = field(default=True)
    excluded_reason: str | None = field(default=None)
    # PR-14 durable local source proof.  Populated only for eligible EasyWeek
    # preview recipients; Altegio and excluded EasyWeek rows remain NULL.
    source_easyweek_event_id: int | None = field(default=None)
    source_record_id: int | None = field(default=None)
    source_booking_uuid: uuid.UUID | None = field(default=None)
    source_visits_total: int | None = field(default=None)
    source_visits_total_updated_at: datetime | None = field(default=None)

    @property
    def is_eligible(self) -> bool:
        return self.excluded_reason is None
