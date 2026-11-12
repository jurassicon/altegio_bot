-- Run with psql as documented in VOUCHER_PRODUCTION_MAILING_RUNBOOK.md §18.2.
DO $karlsruhe$
DECLARE
    operation constant text := current_setting('easyweek.template_action', true);
    template_code constant text := 'new_client_voucher_followup_v1';
    meta_name constant text := 'kitilash_ka_new_client_voucher_followup_v1';
    prefix constant text := 'Hallo {client_name}, für Sie ist bei uns ein Gutschein über 10 € hinterlegt. Termin buchen: {booking_link}. ';
    optout constant text := 'Wenn Sie keine Nachrichten mehr von uns möchten, antworten Sie mit STOP';
    previous_body constant text := prefix || optout || ' (СТОП).';
    approved_body constant text := prefix || chr(10) || optout || '.';
    existing public.message_templates%ROWTYPE;
    total bigint;
    matching bigint;
BEGIN
    IF operation IS NULL OR operation NOT IN ('transition', 'install') THEN
        RAISE EXCEPTION 'Specify transition or install';
    END IF;
    LOCK TABLE public.message_templates IN SHARE ROW EXCLUSIVE MODE;
    BEGIN
        SELECT * INTO STRICT existing FROM public.message_templates
        WHERE provider = 'easyweek' AND company_id = 322579
          AND code = template_code AND language = 'de';
    EXCEPTION
        WHEN too_many_rows THEN
            RAISE EXCEPTION 'Multiple Karlsruhe template rows; inspect before proceeding';
        WHEN no_data_found THEN
            IF operation = 'transition' THEN
                RAISE EXCEPTION 'Karlsruhe template missing; use the first-install procedure';
            END IF;
    END;

    IF existing.id IS NULL THEN
        INSERT INTO public.message_templates (provider, company_id, code, language, body, meta_template_name, is_active)
        VALUES ('easyweek', 322579, template_code, 'de', approved_body, meta_name, true);
        RAISE NOTICE 'Installed Karlsruhe follow-up template';
    ELSIF existing.is_active IS NOT TRUE OR existing.meta_template_name IS DISTINCT FROM meta_name THEN
        RAISE EXCEPTION 'Unexpected Karlsruhe template identity or inactive row';
    ELSIF existing.body = approved_body THEN
        RAISE NOTICE 'Karlsruhe follow-up template already matches';
    ELSIF operation = 'transition' AND existing.body = previous_body THEN
        UPDATE public.message_templates SET body = approved_body WHERE id = existing.id;
        RAISE NOTICE 'Updated Karlsruhe follow-up body';
    ELSE
        RAISE EXCEPTION 'Unexpected existing Karlsruhe body for this operation';
    END IF;

    SELECT count(*), count(*) FILTER (WHERE is_active AND meta_template_name = meta_name AND body = approved_body)
    INTO total, matching FROM public.message_templates
    WHERE provider = 'easyweek' AND company_id = 322579 AND code = template_code AND language = 'de';
    IF total <> 1 OR matching <> 1 THEN
        RAISE EXCEPTION 'Karlsruhe template verification failed';
    END IF;
END;
$karlsruhe$;
