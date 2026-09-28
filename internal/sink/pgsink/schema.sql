-- Prospects and their outbox events. Deliberately NO unique index on (account_id, lower(email)):
-- many real CRM schemas lack one, so de-duplication must be correct at the application level.
CREATE TABLE IF NOT EXISTS prospects (
    id          bigserial PRIMARY KEY,
    account_id  bigint       NOT NULL,
    email       varchar(250),
    first_name  varchar(250),
    last_name   varchar(150),
    title       varchar(250),
    company     varchar(300),
    phone       varchar(20),
    linkedin    varchar(300),
    city        varchar(300),
    country     varchar(300),
    industry    varchar(300),
    source      varchar(32),
    import_id   varchar(64),
    deleted_at  timestamptz,
    created_at  timestamptz  NOT NULL DEFAULT now(),
    updated_at  timestamptz  NOT NULL DEFAULT now()
);
-- Selective column FIRST: with (account_id, x), a planner without stats scans the whole account through the index.
CREATE INDEX IF NOT EXISTS ix_prospects_email_account ON prospects (lower(email), account_id);
CREATE INDEX IF NOT EXISTS ix_prospects_linkedin_account ON prospects (linkedin, account_id);
CREATE INDEX IF NOT EXISTS ix_prospects_phone_account ON prospects (phone, account_id);

CREATE TABLE IF NOT EXISTS prospect_events (
    id          bigserial PRIMARY KEY,
    account_id  bigint      NOT NULL,
    prospect_id bigint      NOT NULL,
    event_type  varchar(32) NOT NULL,
    payload     text,
    created_at  timestamptz NOT NULL DEFAULT now()
);
