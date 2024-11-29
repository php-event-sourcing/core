CREATE TABLE events
(
    id             SERIAL PRIMARY KEY,
    payload        TEXT NOT NULL,
    transaction_id XID8 DEFAULT pg_current_xact_id()
);

-- Optionally, create a table to track processed rows
CREATE TABLE position_tracker
(
    id            SERIAL PRIMARY KEY,
    last_position BIGINT DEFAULT 0
);

DO
$$
    BEGIN
        FOR i IN 1..1000000
            LOOP
                INSERT INTO events (payload) VALUES ('Event ' || i);
            END LOOP;
    END
$$;