CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    name TEXT,
    email TEXT,
    role TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS users_audit (
    id SERIAL PRIMARY KEY,
    user_id INTEGER,
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    changed_by TEXT,
    field_changed TEXT,
    old_value TEXT,
    new_value TEXT
);

CREATE OR REPLACE FUNCTION log_users_update()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.name IS DISTINCT FROM OLD.name THEN
        INSERT INTO users_audit (user_id, changed_by, field_changed, old_value, new_value)
        VALUES (OLD.id, current_user, 'name', OLD.name, NEW.name);
    END IF;
    
    IF NEW.email IS DISTINCT FROM OLD.email THEN
        INSERT INTO users_audit (user_id, changed_by, field_changed, old_value, new_value)
        VALUES (OLD.id, current_user, 'email', OLD.email, NEW.email);
    END IF;
    
    IF NEW.role IS DISTINCT FROM OLD.role THEN
        INSERT INTO users_audit (user_id, changed_by, field_changed, old_value, new_value)
        VALUES (OLD.id, current_user, 'role', OLD.role, NEW.role); 
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_log_users_update ON users;

CREATE TRIGGER trigger_log_users_update
AFTER UPDATE ON users
FOR EACH ROW
EXECUTE FUNCTION log_users_update();

CREATE EXTENSION IF NOT EXISTS pg_cron;

CREATE OR REPLACE FUNCTION export_to_csv()
RETURNS void AS $outer$
DECLARE
    path TEXT := '/tmp/users_audit_export_' || to_char(NOW(), 'YYYYMMDD_HH24MI') || '.csv';
BEGIN
    EXECUTE format(
        $inner$
        COPY (
            SELECT user_id, field_changed, old_value, new_value, changed_by, changed_at
            FROM users_audit
            WHERE changed_at >= NOW() - INTERVAL '1 day'
            ORDER BY changed_at
        ) TO %L WITH CSV HEADER
        $inner$,
        path
    );
END;
$outer$ LANGUAGE plpgsql;

SELECT cron.schedule(
    job_name := 'daily_audit_export',
    schedule := '0 3 * * *',
    command := $$SELECT export_to_csv()$$
);
