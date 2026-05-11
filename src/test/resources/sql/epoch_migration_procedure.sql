CREATE PROCEDURE epochMigrationProcedure()
BEGIN
    START TRANSACTION;

    INSERT IGNORE INTO object_format (name)
    SELECT DISTINCT t.object_format
    FROM epoch_migration_temp_table t
             LEFT JOIN object_format f
                       ON f.name = t.object_format
    WHERE f.id IS NULL;

    UPDATE logfile l
        INNER JOIN epoch_migration_temp_table t
        ON l.id = t.logfile_id
        INNER JOIN object_format f
        ON f.name = t.object_format
    SET l.epoch_hour       = t.epoch_hour,
        l.object_format_id = f.id;
    COMMIT;
END;