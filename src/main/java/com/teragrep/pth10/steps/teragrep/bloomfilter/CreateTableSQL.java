package com.teragrep.pth10.steps.teragrep.bloomfilter;

import java.util.regex.Pattern;

public class CreateTableSQL {
    private final String name;

    private void nameIsValid() {
        final Pattern pattern = Pattern.compile("^[A-Za-z0-9_]+$");
        if (!pattern.matcher(name).find()) {
            throw new RuntimeException("dpl.pth_06.bloom.table.name malformed name, only use alphabets, numbers and _");
        }
        if (name.length() > 100) {
            throw new RuntimeException(
                    "dpl.pth_06.bloom.table.name was too long, allowed maximum length is 100 characters"
            );
        }
    }
    public CreateTableSQL(String name) {
        this.name = name;
    }

    public String sql() {
        nameIsValid();
        return "CREATE TABLE IF NOT EXISTS `" + name + "`("
                + "`id` BIGINT UNSIGNED NOT NULL auto_increment PRIMARY KEY,"
                + "`partition_id` BIGINT UNSIGNED NOT NULL UNIQUE," + "`filter_type_id` BIGINT UNSIGNED NOT NULL,"
                + "`filter` LONGBLOB NOT NULL," + "CONSTRAINT `" + name
                + "_ibfk_1` FOREIGN KEY (filter_type_id) REFERENCES filtertype (id)" + "ON DELETE CASCADE,"
                + "CONSTRAINT `" + name + "_ibfk_2` FOREIGN KEY (partition_id) REFERENCES journaldb.logfile (id)"
                + "ON DELETE CASCADE" + ");";

    }

    public String ignoreConstraintsSql() {
        nameIsValid();
        return "CREATE TABLE IF NOT EXISTS `" + name + "`("
                + "`id` BIGINT UNSIGNED NOT NULL auto_increment PRIMARY KEY,"
                + "`partition_id` BIGINT UNSIGNED NOT NULL UNIQUE," + "`filter_type_id` BIGINT UNSIGNED NOT NULL,"
                + "`filter` LONGBLOB NOT NULL);";
    }

    @Override
    public boolean equals(final Object object) {
        if (this == object)
            return true;
        if (object == null)
            return false;
        if (object.getClass() != this.getClass())
            return false;
        final CreateTableSQL cast = (CreateTableSQL) object;
        return this.name.equals(cast.name);
    }
}
