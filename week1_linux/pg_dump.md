`pg_dump -U username -d database_name -f backup.sql`

🔹 Options:

    -U username → Connect as this user

    -d database_name → Database to back up

    -f backup.sql → Output file

Restoring a Backup
Restore from a Plain SQL File

`psql -U username -d database_name -f backup.sql`

Advanced Usage
1. Dump Only Schema

`pg_dump -U username -d mydb --schema-only -f schema.sql`

✅ Use case: Backup just the database structure (tables, indexes, etc.).
2. Dump Only Data

`pg_dump -U username -d mydb --data-only -f data.sql`

✅ Use case: Backup only data without schema.
3. Backup a Specific Table

`pg_dump -U username -d mydb -t mytable -f mytable.sql`

✅ Use case: Export a single table instead of the entire database.


Compressing Backups
Gzip Compression

`pg_dump -U username -d mydb | gzip > backup.sql.gz`

✅ Use case: Reduce backup file size.
Restoring from Gzip Backup

`gunzip < backup.sql.gz | psql -U username -d mydb`