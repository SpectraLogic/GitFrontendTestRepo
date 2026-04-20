package com.spectralogic.s3.common.dao.domain.ds3;

import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.lang.DatabaseView;
import com.spectralogic.util.db.lang.ViewDefinition;
import com.spectralogic.util.db.lang.shared.ExcludeFromDatabasePersistence;

import java.util.UUID;

@ViewDefinition(
        "select n_tup_del as job_entry_n_tup_del from pg_stat_all_tables where relname='job_entry'"
)
public interface PgStatAllTables extends DatabasePersistable, DatabaseView {

    @ExcludeFromDatabasePersistence
    UUID getId();

    String JOB_ENTRY_N_TUP_DEL = "jobEntryNTupDel";

    long getJobEntryNTupDel();

    PgStatAllTables setJobEntryNTupDel(final long value);

}
