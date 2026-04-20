package com.spectralogic.s3.common.dao.service.ds3;

import com.spectralogic.s3.common.dao.domain.ds3.PgStatAllTables;
import com.spectralogic.util.db.service.BaseService;

final class PgStatAllTablesServiceImpl extends BaseService<PgStatAllTables> implements PgStatAllTablesService {

    PgStatAllTablesServiceImpl() { super(PgStatAllTables.class); }
}
