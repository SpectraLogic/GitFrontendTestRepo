package com.spectralogic.s3.common.dao.service.ds3;

import com.spectralogic.s3.common.dao.domain.ds3.DataMigration;
import com.spectralogic.util.db.service.BaseService;

final class DataMigrationServiceImpl extends BaseService< DataMigration > implements DataMigrationService
{
    DataMigrationServiceImpl()
    {
        super( DataMigration.class );
    }
}
