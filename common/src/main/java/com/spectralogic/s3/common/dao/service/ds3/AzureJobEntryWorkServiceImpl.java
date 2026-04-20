package com.spectralogic.s3.common.dao.service.ds3;

import com.spectralogic.s3.common.dao.domain.ds3.AzureJobEntryWork;
import com.spectralogic.util.db.service.BaseService;

final class AzureJobEntryWorkServiceImpl
        extends BaseService<AzureJobEntryWork>
        implements AzureJobEntryWorkService {

    AzureJobEntryWorkServiceImpl() { super(AzureJobEntryWork.class); }
}