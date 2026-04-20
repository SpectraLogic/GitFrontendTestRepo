package com.spectralogic.s3.common.dao.service.ds3;

import com.spectralogic.s3.common.dao.domain.ds3.LocalJobEntryWork;
import com.spectralogic.util.db.service.BaseService;

final class LocalJobEntryWorkServiceImpl
        extends BaseService<LocalJobEntryWork>
        implements LocalJobEntryWorkService {

    LocalJobEntryWorkServiceImpl() { super(LocalJobEntryWork.class); }
}