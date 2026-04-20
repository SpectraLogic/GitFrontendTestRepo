package com.spectralogic.s3.common.dao.service.ds3;

import com.spectralogic.s3.common.dao.domain.ds3.S3JobEntryWork;
import com.spectralogic.util.db.service.BaseService;

final class S3JobEntryWorkServiceImpl
        extends BaseService<S3JobEntryWork>
        implements S3JobEntryWorkService {

    S3JobEntryWorkServiceImpl() { super(S3JobEntryWork.class); }
}