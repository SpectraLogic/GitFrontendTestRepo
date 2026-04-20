package com.spectralogic.s3.common.dao.service.ds3;

import com.spectralogic.s3.common.dao.domain.ds3.Ds3JobEntryWork;
import com.spectralogic.util.db.service.BaseService;

final class Ds3JobEntryWorkServiceImpl
        extends BaseService<Ds3JobEntryWork>
        implements Ds3JobEntryWorkService {

    Ds3JobEntryWorkServiceImpl() { super(Ds3JobEntryWork.class); }
}