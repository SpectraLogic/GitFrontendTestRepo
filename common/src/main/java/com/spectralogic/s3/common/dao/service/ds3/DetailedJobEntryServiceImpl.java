package com.spectralogic.s3.common.dao.service.ds3;

import com.spectralogic.s3.common.dao.domain.ds3.DetailedJobEntry;
import com.spectralogic.s3.common.dao.domain.ds3.DetailedLocalBlobDestination;
import com.spectralogic.util.db.service.BaseService;
import com.spectralogic.util.db.service.api.BeansRetriever;

final class DetailedJobEntryServiceImpl
        extends BaseService<DetailedJobEntry>
        implements DetailedJobEntryService {

    DetailedJobEntryServiceImpl() { super(DetailedJobEntry.class); }
}
