package com.spectralogic.s3.common.dao.service.ds3;

import com.spectralogic.s3.common.dao.domain.ds3.Obsoletion;
import com.spectralogic.util.db.service.BaseService;

final class ObsoletionServiceImpl extends BaseService< Obsoletion > implements ObsoletionService
{
    ObsoletionServiceImpl()
    {
        super( Obsoletion.class );
    }
}
