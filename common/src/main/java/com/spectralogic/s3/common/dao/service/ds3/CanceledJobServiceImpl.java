/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.ds3;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.CanceledJob;
import com.spectralogic.s3.common.dao.domain.ds3.Job;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.BaseService;
import com.spectralogic.util.lang.CollectionFactory;

final class CanceledJobServiceImpl extends BaseService< CanceledJob > implements CanceledJobService
{
    CanceledJobServiceImpl()
    {
        super( CanceledJob.class );
    }
    
    
    public void markAsTimedOut( final UUID jobId )
    {
        getDataManager().updateBeans(
                CollectionFactory.toSet( Job.TRUNCATED_DUE_TO_TIMEOUT ),
                BeanFactory.newBean( Job.class ).setTruncatedDueToTimeout( true ), 
                Require.beanPropertyEquals( Identifiable.ID, jobId ) );
        getDataManager().updateBeans(
                CollectionFactory.toSet( CanceledJob.CANCELED_DUE_TO_TIMEOUT ),
                BeanFactory.newBean( CanceledJob.class ).setCanceledDueToTimeout( true ), 
                Require.beanPropertyEquals( Identifiable.ID, jobId ) );
    }
}
