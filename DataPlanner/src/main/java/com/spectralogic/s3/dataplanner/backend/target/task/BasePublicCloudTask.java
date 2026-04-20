/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.target.task;

import com.spectralogic.s3.common.dao.domain.ds3.BlobStoreTaskPriority;
import com.spectralogic.s3.common.dao.domain.target.PublicCloudReplicationTarget;
import com.spectralogic.s3.common.dao.service.ds3.JobProgressManager;
import com.spectralogic.s3.common.dao.service.target.PublicCloudBucketNameService;
import com.spectralogic.s3.common.platform.cache.DiskManager;
import com.spectralogic.s3.common.platform.target.PublicCloudBucketSupport;
import com.spectralogic.s3.common.platform.target.PublicCloudBucketSupportImpl;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.service.api.BeansServiceManager;

import java.util.UUID;

abstract class BasePublicCloudTask< T extends DatabasePersistable & PublicCloudReplicationTarget< T >, CF >
    extends BaseTargetTask< T, CF >
{
    protected BasePublicCloudTask(
            final Class< ? extends PublicCloudBucketNameService< ? > > bucketNameServiceType,
            final Class< T > targetType,
            final UUID targetId,
            final DiskManager diskManager,
            final JobProgressManager jobProgressManager,
            final BeansServiceManager serviceManager,
            final CF connectionFactory,
            final BlobStoreTaskPriority priority )
    {
        super( targetType, targetId, diskManager, jobProgressManager, serviceManager, connectionFactory, priority );
        m_bucketNameServiceType = bucketNameServiceType;
    }
    
    
    final protected PublicCloudBucketSupport getCloudBucketSupport()
    {
        synchronized ( m_lock )
        {
            if ( null == m_cloudBucketSupport )
            {
                m_cloudBucketSupport = 
                        new PublicCloudBucketSupportImpl( m_bucketNameServiceType, getServiceManager() );
            }
            return m_cloudBucketSupport;
        }
    }
    
    
    private PublicCloudBucketSupport m_cloudBucketSupport;
    private final Class< ? extends PublicCloudBucketNameService< ? > > m_bucketNameServiceType;
    private final Object m_lock = new Object();
}
