/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.target;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.target.PublicCloudTargetBucketName;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.service.api.BeanCreator;
import com.spectralogic.util.db.service.api.BeanDeleter;
import com.spectralogic.util.db.service.api.BeanUpdater;
import com.spectralogic.util.db.service.api.BeansRetriever;

public interface PublicCloudBucketNameService
    < T extends DatabasePersistable & PublicCloudTargetBucketName< T > >
    extends BeansRetriever< T >, 
            BeanCreator< T >, 
            BeanUpdater< T >,
            BeanDeleter
{
    String attainTargetBucketName( final UUID bucketId, final UUID targetId );
 
    
    String generateTargetBucketName( final String bucketName, final UUID targetId );
}
