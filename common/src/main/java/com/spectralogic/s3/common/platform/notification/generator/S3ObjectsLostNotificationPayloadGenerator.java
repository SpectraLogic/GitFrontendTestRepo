/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.platform.notification.generator;

import java.util.Set;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.platform.domain.BlobApiBeanBuilder;
import com.spectralogic.s3.common.platform.notification.domain.payload.S3ObjectsLostNotificationPayload;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansRetriever;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.notification.domain.NotificationPayload;
import com.spectralogic.util.notification.domain.NotificationPayloadGenerator;

public final class S3ObjectsLostNotificationPayloadGenerator implements NotificationPayloadGenerator
{
    public S3ObjectsLostNotificationPayloadGenerator( 
            final Set< UUID > lostBlobIds,
            final BeansRetriever< Bucket > bucketRetriever,
            final BeansRetriever< S3Object > objectRetriever,
            final BeansRetriever< Blob > blobRetriever )
    {
        m_lostBlobIds = lostBlobIds;
        m_bucketRetriever = bucketRetriever;
        m_objectRetriever = objectRetriever;
        m_blobRetriever = blobRetriever;
        Validations.verifyNotNull( "Lost blob ids", m_lostBlobIds );
        Validations.verifyNotNull( "Bucket retriever", m_bucketRetriever );
        Validations.verifyNotNull( "Object retriever", m_objectRetriever );
        Validations.verifyNotNull( "Blob retriever", m_blobRetriever );
    }

    
    public NotificationPayload generateNotificationPayload()
    {
        final Set< Blob > blobs = m_blobRetriever.retrieveAll(
                Require.beanPropertyEqualsOneOf( Identifiable.ID, m_lostBlobIds ) ).toSet();
        final S3ObjectsLostNotificationPayload retval = 
                BeanFactory.newBean( S3ObjectsLostNotificationPayload.class );
        retval.setObjects( new BlobApiBeanBuilder( m_bucketRetriever, m_objectRetriever, blobs ).build() );
        return retval;
    }
    
    
    private final Set< UUID > m_lostBlobIds;
    private final BeansRetriever< Bucket > m_bucketRetriever;
    private final BeansRetriever< S3Object > m_objectRetriever;
    private final BeansRetriever< Blob > m_blobRetriever;
}
