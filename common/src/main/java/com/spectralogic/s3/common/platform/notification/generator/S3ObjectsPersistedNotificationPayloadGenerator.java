/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.platform.notification.generator;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.JobEntry;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.shared.BlobObservable;
import com.spectralogic.s3.common.platform.domain.BlobApiBeanBuilder;
import com.spectralogic.s3.common.platform.notification.domain.payload.S3ObjectsPersistedNotificationPayload;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.db.service.api.BeansRetriever;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.notification.domain.NotificationPayload;
import com.spectralogic.util.notification.domain.NotificationPayloadGenerator;

public final class S3ObjectsPersistedNotificationPayloadGenerator implements NotificationPayloadGenerator
{
    public S3ObjectsPersistedNotificationPayloadGenerator( 
            final Set<JobEntry> jobEntries,
            final BeansRetriever< S3Object > objectRetriever,
            final BeansRetriever< Blob > blobRetriever )
    {
        m_jobEntries = jobEntries;
        m_objectRetriever = objectRetriever;
        m_blobRetriever = blobRetriever;
        Validations.verifyNotNull( "Chunk", m_jobEntries );
        Validations.verifyNotNull( "Object retriever", m_objectRetriever );
        Validations.verifyNotNull( "Blob retriever", m_blobRetriever );
        if ( m_jobEntries.isEmpty() )
        {
            throw new IllegalArgumentException( 
                    "Must have at least 1 job entry to send a notification for." );
        }
    }

    
    public NotificationPayload generateNotificationPayload()
    {
        final Set< Blob > blobs = m_blobRetriever.retrieveAll(
                new HashSet<>( BeanUtils.<JobEntry, UUID >toMap(
                        m_jobEntries, BlobObservable.BLOB_ID ).values() ) ).toSet();
        final S3ObjectsPersistedNotificationPayload retval = 
                BeanFactory.newBean( S3ObjectsPersistedNotificationPayload.class );
        retval.setJobId( m_jobEntries.iterator().next().getJobId() );
        retval.setObjects( new BlobApiBeanBuilder( null, m_objectRetriever, blobs ).build() );
        return retval;
    }
    
    
    private final Set<JobEntry> m_jobEntries;
    private final BeansRetriever< S3Object > m_objectRetriever;
    private final BeansRetriever< Blob > m_blobRetriever;
}
