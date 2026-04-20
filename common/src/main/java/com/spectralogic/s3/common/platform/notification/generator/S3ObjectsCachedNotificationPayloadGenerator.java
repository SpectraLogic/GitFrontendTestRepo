/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.platform.notification.generator;

import java.util.Collection;
import java.util.Set;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.JobEntry;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.shared.BlobObservable;
import com.spectralogic.s3.common.platform.domain.BlobApiBeanBuilder;
import com.spectralogic.s3.common.platform.notification.domain.payload.S3ObjectsCachedNotificationPayload;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansRetriever;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.notification.domain.NotificationPayload;
import com.spectralogic.util.notification.domain.NotificationPayloadGenerator;
import lombok.NonNull;

public final class S3ObjectsCachedNotificationPayloadGenerator implements NotificationPayloadGenerator
{
    public S3ObjectsCachedNotificationPayloadGenerator(
            @NonNull final UUID jobId,
            @NonNull final Collection<JobEntry> chunks,
            @NonNull final BeansRetriever< S3Object > objectRetriever,
            @NonNull final BeansRetriever< Blob > blobRetriever )
    {
        m_jobId = jobId;
        m_chunks = chunks;
        m_objectRetriever = objectRetriever;
        m_blobRetriever = blobRetriever;
        Validations.verifyNotNull( "Chunk", m_chunks );
        Validations.verifyNotNull( "Object retriever", m_objectRetriever );
        Validations.verifyNotNull( "Blob retriever", m_blobRetriever );
    }

    
    public NotificationPayload generateNotificationPayload()
    {
        final Set< Blob > blobs = m_blobRetriever.retrieveAll(
                Require.exists( 
                        JobEntry.class,
                        BlobObservable.BLOB_ID,
                        Require.beanPropertyEqualsOneOf( JobEntry.ID, BeanUtils.extractPropertyValues(m_chunks, Identifiable.ID) ) ) ).toSet();
        final S3ObjectsCachedNotificationPayload retval = 
                BeanFactory.newBean( S3ObjectsCachedNotificationPayload.class );
        retval.setJobId( m_jobId );
        retval.setObjects( new BlobApiBeanBuilder( null, m_objectRetriever, blobs ).build() );
        return retval;
    }
    

    private final UUID m_jobId;
    private final Collection<JobEntry> m_chunks;
    private final BeansRetriever< S3Object > m_objectRetriever;
    private final BeansRetriever< Blob > m_blobRetriever;
}
