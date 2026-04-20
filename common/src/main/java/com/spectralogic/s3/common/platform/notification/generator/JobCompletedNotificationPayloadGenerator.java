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
import com.spectralogic.s3.common.dao.domain.ds3.JobEntry;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.shared.BlobObservable;
import com.spectralogic.s3.common.platform.domain.BlobApiBeanBuilder;
import com.spectralogic.s3.common.platform.notification.domain.payload.JobCompletedNotificationPayload;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansRetriever;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.notification.domain.NotificationPayload;
import com.spectralogic.util.notification.domain.NotificationPayloadGenerator;

public final class JobCompletedNotificationPayloadGenerator implements NotificationPayloadGenerator
{
    public JobCompletedNotificationPayloadGenerator( 
            final UUID jobId,
            final boolean truncatedOrCanceled,
            final BeansRetriever< S3Object > objectRetriever,
            final BeansRetriever< Blob > blobRetriever )
    {
        m_jobId = jobId;
        m_truncatedOrCanceled = truncatedOrCanceled;
        m_objectRetriever = objectRetriever;
        m_blobRetriever = blobRetriever;
        Validations.verifyNotNull( "Job", m_jobId );
        Validations.verifyNotNull( "Object retriever", m_objectRetriever );
        Validations.verifyNotNull( "Blob retriever", m_blobRetriever );
    }

    
    public NotificationPayload generateNotificationPayload()
    {
        final Set< Blob > blobs = m_blobRetriever.retrieveAll(
                Require.exists( 
                        JobEntry.class,
                        BlobObservable.BLOB_ID,
                        Require.beanPropertyEquals( JobEntry.JOB_ID, m_jobId ) ) ).toSet();
        final JobCompletedNotificationPayload retval = 
                BeanFactory.newBean( JobCompletedNotificationPayload.class );
        retval.setJobId( m_jobId );
        retval.setObjectsNotPersisted( new BlobApiBeanBuilder( null, m_objectRetriever, blobs ).build() );
        //NOTE: if the job was canceled but not truncated, blobs should not be empty, so check for blobs should be redundant
        retval.setCancelOccurred( m_truncatedOrCanceled || !blobs.isEmpty() );
        return retval;
    }
    
    
    private final UUID m_jobId;
    private final boolean m_truncatedOrCanceled;
    private final BeansRetriever< S3Object > m_objectRetriever;
    private final BeansRetriever< Blob > m_blobRetriever;
}
