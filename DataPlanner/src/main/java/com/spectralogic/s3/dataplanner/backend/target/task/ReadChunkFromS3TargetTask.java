/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.target.task;

import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.JobEntry;
import com.spectralogic.s3.common.dao.domain.shared.BlobObservable;

import com.spectralogic.s3.common.dao.domain.target.*;

import com.spectralogic.s3.common.dao.service.ds3.JobEntryService;
import com.spectralogic.s3.common.dao.service.ds3.JobProgressManager;
import com.spectralogic.s3.common.dao.service.target.S3TargetBucketNameService;
import com.spectralogic.s3.common.dao.service.target.S3TargetFailureService;
import com.spectralogic.s3.common.dao.service.target.SuspectBlobS3TargetService;
import com.spectralogic.s3.common.platform.cache.DiskManager;
import com.spectralogic.s3.common.rpc.target.S3ConnectionFactory;
import com.spectralogic.s3.dataplanner.backend.frmwrk.ReadDirective;
import com.spectralogic.s3.dataplanner.backend.target.api.OfflineDataStagingWindowManager;
import com.spectralogic.util.bean.BeanCopier;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansServiceManager;


import java.util.*;


public final class ReadChunkFromS3TargetTask
    extends BaseReadChunkFromPublicCloudTask< S3Target, S3ConnectionFactory >
{

    public ReadChunkFromS3TargetTask(
            final OfflineDataStagingWindowManager offlineDataStagingWindowManager,
            final JobProgressManager jobProgressManager,
            final DiskManager diskManager,
            final BeansServiceManager serviceManager,
            final S3ConnectionFactory connectionFactory,
            final ReadDirective readDirective )
    {
        super( offlineDataStagingWindowManager,
                S3Target.class,
                S3TargetBucketNameService.class,
                S3TargetFailureService.class,
                diskManager,
                jobProgressManager,
                serviceManager,
                connectionFactory,
                readDirective );
        m_readDirective = readDirective;
    }

    public Collection<UUID> getChunkIds() {
        return BeanUtils.extractPropertyValues(getEntries(), Identifiable.ID);
    }

    @Override
    Set<UUID> getMissingBlobs() {
        List<JobEntry> jobEntries = m_readDirective.getEntries();
        Set<UUID> failedBlobIds = new HashSet<>();

        for (JobEntry entry : jobEntries) {
            UUID blobId = entry.getBlobId();
            Set<SuspectBlobS3Target> suspectBlobs = m_serviceManager.getRetriever(SuspectBlobS3Target.class).retrieveAll(Require.all(
                    Require.beanPropertyEqualsOneOf(
                            BlobTarget.TARGET_ID, m_targetId),
                    Require.beanPropertyEqualsOneOf(
                            BlobObservable.BLOB_ID, blobId))).toSet();
            if (!suspectBlobs.isEmpty()) {
                failedBlobIds.add(blobId);
            }
        }
        return failedBlobIds;
    }

    @Override
    protected void markBlobSuspect(UUID blobId) {
        final JobEntryService entryService = getServiceManager().getService( JobEntryService.class );
        Set<UUID> failedBlobIds = new HashSet<>();

        Set<BlobS3Target> blobTargets = m_serviceManager.getRetriever(BlobS3Target.class).retrieveAll(Require.all(
                Require.beanPropertyEqualsOneOf(
                        BlobTarget.TARGET_ID, m_targetId),
                Require.beanPropertyEqualsOneOf(
                        BlobObservable.BLOB_ID, blobId))).toSet();
        for ( final BlobS3Target blobTarget : blobTargets ) {
            Set<SuspectBlobS3Target> suspectBlobs = m_serviceManager.getRetriever(SuspectBlobS3Target.class).retrieveAll(Require.all(
                    Require.beanPropertyEqualsOneOf(
                            BlobTarget.TARGET_ID, m_targetId),
                    Require.beanPropertyEqualsOneOf(
                            BlobObservable.BLOB_ID, blobId))).toSet();
            if (suspectBlobs.isEmpty()) {
                final SuspectBlobS3Target bean = BeanFactory.newBean( SuspectBlobS3Target.class );
                BeanCopier.copy( bean, blobTarget );
                m_serviceManager.getService( SuspectBlobS3TargetService.class ).create( bean );
                failedBlobIds.add(blobId);
            }
            final Map<UUID, JobEntry> failedEntriesById = entryService.retrieveAll(
                    Require.all(
                            Require.beanPropertyEqualsOneOf(Identifiable.ID, getChunkIds()),
                            Require.beanPropertyEqualsOneOf(JobEntry.BLOB_ID, failedBlobIds)
                    )).toMap();
            for (JobEntry entry : failedEntriesById.values()) {
                LOG.info( "Job entry " + entry + " failed to read from S3 target. Marking it as requiring re-read from other source." );
                entryService.update( entry.setReadFromS3TargetId(null), JobEntry.READ_FROM_S3_TARGET_ID );
            }

        }

    }

    private ReadDirective m_readDirective;
}
