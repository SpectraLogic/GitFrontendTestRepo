/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.target.task;

import com.azure.core.exception.AzureException;
import com.azure.core.exception.HttpResponseException;
import com.spectralogic.s3.common.dao.domain.ds3.JobChunkBlobStoreState;
import com.spectralogic.s3.common.dao.domain.ds3.JobEntry;
import com.spectralogic.s3.common.dao.domain.shared.BlobObservable;
import com.spectralogic.s3.common.dao.domain.target.*;
import com.spectralogic.s3.common.dao.service.ds3.JobEntryService;
import com.spectralogic.s3.common.dao.service.ds3.JobProgressManager;
import com.spectralogic.s3.common.dao.service.target.AzureTargetBucketNameService;
import com.spectralogic.s3.common.dao.service.target.AzureTargetFailureService;
import com.spectralogic.s3.common.dao.service.target.SuspectBlobAzureTargetService;
import com.spectralogic.s3.common.platform.cache.DiskManager;
import com.spectralogic.s3.common.rpc.target.AzureConnectionFactory;
import com.spectralogic.s3.dataplanner.backend.frmwrk.ReadDirective;
import com.spectralogic.s3.dataplanner.backend.target.api.OfflineDataStagingWindowManager;
import com.spectralogic.util.bean.BeanCopier;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansServiceManager;

import java.util.*;
import java.util.concurrent.ExecutionException;

public final class ReadChunkFromAzureTargetTask
    extends BaseReadChunkFromPublicCloudTask< AzureTarget, AzureConnectionFactory >
{

    public ReadChunkFromAzureTargetTask(
            final OfflineDataStagingWindowManager offlineDataStagingWindowManager,
            final JobProgressManager jobProgressManager,
            final DiskManager diskManager,
            final BeansServiceManager serviceManager,
            final AzureConnectionFactory connectionFactory,
            final ReadDirective readDirective )
    {
        super( offlineDataStagingWindowManager,
                AzureTarget.class,
                AzureTargetBucketNameService.class,
                AzureTargetFailureService.class,
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
    protected void markBlobSuspect(UUID blobId) {
        final JobEntryService entryService = getServiceManager().getService( JobEntryService.class );
        Set<UUID> failedBlobIds = new HashSet<>();
        Set<BlobAzureTarget> blobTargets = m_serviceManager.getRetriever(BlobAzureTarget.class).retrieveAll(Require.all(
                Require.beanPropertyEqualsOneOf(
                        BlobTarget.TARGET_ID, this.m_targetId),
                Require.beanPropertyEqualsOneOf(
                        BlobObservable.BLOB_ID, blobId))).toSet();
        for ( final BlobAzureTarget blobTarget : blobTargets )
        {
            Set<SuspectBlobAzureTarget> suspectBlobs = m_serviceManager.getRetriever(SuspectBlobAzureTarget.class).retrieveAll(Require.all(
                    Require.beanPropertyEqualsOneOf(
                            BlobTarget.TARGET_ID, m_targetId),
                    Require.beanPropertyEqualsOneOf(
                            BlobObservable.BLOB_ID, blobId))).toSet();
            if (suspectBlobs.isEmpty()) {
                final SuspectBlobAzureTarget bean = BeanFactory.newBean( SuspectBlobAzureTarget.class );
                BeanCopier.copy( bean, blobTarget );
                m_serviceManager.getService( SuspectBlobAzureTargetService.class ).create( bean );
            }
            final Map<UUID, JobEntry> failedEntriesById = entryService.retrieveAll(
                    Require.all(
                            Require.beanPropertyEqualsOneOf(Identifiable.ID, getChunkIds()),
                            Require.beanPropertyEqualsOneOf(JobEntry.BLOB_ID, failedBlobIds)
                    )).toMap();
            for (JobEntry entry : failedEntriesById.values()) {
                LOG.info( "Job entry " + entry + " failed to read from Azure target. Marking it as requiring re-read from other source." );
                entryService.update( entry.setReadFromAzureTargetId(null), JobEntry.READ_FROM_AZURE_TARGET_ID );
            }

        }

    }

    @Override
    Set<UUID> getMissingBlobs() {
        List<JobEntry> jobEntries = m_readDirective.getEntries();
        Set<UUID> failedBlobIds = new HashSet<>();

        for (JobEntry entry : jobEntries) {
            UUID blobId = entry.getBlobId();
            Set<SuspectBlobAzureTarget> suspectBlobs = m_serviceManager.getRetriever(SuspectBlobAzureTarget.class).retrieveAll(Require.all(
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


    private ReadDirective m_readDirective;
}
