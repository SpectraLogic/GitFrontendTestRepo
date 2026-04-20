/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.target;

import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.shared.BlobObservable;
import com.spectralogic.s3.common.dao.domain.target.*;
import com.spectralogic.s3.common.dao.service.ds3.JobProgressManager;
import com.spectralogic.s3.common.dao.service.target.ImportS3TargetDirectiveService;
import com.spectralogic.s3.common.dao.service.target.S3TargetFailureService;
import com.spectralogic.s3.common.dao.service.target.SuspectBlobAzureTargetService;
import com.spectralogic.s3.common.dao.service.target.SuspectBlobS3TargetService;
import com.spectralogic.s3.common.platform.cache.DiskManager;
import com.spectralogic.s3.common.rpc.target.AzureConnection;
import com.spectralogic.s3.common.rpc.target.PublicCloudConnection;
import com.spectralogic.s3.common.rpc.target.S3Connection;
import com.spectralogic.s3.common.rpc.target.S3ConnectionFactory;
import com.spectralogic.s3.dataplanner.backend.frmwrk.IODirective;
import com.spectralogic.s3.dataplanner.backend.frmwrk.ReadDirective;
import com.spectralogic.s3.dataplanner.backend.frmwrk.S3TargetWorkAggregationUtils;
import com.spectralogic.s3.dataplanner.backend.frmwrk.TargetWriteDirective;
import com.spectralogic.s3.dataplanner.backend.frmwrk.WorkAggregationUtils;
import com.spectralogic.s3.dataplanner.backend.target.api.PublicCloudTargetBlobStore;

import com.spectralogic.s3.dataplanner.backend.target.task.ImportS3TargetTask;
import com.spectralogic.s3.dataplanner.backend.target.task.ReadChunkFromS3TargetTask;
import com.spectralogic.s3.dataplanner.backend.target.task.WriteChunkToS3TargetTask;

import com.spectralogic.util.db.service.api.BeansServiceManager;
import lombok.NonNull;

import org.apache.log4j.Logger;


public class S3TargetBlobStore
    extends BasePublicCloudTargetBlobStore
            < S3Target, S3ConnectionFactory, S3TargetFailureService, ImportS3TargetDirective > 
    implements PublicCloudTargetBlobStore< ImportS3TargetDirective >
{
    private static final Logger LOG = Logger.getLogger( S3TargetBlobStore.class );

    public S3TargetBlobStore(
            final S3ConnectionFactory azureConnectionFactory,
            final DiskManager diskManager,
            final JobProgressManager jobProgressManager,
            final BeansServiceManager serviceManager )
    {
        super( S3Target.class, 
                S3TargetFailureService.class,
                ImportS3TargetDirectiveService.class,
                azureConnectionFactory, 
                FeatureKeyType.AWS_S3_CLOUD_OUT,
                SystemFailureType.AWS_S3_WRITES_REQUIRE_FEATURE_LICENSE,
                diskManager, 
                jobProgressManager,
                serviceManager );
    }


    @Override
    protected void verifyCanConnect( final S3Target target )
    {
        m_connectionFactory.connect( target ).shutdown();
    }


    @Override
    protected ImportS3TargetTask createImportTask( final ImportS3TargetDirective directive )
    {
        return new ImportS3TargetTask( directive.getPriority(),
                directive.getTargetId(),
                m_diskManager,
                m_jobProgressManager,
                m_serviceManager,
                m_connectionFactory);
    }

    public void write(@NonNull final TargetWriteDirective<S3Target, S3BlobDestination> writeInfo) {
        addIoTask(
                new WriteChunkToS3TargetTask(
                        m_jobProgressManager,
                        m_diskManager,
                        m_serviceManager,
                        m_connectionFactory,
                        writeInfo ) );
    }

    public void read(@NonNull ReadDirective readDirective) {
        addIoTask( createReadChunkFromS3TargetTask( readDirective ) );
    }


    protected ReadChunkFromS3TargetTask createReadChunkFromS3TargetTask( final ReadDirective readDirective )
    {
        return new ReadChunkFromS3TargetTask(
                m_offlineDataStagingWindowManager,
                m_jobProgressManager,
                m_diskManager,
                m_serviceManager,
                m_connectionFactory,
                readDirective );
    }

    @Override
    protected void discoverWork() {
        for (final IODirective directive : S3TargetWorkAggregationUtils.discoverS3TargetWorkAggregated(m_serviceManager)) {
            if (directive instanceof TargetWriteDirective) {
                final TargetWriteDirective<S3Target, S3BlobDestination> wd = (TargetWriteDirective<S3Target, S3BlobDestination>) directive;
                WorkAggregationUtils.markWriteChunksInProgress(wd, m_serviceManager);
                WorkAggregationUtils.markS3DestinationsInProgress(wd.getBlobDestinations(), m_serviceManager);
                write(wd);
            } else if (directive instanceof ReadDirective) {
                final ReadDirective rd = (ReadDirective) directive;
                WorkAggregationUtils.markReadChunksInProgress(rd, m_serviceManager);
                read(rd);
            }
        }
    }
}
