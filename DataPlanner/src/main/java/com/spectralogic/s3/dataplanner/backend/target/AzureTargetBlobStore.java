/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.target;

import com.azure.core.exception.AzureException;
import com.azure.core.exception.HttpResponseException;
import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.shared.BlobObservable;
import com.spectralogic.s3.common.dao.domain.target.*;
import com.spectralogic.s3.common.dao.service.ds3.JobProgressManager;
import com.spectralogic.s3.common.dao.service.target.AzureTargetFailureService;
import com.spectralogic.s3.common.dao.service.target.ImportAzureTargetDirectiveService;
import com.spectralogic.s3.common.dao.service.target.SuspectBlobAzureTargetService;
import com.spectralogic.s3.common.platform.cache.DiskManager;
import com.spectralogic.s3.common.rpc.target.AzureConnectionFactory;
import com.spectralogic.s3.dataplanner.backend.frmwrk.AzureTargetWorkAggregationUtils;
import com.spectralogic.s3.dataplanner.backend.frmwrk.IODirective;
import com.spectralogic.s3.dataplanner.backend.frmwrk.ReadDirective;
import com.spectralogic.s3.dataplanner.backend.frmwrk.TargetWriteDirective;
import com.spectralogic.s3.dataplanner.backend.frmwrk.WorkAggregationUtils;
import com.spectralogic.s3.dataplanner.backend.target.api.PublicCloudTargetBlobStore;
import com.spectralogic.s3.dataplanner.backend.target.task.*;
import com.spectralogic.util.bean.BeanCopier;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import lombok.NonNull;


import java.util.Set;
import java.util.UUID;

public  class AzureTargetBlobStore
    extends BasePublicCloudTargetBlobStore
            < AzureTarget, AzureConnectionFactory, AzureTargetFailureService, ImportAzureTargetDirective > 
    implements PublicCloudTargetBlobStore< ImportAzureTargetDirective >
{
    public AzureTargetBlobStore(
            final AzureConnectionFactory azureConnectionFactory,
            final DiskManager diskManager,
            final JobProgressManager jobProgressManager,
            final BeansServiceManager serviceManager )
    {
        super( AzureTarget.class, 
                AzureTargetFailureService.class,
                ImportAzureTargetDirectiveService.class,
                azureConnectionFactory, 
                FeatureKeyType.MICROSOFT_AZURE_CLOUD_OUT,
                SystemFailureType.MICROSOFT_AZURE_WRITES_REQUIRE_FEATURE_LICENSE,
                diskManager, 
                jobProgressManager,
                serviceManager );
    }


    @Override
    protected void discoverWork() {
        for (final IODirective directive : AzureTargetWorkAggregationUtils.discoverAzureTargetWorkAggregated(m_serviceManager)) {
            if (directive instanceof TargetWriteDirective) {
                final TargetWriteDirective<AzureTarget, AzureBlobDestination> wd = (TargetWriteDirective<AzureTarget, AzureBlobDestination>) directive;
                WorkAggregationUtils.markWriteChunksInProgress(wd, m_serviceManager);
                WorkAggregationUtils.markAzureDestinationsInProgress(wd.getBlobDestinations(), m_serviceManager);
                write(wd);
            } else if (directive instanceof ReadDirective) {
                final ReadDirective rd = (ReadDirective) directive;
                WorkAggregationUtils.markReadChunksInProgress(rd, m_serviceManager);
                read(rd);
            }
        }
    }

    @Override
    protected void verifyCanConnect( final AzureTarget target )
    {
        m_connectionFactory.connect( target ).shutdown();
    }


    @Override
    protected ImportAzureTargetTask createImportTask( final ImportAzureTargetDirective directive )
    {
        return new ImportAzureTargetTask( directive.getPriority(),
                directive.getTargetId(),
                m_diskManager,
                m_jobProgressManager,
                m_serviceManager,
                m_connectionFactory );
    }

    public void write(@NonNull final TargetWriteDirective<AzureTarget, AzureBlobDestination> writeInfo) {
        addIoTask(
                new WriteChunkToAzureTargetTask(
                        m_jobProgressManager,
                        m_diskManager,
                        m_serviceManager,
                        m_connectionFactory,
                        writeInfo ) );
    }

    public void read(@NonNull ReadDirective readDirective) {
        addIoTask(createReadChunkFromAzureTargetTask( readDirective ) );
    }


    protected ReadChunkFromAzureTargetTask createReadChunkFromAzureTargetTask(final ReadDirective readDirective )
    {
        return new ReadChunkFromAzureTargetTask(
                m_offlineDataStagingWindowManager,
                m_jobProgressManager,
                m_diskManager,
                m_serviceManager,
                m_connectionFactory,
                readDirective );
    }
}
