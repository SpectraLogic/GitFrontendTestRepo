/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.target;

import com.spectralogic.s3.common.dao.domain.ds3.Ds3BlobDestination;
import com.spectralogic.s3.common.dao.domain.target.Ds3Target;
import com.spectralogic.s3.common.dao.service.ds3.JobProgressManager;
import com.spectralogic.s3.common.dao.service.target.Ds3TargetFailureService;
import com.spectralogic.s3.common.platform.cache.DiskManager;
import com.spectralogic.s3.common.rpc.target.Ds3ConnectionFactory;
import com.spectralogic.s3.dataplanner.backend.frmwrk.Ds3TargetWorkAggregationUtils;
import com.spectralogic.s3.dataplanner.backend.frmwrk.IODirective;
import com.spectralogic.s3.dataplanner.backend.frmwrk.ReadDirective;
import com.spectralogic.s3.dataplanner.backend.frmwrk.TargetWriteDirective;
import com.spectralogic.s3.dataplanner.backend.frmwrk.WorkAggregationUtils;
import com.spectralogic.s3.dataplanner.backend.target.api.TargetBlobStore;
import com.spectralogic.s3.dataplanner.backend.target.task.ReadChunkFromDs3TargetTask;
import com.spectralogic.s3.dataplanner.backend.target.task.WriteChunkToDs3TargetTask;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import lombok.NonNull;

public final class Ds3TargetBlobStore
    extends BaseTargetBlobStore< Ds3Target, Ds3ConnectionFactory, Ds3TargetFailureService > 
    implements TargetBlobStore
{
    public Ds3TargetBlobStore(
            final Ds3ConnectionFactory ds3ConnectionFactory,
            final DiskManager diskManager,
            final JobProgressManager jobProgressManager,
            final BeansServiceManager serviceManager )
    {
        super( Ds3Target.class, 
                Ds3TargetFailureService.class,
                ds3ConnectionFactory, 
                null,
                null,
                diskManager, 
                jobProgressManager,
                serviceManager );
    }

    public void read(@NonNull ReadDirective readDirective) {
        addIoTask(
                new ReadChunkFromDs3TargetTask(
                        m_jobProgressManager, m_diskManager, m_serviceManager, m_connectionFactory, readDirective
                ));
    }


    @Override
    protected void discoverWork() {
        for (final IODirective directive : Ds3TargetWorkAggregationUtils.discoverDs3TargetWorkAggregated(m_serviceManager)) {
            if (directive instanceof TargetWriteDirective) {
                final TargetWriteDirective<Ds3Target, Ds3BlobDestination> wd = (TargetWriteDirective<Ds3Target, Ds3BlobDestination>) directive;
                WorkAggregationUtils.markWriteChunksInProgress(wd, m_serviceManager);
                WorkAggregationUtils.markDs3DestinationsInProgress(wd.getBlobDestinations(), m_serviceManager);
                write(wd);
            } else if (directive instanceof ReadDirective) {
                final ReadDirective rd = (ReadDirective) directive;
                WorkAggregationUtils.markReadChunksInProgress(rd, m_serviceManager);
                read(rd);
            }
        }
    }

    @Override
    protected void verifyCanConnect( final Ds3Target target )
    {
        m_connectionFactory.connect( null, target ).shutdown();
    }

    public void write(@NonNull final TargetWriteDirective<Ds3Target, Ds3BlobDestination> writeInfo) {
        addIoTask(
                new WriteChunkToDs3TargetTask(
                        m_jobProgressManager, m_diskManager, m_serviceManager, m_connectionFactory, writeInfo
                ));
    }
}
