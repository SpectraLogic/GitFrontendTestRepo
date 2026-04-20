package com.spectralogic.s3.dataplanner.testfrmwrk;

import com.spectralogic.s3.common.dao.domain.ds3.AzureBlobDestination;
import com.spectralogic.s3.common.dao.domain.ds3.BlobStoreTaskPriority;
import com.spectralogic.s3.common.dao.domain.target.AzureTarget;
import com.spectralogic.s3.common.rpc.target.AzureConnectionFactory;
import com.spectralogic.s3.dataplanner.backend.frmwrk.ReadDirective;
import com.spectralogic.s3.dataplanner.backend.frmwrk.TargetWriteDirective;
import com.spectralogic.s3.dataplanner.backend.target.task.ImportAzureTargetTask;
import com.spectralogic.s3.dataplanner.backend.target.task.ReadChunkFromAzureTargetTask;
import com.spectralogic.s3.dataplanner.backend.target.task.WriteChunkToAzureTargetTask;

import java.util.UUID;

public interface AzureTargetTaskBuilder {

    AzureTargetTaskBuilder withAzureConnectionFactory(final AzureConnectionFactory s3ConnectionFactory);

    ReadChunkFromAzureTargetTask buildReadChunkFromAzureTargetTask(ReadDirective readDirective);

    WriteChunkToAzureTargetTask buildWriteChunkToAzureTargetTask(TargetWriteDirective<AzureTarget, AzureBlobDestination> writeDirective);

    ImportAzureTargetTask buildImportAzureTargetTask(final BlobStoreTaskPriority priority, final UUID targetId);
}
