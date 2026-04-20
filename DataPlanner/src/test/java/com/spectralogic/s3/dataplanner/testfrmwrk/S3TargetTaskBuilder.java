package com.spectralogic.s3.dataplanner.testfrmwrk;

import com.spectralogic.s3.common.dao.domain.ds3.BlobStoreTaskPriority;
import com.spectralogic.s3.common.dao.domain.ds3.S3BlobDestination;
import com.spectralogic.s3.common.dao.domain.target.S3Target;
import com.spectralogic.s3.common.rpc.target.S3ConnectionFactory;
import com.spectralogic.s3.dataplanner.backend.frmwrk.ReadDirective;
import com.spectralogic.s3.dataplanner.backend.frmwrk.TargetWriteDirective;
import com.spectralogic.s3.dataplanner.backend.target.task.ImportS3TargetTask;
import com.spectralogic.s3.dataplanner.backend.target.task.ReadChunkFromS3TargetTask;
import com.spectralogic.s3.dataplanner.backend.target.task.WriteChunkToS3TargetTask;

import java.util.UUID;

public interface S3TargetTaskBuilder {

    S3TargetTaskBuilder withS3ConnectionFactory(final S3ConnectionFactory s3ConnectionFactory);

    ReadChunkFromS3TargetTask buildReadChunkFromS3TargetTask(ReadDirective readDirective);

    WriteChunkToS3TargetTask buildWriteChunkToS3TargetTask(TargetWriteDirective<S3Target, S3BlobDestination> writeDirective);

    ImportS3TargetTask buildImportS3TargetTask(final BlobStoreTaskPriority priority, final UUID targetId);
}
