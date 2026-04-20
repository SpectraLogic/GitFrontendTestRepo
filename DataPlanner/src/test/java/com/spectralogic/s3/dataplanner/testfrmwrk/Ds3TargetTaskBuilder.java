package com.spectralogic.s3.dataplanner.testfrmwrk;

import com.spectralogic.s3.common.dao.domain.ds3.Ds3BlobDestination;
import com.spectralogic.s3.common.dao.domain.target.Ds3Target;
import com.spectralogic.s3.common.rpc.target.Ds3ConnectionFactory;
import com.spectralogic.s3.dataplanner.backend.frmwrk.ReadDirective;
import com.spectralogic.s3.dataplanner.backend.frmwrk.TargetWriteDirective;
import com.spectralogic.s3.dataplanner.backend.target.task.ReadChunkFromDs3TargetTask;
import com.spectralogic.s3.dataplanner.backend.target.task.WriteChunkToDs3TargetTask;

public interface Ds3TargetTaskBuilder {

    Ds3TargetTaskBuilder withDs3ConnectionFactory(final Ds3ConnectionFactory s3ConnectionFactory);

    ReadChunkFromDs3TargetTask buildReadChunkFromDs3TargetTask(ReadDirective readDirective);

    WriteChunkToDs3TargetTask buildWriteChunkToDs3TargetTask(TargetWriteDirective<Ds3Target, Ds3BlobDestination> writeDirective);
}
