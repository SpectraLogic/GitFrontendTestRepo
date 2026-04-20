package com.spectralogic.s3.dataplanner.backend.frmwrk;

import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.target.ReplicationTarget;

import java.util.Collection;

public class TargetWriteDirective<T extends ReplicationTarget<T>, PT extends RemoteBlobDestination<PT>> extends WriteDirective {

    public TargetWriteDirective(final Class<T> targetType, final Collection<PT> persistenceTargets, final T target, final BlobStoreTaskPriority priority, final Collection<JobEntry> chunks, final long sizeInBytes, final Bucket bucket) {
        super(priority, chunks, bucket, sizeInBytes);
        this.m_targetType = targetType;
        this.m_persistenceTargets = persistenceTargets;
        this.m_target = target;
    }

    public Collection<PT> getBlobDestinations() {
        return m_persistenceTargets;
    }

    public T getTarget() {
        return m_target;
    }


    final Class<T> m_targetType;
    final Collection<PT> m_persistenceTargets;
    final T m_target;
}