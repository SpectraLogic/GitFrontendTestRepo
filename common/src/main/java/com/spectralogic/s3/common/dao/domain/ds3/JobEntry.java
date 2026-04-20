/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.ds3;

import java.util.Date;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.shared.BlobObservable;
import com.spectralogic.s3.common.dao.domain.shared.ReadFromObservable;
import com.spectralogic.util.bean.lang.DefaultBooleanValue;
import com.spectralogic.util.bean.lang.DefaultEnumValue;
import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.bean.lang.SortBy;
import com.spectralogic.util.db.lang.CascadeDelete;
import com.spectralogic.util.db.lang.CascadeDelete.WhenReferenceIsDeleted;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.lang.Index;
import com.spectralogic.util.db.lang.Indexes;
import com.spectralogic.util.db.lang.References;
import com.spectralogic.util.db.lang.Unique;
import com.spectralogic.util.db.lang.UniqueIndexes;

/**
 * A {@link Blob} to be serviced (written or retrieved) by a {@link Job}.
 */
@Indexes(
{
    @Index( JobEntry.BLOB_STORE_STATE )
})
@UniqueIndexes({
    @Unique({ JobEntry.CHUNK_NUMBER, JobEntry.JOB_ID })
})
public interface JobEntry extends DatabasePersistable, ReadFromObservable<JobEntry>, BlobObservable<JobEntry>
{
    String JOB_ID = "jobId";
    
    @SortBy( 3 )
    @References( Job.class )
    @CascadeDelete
    UUID getJobId();
    
    JobEntry setJobId(final UUID value );


    String NODE_ID = "nodeId";

    @Optional
    @References( Node.class )
    @CascadeDelete( WhenReferenceIsDeleted.SET_NULL )
    UUID getNodeId();
    
    JobEntry setNodeId(final UUID value );
    
    
    String CHUNK_NUMBER = "chunkNumber";

    @SortBy( 4 )
    int getChunkNumber();

    JobEntry setChunkNumber(final int value );
    
    
    String PENDING_TARGET_COMMIT = "pendingTargetCommit";
    
    /**
     * @return TRUE if the chunk is part of a {@link JobRequestType#PUT} job and all local and replicated
     * copies required have been made, and the only thing we're waiting on to delete this chunk and mark it
     * as completed is for the target(s) to complete their local and replicated copies
     */
    @DefaultBooleanValue( false )
    boolean isPendingTargetCommit();
    
    JobEntry setPendingTargetCommit(final boolean value );


    String BLOB_STORE_STATE = "blobStoreState";

    @DefaultEnumValue( "PENDING" )
    JobChunkBlobStoreState getBlobStoreState();

    JobEntry setBlobStoreState(final JobChunkBlobStoreState value );


    String CHUNK_ID = "chunkId";

    @Optional
    UUID getChunkId();

    JobEntry setChunkId(final UUID value );
}
