/*
 *
 * Copyright C 2018, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.s3.common.dao.domain.ds3;

import java.util.Date;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.shared.ErrorMessageObservable;
import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.util.bean.lang.DefaultBooleanValue;
import com.spectralogic.util.bean.lang.DefaultEnumValue;
import com.spectralogic.util.bean.lang.DefaultStringValue;
import com.spectralogic.util.bean.lang.DefaultToCurrentDate;
import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.bean.lang.SortBy;
import com.spectralogic.util.bean.lang.SortBy.Direction;
import com.spectralogic.util.db.lang.CascadeDelete;
import com.spectralogic.util.db.lang.References;

public interface JobObservable< T >
    extends UserIdObservable< T >, ErrorMessageObservable< T >, NameObservable< T >
{
    @DefaultStringValue( DEFAULT_JOB_NAME )
    String getName();
    
    
    String REQUEST_TYPE = "requestType";
    
    JobRequestType getRequestType();
    
    T setRequestType( final JobRequestType value );
    

    String CHUNK_CLIENT_PROCESSING_ORDER_GUARANTEE = "chunkClientProcessingOrderGuarantee";
    
    /**
     * For PUT jobs, the chunk client processing guarantee shall always be 
     * {@link JobChunkClientProcessingOrderGuarantee#IN_ORDER}, which means that if chunk B is after chunk A, 
     * that clients should start with chunk A before moving onto chunk B.  Note that clients can send chunks
     * out of order without significant negative side effects provided that no single object is greater than a
     * {@link JobEntry}.  <br><br>
     * 
     * For GET jobs, higher performance may be achieved by using 
     * {@link JobChunkClientProcessingOrderGuarantee#NONE}, which requires the client to be intelligent
     * enough to see which chunks are available for processing and to process the available chunks, whether
     * or not they become available in order or not.  There are some clients that may not support chunks 
     * coming back out-of-order though, and for those clients,
     * {@link JobChunkClientProcessingOrderGuarantee#IN_ORDER} must be used.  Note that the performance 
     * improvement from using {@link JobChunkClientProcessingOrderGuarantee#NONE} can be huge (>100% speedup).
     */
    @DefaultEnumValue( "IN_ORDER" )
    JobChunkClientProcessingOrderGuarantee getChunkClientProcessingOrderGuarantee();
    
    T setChunkClientProcessingOrderGuarantee( final JobChunkClientProcessingOrderGuarantee value );
    
    
    String NAKED = "naked";
    
    /**
     * @return TRUE if this job was created implicitly (as a result of a naked S3 operation rather than a
     * bulk operation)
     */
    @DefaultBooleanValue( false )
    boolean isNaked();
    
    T setNaked( final boolean value );
    
    
    String BUCKET_ID = "bucketId";
    
    @References( Bucket.class )
    @CascadeDelete
    UUID getBucketId();
    
    T setBucketId( final UUID value );
    
    
    String CREATED_AT = "createdAt";
    
    @SortBy( direction = Direction.DESCENDING )
    @DefaultToCurrentDate
    Date getCreatedAt();
    
    T setCreatedAt( final Date value );
    
    
    String TRUNCATED = "truncated";
    
    /**
     * @return TRUE if the job has been truncated (made smaller) from it's original definition <br><br>
     * 
     * A job can be truncated in the following ways:
     * <ol>
     * <li>Canceling a PUT job without the force flag when there exists some objects that have been entirely
     * uploaded to cache, but not entirely persisted, <b>and</b> there exists some objects that have not been
     * entirely uploaded to cache, will result in the objects that haven't been entirely uploaded to cache
     * being deleted and truncated from the job so that the job can complete without any further transmission
     * of data from any client(s).
     * 
     * <li>Servicing a GET or VERIFY job where we discover in-flight that some of the blobs cannot be gotten
     * (for example, all media containing a blob has been ejected, lost, or corrupted), in which case we
     * truncate the job for what we cannot GET or VERIFY.  Note that we will fail the job creation request
     * if we can tell at that time that some of the blobs we are being asked to GET or VERIFY cannot be
     * gotten.
     * </ol>
     */
    @DefaultBooleanValue( false )
    boolean isTruncated();
    
    T setTruncated( final boolean value );
    
    
    String RECHUNKED = "rechunked";
    
    /**
     * @return the most recent date the job had to be re-chunked in-flight from its original definition, or
     * null if the job has not been re-chunked yet (does not apply to PUT jobs) <br><br>
     * 
     * A GET or VERIFY job may have to be re-chunked in-flight if one or more chunks of the job target a
     * specific pool or tape that goes offline (for example, is ejected or lost), or if it is determined that
     * one or more blobs on that media are corrupted or missing.  <br><br>
     * 
     * In this case, we will attempt to "re-chunk" that chunk that can no longer be serviced by the specified
     * media into one or more new chunks that target other media.  If there is no other media that we can
     * service the GET or VERIFY from (for example, if there was only one copy of the blobs that was online),
     * the job will be truncated.  But if we can service the chunk from other media, the job will be
     * re-chunked to use that other media.
     */
    @Optional
    Date getRechunked();
    
    T setRechunked( final Date value );
    
    
    String PRIORITY = "priority";
    
    @DefaultEnumValue( "NORMAL" )
    BlobStoreTaskPriority getPriority();
    
    T setPriority( final BlobStoreTaskPriority value );
    
    
    String ORIGINAL_SIZE_IN_BYTES = "originalSizeInBytes";
    
    /**
     * The original (total) size of the job (as entries in the job are completed, they will go away, and so
     * the current size of a job shrinks over time).
     */
    long getOriginalSizeInBytes();
    
    T setOriginalSizeInBytes( final long value );
    
    
    String CACHED_SIZE_IN_BYTES = "cachedSizeInBytes";
    
    /**
     * The amount of data of the job that has been transferred into cache.  For PUTs, this is the sum of the
     * blob lengths successfully uploaded from the client.  For GETs, this is the sum of the blob lengths
     * either (i) in cache initially, or (ii) loaded into cache from tape.
     */
    long getCachedSizeInBytes();
    
    T setCachedSizeInBytes( final long value );
    
    
    String COMPLETED_SIZE_IN_BYTES = "completedSizeInBytes";
    
    /**
     * The amount of data of the job that has been completely processed.  For PUTs, this is the sum of the
     * blob lengths successfully written to physical data stores.  For GETs, this is the sum of the blob 
     * lengths that have been read completely by the client.
     */
    long getCompletedSizeInBytes();
    
    T setCompletedSizeInBytes( final long value );
    
    
    int HOURS_TO_RETAIN_ONCE_DONE = 30 * 24;
    
    int MAX_COUNT_TO_RETAIN_ONCE_DONE = 100000;
    
    public String DEFAULT_JOB_NAME = "Untitled";
}
