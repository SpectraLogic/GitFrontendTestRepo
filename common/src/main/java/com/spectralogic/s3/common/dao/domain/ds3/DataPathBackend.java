/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.ds3;

import java.util.Date;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.shared.PersistenceTarget;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.util.bean.lang.DefaultBooleanValue;
import com.spectralogic.util.bean.lang.DefaultDoubleValue;
import com.spectralogic.util.bean.lang.DefaultEnumValue;
import com.spectralogic.util.bean.lang.DefaultIntegerValue;
import com.spectralogic.util.bean.lang.DefaultToCurrentDate;
import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.db.lang.ConfigureSqlLogLevels;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.lang.SqlLogLevels;
import com.spectralogic.util.marshal.ExcludeFromMarshaler;

@ConfigureSqlLogLevels( SqlLogLevels.ALL_OPERATIONS_LOGGED_AT_DEBUG_LEVEL )
public interface DataPathBackend extends DatabasePersistable
{
    String ACTIVATED = "activated";
    
    boolean isActivated();
    
    DataPathBackend setActivated( final boolean value );
    
    
    String AUTO_ACTIVATE_TIMEOUT_IN_MINS = "autoActivateTimeoutInMins";
    
    /**
     * @return null if auto-activation is disabled; or the maximum number of seconds that the data path can
     * be offline until auto-activation no longer be performed
     */
    @Optional
    Integer getAutoActivateTimeoutInMins();
    
    DataPathBackend setAutoActivateTimeoutInMins( final Integer value );
    
    
    String DEFAULT_VERIFY_DATA_PRIOR_TO_IMPORT = "defaultVerifyDataPriorToImport";
    
    @DefaultBooleanValue( true )
    boolean isDefaultVerifyDataPriorToImport();
    
    DataPathBackend setDefaultVerifyDataPriorToImport( final boolean value );
    
    
    String DEFAULT_VERIFY_DATA_AFTER_IMPORT = "defaultVerifyDataAfterImport";
    
    @Optional
    BlobStoreTaskPriority getDefaultVerifyDataAfterImport();
    
    DataPathBackend setDefaultVerifyDataAfterImport( final BlobStoreTaskPriority value );
    
    
    String AUTO_INSPECT = "autoInspect";
    
    @DefaultEnumValue( "FULL" )
    AutoInspectMode getAutoInspect();
    
    DataPathBackend setAutoInspect( final AutoInspectMode value );
    
    
    String LAST_HEARTBEAT = "lastHeartbeat";
    
    @DefaultToCurrentDate
    Date getLastHeartbeat();
    
    DataPathBackend setLastHeartbeat( final Date value );
    
    
    String UNAVAILABLE_TAPE_PARTITION_MAX_JOB_RETRY_IN_MINS = "unavailableTapePartitionMaxJobRetryInMins";
    
    /**
     * @return the maximum number of minutes that can elapse from the first failed attempt to read or verify
     * a {@link JobEntry} due to the tape partition being offline, in error, or quiesced, before a subsequent
     * failure will trigger job re-chunking.  This only applies to {@link JobRequestType#GET} and 
     * {@link JobRequestType#VERIFY} jobs.
     */
    @DefaultIntegerValue( 20 )
    int getUnavailableTapePartitionMaxJobRetryInMins();
    
    DataPathBackend setUnavailableTapePartitionMaxJobRetryInMins( final int value );
    
    
    String UNAVAILABLE_POOL_MAX_JOB_RETRY_IN_MINS = "unavailablePoolMaxJobRetryInMins";

    /**
     * @return the maximum number of minutes that can elapse from the first failed attempt to read or verify
     * a {@link JobEntry} due to the pool being quiesced before a subsequent failure will trigger job
     * re-chunking.  This only applies to {@link JobRequestType#GET} and {@link JobRequestType#VERIFY} jobs.
     */
    @DefaultIntegerValue( 20 )
    int getUnavailablePoolMaxJobRetryInMins();
    
    DataPathBackend setUnavailablePoolMaxJobRetryInMins( final int value );
    
    
    String UNAVAILABLE_MEDIA_POLICY = "unavailableMediaPolicy";

    /**
     * @return the policy surrounding unavailable media (pools or tape partitions) for creating new jobs or 
     * re-chunking job parts
     */
    @DefaultEnumValue( "DISCOURAGED" )
    UnavailableMediaUsagePolicy getUnavailableMediaPolicy();
    
    DataPathBackend setUnavailableMediaPolicy( final UnavailableMediaUsagePolicy value );
    
    
    String INSTANCE_ID = "instanceId";
    
    /**
     * @return a unique identifier to identify this Black Pearl instance
     * 
     * Note that the BP instance id is tied to the dao database for Black Pearl and NOT the system serial
     * number.  There are cases where the instance ID should be relied upon rather than the system serial
     * number since, for example, an instance could be moved from one physical box with one serial number to
     * another box with a different one.  Furthermore, clustered instances may have multiple boxes with
     * different serial numbers.
     */
    UUID getInstanceId();
    
    DataPathBackend setInstanceId( final UUID value );
    
    
    String PARTIALLY_VERIFY_LAST_PERCENT_OF_TAPES = "partiallyVerifyLastPercentOfTapes";
    
    /**
     * If null (the default), verify tape commands will verify the entire tape in interruptible segments, and 
     * once the entire tape has been verified, the {@link PersistenceTarget#LAST_VERIFIED} will be set.
     * <br><br>
     * 
     * If non-null, verify tape commands will verify only the last x% of the tape in a single, 
     * non-interruptible segment, and once complete, the {@link Tape#PARTIALLY_VERIFIED_END_OF_TAPE} will be
     * set.  <br><br>
     * 
     * If this value is changed after a verify tape command has been accepted but before the verify tape
     * command completes, then it is indeterminate as to whether the verify tape command will use the old or
     * new value.
     */
    @Optional
    Integer getPartiallyVerifyLastPercentOfTapes();
    
    DataPathBackend setPartiallyVerifyLastPercentOfTapes( final Integer value );
    
    
    String ALLOW_NEW_JOB_REQUESTS = "allowNewJobRequests";
    
    @DefaultBooleanValue( true )
    boolean isAllowNewJobRequests();
    
    DataPathBackend setAllowNewJobRequests( final boolean value );


    String CACHE_AVAILABLE_RETRY_AFTER_IN_SECONDS = "cacheAvailableRetryAfterInSeconds";

    @DefaultIntegerValue( 60 )
    int getCacheAvailableRetryAfterInSeconds();

    DataPathBackend setCacheAvailableRetryAfterInSeconds( final int value );
    
    
    String IOM_ENABLED = "iomEnabled";
    
    @DefaultBooleanValue( true )
    boolean isIomEnabled();
    
    DataPathBackend setIomEnabled( final boolean value );


    String IOM_CACHE_LIMITATION_PERCENT = "iomCacheLimitationPercent";

    @DefaultDoubleValue(0.5)
    double getIomCacheLimitationPercent();
    DataPathBackend setIomCacheLimitationPercent(double iomCacheLimitationPercent);


    String MAX_AGGREGATED_BLOBS_PER_CHUNK = "maxAggregatedBlobsPerChunk";

    @DefaultIntegerValue(20000)
    int getMaxAggregatedBlobsPerChunk();
    DataPathBackend setMaxAggregatedBlobsPerChunk(int maxAggregatedBlobsPerChunk);


    String VERIFY_CHECKPOINT_BEFORE_READ = "verifyCheckpointBeforeRead";

    @DefaultBooleanValue( true )
    boolean isVerifyCheckpointBeforeRead();
    DataPathBackend setVerifyCheckpointBeforeRead(final boolean value);
    
    String POOL_SAFETY_ENABLED = "poolSafetyEnabled";
    
    /**
     * @return if we have sync enabled on writes to pools or not (false means the possibility of data loss)
     */
    @DefaultBooleanValue( true )
    boolean getPoolSafetyEnabled();
    
    DataPathBackend setPoolSafetyEnabled( final boolean value );

    String MAX_NUMBER_OF_CONCURRENT_JOBS = "maxNumberOfConcurrentJobs";

    /**
     * @return the maximum number of jobs that can be active in the system at one time
     */
    @DefaultIntegerValue(40000)
    int getMaxNumberOfConcurrentJobs();
    DataPathBackend setMaxNumberOfConcurrentJobs( final int value );

    String ALWAYS_ROLLBACK = "alwaysRollback";

    /**
     * @return if rollback is allowed
     */

    @DefaultBooleanValue(true)
    boolean getAlwaysRollback();
    DataPathBackend setAlwaysRollback( final boolean value );


    String EMULATE_CHUNKS = "emulateChunks";

    /**
     * @return the chunk size to emulate, or null if chunk emulation is disabled
     */
    @ExcludeFromMarshaler(ExcludeFromMarshaler.When.ALWAYS)
    @DefaultBooleanValue(false)
    boolean getEmulateChunks();
    DataPathBackend setEmulateChunks(final boolean value );

}
