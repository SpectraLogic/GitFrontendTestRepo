/*******************************************************************************
 *
 * Copyright C 2026, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.tunables;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;

import org.apache.log4j.Logger;

import com.spectralogic.util.db.domain.KeyValue;
import com.spectralogic.util.db.domain.service.KeyValueService;
import com.spectralogic.util.lang.iterate.EnhancedIterable;

/**
 * Centralized runtime tunable accessors. Each named getter returns either a
 * compiled-in default or, if the {@code framework.key_value} table contains an
 * entry for the corresponding key (with a non-null long_value), the override
 * loaded at startup.
 * <p>
 * Each JVM (DataPlanner, server) calls {@link #install(KeyValueService)} once
 * during startup. After install, getters resolve to overrides where present
 * and defaults otherwise.
 * <p>
 * Reads before {@code install()} log a warning (once per uninstalled period)
 * and return defaults. They do NOT throw — the goal is "loud but safe":
 * surface a bootstrap-ordering bug while staying functional.
 * <p>
 * See EMPROD-6999.
 */
public final class Tunables
{
    private static final Logger LOG = Logger.getLogger( Tunables.class );

    /** All tunable keys live under this prefix in the {@code key_value} table. */
    private static final String KEY_PREFIX = "tunable.";

    private static volatile boolean installed = false;
    private static volatile Map< String, Long > overrides = Map.of();
    private static final AtomicBoolean WARNED_PRE_INIT = new AtomicBoolean( false );

    private Tunables() {}


    /**
     * Load overrides from the key_value table and install them. Call once per JVM at startup.
     *
     * @param kvs The key/value service. May be null (e.g. unit tests); in that case no overrides
     *            are loaded and defaults apply.
     */
    public static void install( final KeyValueService kvs )
    {
        Map< String, Long > loaded = Map.of();
        if ( kvs != null )
        {
            try
            {
                final Map< String, Long > tmp = new HashMap<>();
                for ( final KeyValue kv : kvs.retrieveAll().toSet() )
                {
                    if ( kv.getKey() != null
                         && kv.getKey().startsWith( KEY_PREFIX )
                         && kv.getLongValue() != null )
                    {
                        tmp.put( kv.getKey(), kv.getLongValue() );
                    }
                }
                loaded = tmp;
            }
            catch ( final RuntimeException e )
            {
                LOG.warn( "Failed to load tunable overrides; using compiled defaults.", e );
            }
        }
        overrides = Map.copyOf( loaded );
        installed = true;
        WARNED_PRE_INIT.set( false );

        if ( loaded.isEmpty() )
        {
            LOG.info( "Tunables installed; no overrides present, running compiled defaults." );
        }
        else
        {
            LOG.warn( "Tunables installed with " + loaded.size() + " override(s) — "
                    + "this system is NOT running default values "
                    + "(typically only set during support escalations): " + loaded );
        }
    }


    /** Test-only: reset to the uninstalled state. */
    static void uninstall()
    {
        installed = false;
        overrides = Map.of();
        WARNED_PRE_INIT.set( false );
    }


    private static long lookup( final String key, final long defaultValue )
    {
        if ( !installed && WARNED_PRE_INIT.compareAndSet( false, true ) )
        {
            LOG.warn( "Tunables read before install() — bootstrap ordering bug. "
                    + "Returning compiled defaults until install() runs. First offender: " + key );
        }
        final Long v = overrides.get( key );
        return v != null ? v : defaultValue;
    }


    private static int lookupInt( final String key, final int defaultValue )
    {
        final long v = lookup( key, defaultValue );
        if ( v < Integer.MIN_VALUE || v > Integer.MAX_VALUE )
        {
            LOG.warn( "Tunable override " + key + "=" + v
                    + " is out of int range; using compiled default " + defaultValue );
            return defaultValue;
        }
        return (int)v;
    }


    /**
     * Variant for tunables whose default is a small calculation (e.g. derived from
     * {@code Runtime.getRuntime().availableProcessors()} or {@code maxMemory()}).
     * The supplier is only evaluated when no override is present.
     */
    private static int lookupInt( final String key, final IntSupplier defaultSupplier )
    {
        if ( !installed && WARNED_PRE_INIT.compareAndSet( false, true ) )
        {
            LOG.warn( "Tunables read before install() — bootstrap ordering bug. "
                    + "Returning compiled defaults until install() runs. First offender: " + key );
        }
        final Long v = overrides.get( key );
        return v != null ? v.intValue() : defaultSupplier.getAsInt();
    }


    private static long lookup( final String key, final LongSupplier defaultSupplier )
    {
        if ( !installed && WARNED_PRE_INIT.compareAndSet( false, true ) )
        {
            LOG.warn( "Tunables read before install() — bootstrap ordering bug. "
                    + "Returning compiled defaults until install() runs. First offender: " + key );
        }
        final Long v = overrides.get( key );
        return v != null ? v : defaultSupplier.getAsLong();
    }


    // ================================================================
    // util
    // ================================================================

    public static int  threadedDataMoverMaxBufferCapacity()                  { return lookupInt( "tunable.threaded-data-mover-max-buffer-capacity", 6 * 1024 * 1024 ); }
    public static int  threadedDataMoverMaxNumberOfBuffers()                 { return lookupInt( "tunable.threaded-data-mover-max-number-of-buffers", 128 ); }
    public static int  threadedDataMoverDataWriterPoolSize()                 { return lookupInt( "tunable.threaded-data-mover-data-writer-pool-size",          () -> Runtime.getRuntime().availableProcessors() * 2 ); }
    public static int  threadedDataMoverChecksumComputerPoolSize()           { return lookupInt( "tunable.threaded-data-mover-checksum-computer-pool-size",    () -> Runtime.getRuntime().availableProcessors() ); }

    public static int  postgresDataManagerMaxBeansPerCreateBeansCommand()    { return lookupInt( "tunable.postgres-data-manager-max-beans-per-create-beans-command", 10000 ); }

    public static int  tcpIpClientNumThreads()                               { return lookupInt( "tunable.tcp-ip-client-num-threads",                         () -> Math.min( 8, Math.max( 2, Runtime.getRuntime().availableProcessors() ) ) ); }
    public static int  tcpIpServerNumThreads()                               { return lookupInt( "tunable.tcp-ip-server-num-threads",                         () -> Math.max( 2, Runtime.getRuntime().availableProcessors() ) ); }

    public static int  dbBackgroundQueryPoolSize()                           { return lookupInt( "tunable.db-background-query-pool-size",                     () -> Math.max( 1, Runtime.getRuntime().availableProcessors() / 6 ) ); }
    public static int  dbParallelQueryPoolSize()                             { return lookupInt( "tunable.db-parallel-query-pool-size",                       () -> Math.max( 1, ( Runtime.getRuntime().availableProcessors() * 3 ) / 4 ) ); }


    // ================================================================
    // common
    // ================================================================

    public static int  groupMembershipCalculatorMaxRecursiveDepthAllowed()   { return lookupInt( "tunable.group-membership-calculator-max-recursive-depth-allowed", 5 ); }

    public static int  blobDestinationUtilsMaxBatchSize()                    { return lookupInt( "tunable.blob-destination-utils-max-batch-size", 10000 ); }

    public static int  iomServiceMaxBeansToQuery()                           { return lookupInt( "tunable.iom-service-max-beans-to-query", 100000 ); }


    // ================================================================
    // target
    // ================================================================

    /** Shared by BasePublicCloudConnection and S3NativeConnectionImpl. */
    public static int  publicCloudMaxTransferRetries()                       { return lookupInt( "tunable.public-cloud-max-transfer-retries", 10 ); }


    // ================================================================
    // DataPlanner
    // ================================================================

    public static int  writeChunkToTapeTaskMaxTapesTaskCanMarkBad()          { return lookupInt( "tunable.write-chunk-to-tape-task-max-tapes-task-can-mark-bad", 3 ); }

    public static int  threadedBlobVerifierNumThreads()                      { return lookupInt( "tunable.threaded-blob-verifier-num-threads",                () -> Math.max( 1, Runtime.getRuntime().availableProcessors() / 2 ) ); }

    /** Shared by Tape, S3Target, Pool, Ds3Target, and AzureTarget WorkAggregationUtils. */
    public static int  workAggregationMaxEntriesPerTask()                    { return lookupInt( "tunable.work-aggregation-max-entries-per-task", 100000 ); }
    public static long workAggregationMaxBytesPerTask()                      { return lookup(    "tunable.work-aggregation-max-bytes-per-task", 100L * 1024L * 1024L * 1024L ); }

    /** Tape-only: spanning-minimization threshold. */
    public static long tapeWorkAggregationUtilsMinspanningTaskSize()         { return lookup(    "tunable.tape-work-aggregation-utils-minspanning-task-size", 1024L * 1024L * 1024L * 1024L ); }

    public static int  compactPoolTaskMaxBlobsToVerify()                     { return lookupInt( "tunable.compact-pool-task-max-blobs-to-verify", 100000 ); }
    public static int  compactPoolTaskMaxContinuousPruneDuration()           { return lookupInt( "tunable.compact-pool-task-max-continuous-prune-duration", 10 ); }

    public static long cacheSpaceReclaimerMaxReclaimSegmentBytes()           { return lookup(    "tunable.cache-space-reclaimer-max-reclaim-segment-bytes", 100 * 1024L * 1024 * 1024 ); }
    public static int  cacheSpaceReclaimerMaxReclaimSegmentBlobs()           { return lookupInt( "tunable.cache-space-reclaimer-max-reclaim-segment-blobs", 1000 ); }

    public static long cacheManagerMaxChunkSize()                            { return lookup(    "tunable.cache-manager-max-chunk-size", 1024L * 1024 * 1024 * 1024 ); }

    public static int  baseReadChunkFromPublicCloudTaskMaxStageRequestFrequencyInHours() { return lookupInt( "tunable.base-read-chunk-from-public-cloud-task-max-stage-request-frequency-in-hours", 6 ); }

    public static int  tapeFailureManagementMaxFailureAgeInHours()           { return lookupInt( "tunable.tape-failure-management-max-failure-age-in-hours", 24 ); }

    public static int  iomDriverMaxConcurrentDataMigrations()                { return lookupInt( "tunable.iom-driver-max-concurrent-data-migrations", 10 ); }
    public static int  iomDriverKnownMissingBlobsResetInMinutes()            { return lookupInt( "tunable.iom-driver-known-missing-blobs-reset-in-minutes", 60 * 12 ); }

    public static long tapeBlobStoreProcessorMaxTaskSizeInBytes()            { return lookup(    "tunable.tape-blob-store-processor-max-task-size-in-bytes", 30 * 1024 * 1024 * 1024L ); }
    public static int  tapeBlobStoreProcessorMaxEntriesPerTask()             { return lookupInt( "tunable.tape-blob-store-processor-max-entries-per-task", 10000 ); }

    public static int  dataPlannerHistoryTableMaxSize()                      { return lookupInt( "tunable.data-planner-history-table-max-size", 500000 ); }

    public static long tapeBlobStoreDefaultMaxTaskSize()                     { return lookup(    "tunable.tape-blob-store-default-max-task-size", 64L * 1024 * 1024 * 1024 ); }

    public static int  getByPhysicalPlacementDataOrderingStrategySingleTransactionMax() { return lookupInt( "tunable.get-by-physical-placement-data-ordering-strategy-single-transaction-max", 10000 ); }

    public static long dataPolicyManagementResourcePublicCloudMinBlobPartSize() { return lookup( "tunable.data-policy-management-resource-public-cloud-min-blob-part-size", 100 * 1024L * 1024 ); }
    public static long dataPolicyManagementResourcePublicCloudMaxBlobPartSize() { return lookup( "tunable.data-policy-management-resource-public-cloud-max-blob-part-size", 1024 * 1024 * 1024L * 1024 ); }
    public static int  dataPolicyManagementResourceMinMinDaysToRetainForNearlinePool() { return lookupInt( "tunable.data-policy-management-resource-min-min-days-to-retain-for-nearline-pool", 1 ); }


    // ================================================================
    // server
    // ================================================================

    public static int  requestPagingPropertiesMaxPageLength()                { return lookupInt( "tunable.request-paging-properties-max-page-length", 1000 ); }

    public static long modifyCacheFilesystemRequestHandlerMinCacheCapacity() { return lookup(    "tunable.modify-cache-filesystem-request-handler-min-cache-capacity", 1024 * 1024L * 1024 ); }

    /** Default scales with JVM heap: capped at the streaming threshold, otherwise heap-kb / 6. */
    public static int  createJobMaxNumberOfObjectsPerJob()                   { return lookupInt( "tunable.create-job-max-number-of-objects-per-job",
                                                                                                  () -> Math.min(
                                                                                                          EnhancedIterable.MAX_NUMBER_OF_RESULTS_BEFORE_STREAMING_IS_REQUIRED,
                                                                                                          (int)( Runtime.getRuntime().maxMemory() / 1024 ) / 6 ) ); }
    /** Default scales with JVM heap: heap-kb / 2.5. */
    public static int  createJobMaxConcurrentObjectsToJob()                  { return lookupInt( "tunable.create-job-max-concurrent-objects-to-job",
                                                                                                  () -> (int)( ( Runtime.getRuntime().maxMemory() / 1024 ) / 2.5 ) ); }
}
