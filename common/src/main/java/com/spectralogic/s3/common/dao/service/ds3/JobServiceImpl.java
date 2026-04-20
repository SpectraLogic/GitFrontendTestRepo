/*
 *
 * Copyright C 2018, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.s3.common.dao.service.ds3;

import java.lang.reflect.Method;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.notification.JobCompletedNotificationRegistration;
import com.spectralogic.s3.common.dao.domain.shared.PersistenceTarget;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.domain.tape.TapeState;
import com.spectralogic.s3.common.platform.api.TapeEjector;
import com.spectralogic.s3.common.platform.notification.domain.event.JobNotificationEvent;
import com.spectralogic.s3.common.platform.notification.generator.JobCompletedNotificationPayloadGenerator;
import com.spectralogic.util.bean.BeanCopier;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.BeanSQLOrdering;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.bean.lang.SortBy;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.query.Query;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.BaseService;
import com.spectralogic.util.exception.DaoException;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.thread.workmon.MonitoredWork;
import com.spectralogic.util.thread.workmon.MonitoredWork.StackTraceLogging;

final class JobServiceImpl extends BaseService< Job > implements JobService
{
    JobServiceImpl()
    {
        super( Job.class );
    }
    
    
    public void migrate( final UUID destinationJobId, final UUID srcJobId )
    {
        if ( !getServiceManager().isTransaction() )
        {
            throw new IllegalStateException( 
                    "Moving job entries must always be called inside a transaction." );
        }
        
        final JobEntryService chunkService = getServiceManager().getService( JobEntryService.class );
        final Set<JobEntry> chunks =
                BeanUtils.sort( chunkService.retrieveAll( JobEntry.JOB_ID, srcJobId ).toSet() );
        int chunkNumber = chunkService.getNextChunkNumber( destinationJobId );
        for ( final JobEntry chunk : chunks )
        {
            chunk.setChunkNumber( ++chunkNumber );
            chunk.setJobId( destinationJobId );
            chunkService.update( chunk, JobEntry.JOB_ID, JobEntry.CHUNK_NUMBER );
        }
        
        getDataManager().updateBeans(
                CollectionFactory.toSet( JobEntry.JOB_ID ),
                BeanFactory.newBean( JobEntry.class ).setJobId( destinationJobId ),
                Require.beanPropertyEquals( JobEntry.JOB_ID, srcJobId ) );
        delete( srcJobId );
    }
    
    
    public UUID getPutJobComponentOfDataMigration( final UUID getJobComponent )
    {
        final Job putJob = getServiceManager().getRetriever( Job.class ).retrieve(
                Require.exists( DataMigration.class,
                                DataMigration.PUT_JOB_ID,
                                Require.beanPropertyEquals( DataMigration.GET_JOB_ID, getJobComponent ) ) );
        if ( null == putJob )
        {
            return null;
        }
        return putJob.getId();
    }
    
    
    public UUID getGetJobComponentOfDataMigration( final UUID putJobComponent )
    {
        final Job getJob = getServiceManager().getRetriever( Job.class ).retrieve(
                Require.exists( DataMigration.class,
                                DataMigration.GET_JOB_ID,
                                Require.beanPropertyEquals( DataMigration.PUT_JOB_ID, putJobComponent ) ) );
        if ( null == getJob )
        {
            return null;
        }
        return getJob.getId();
    }
    
    public boolean isIomJob( final UUID jobId )
    {
        final Job job = retrieve( jobId );
        return job != null && job.getIomType() != IomType.NONE;
    }
    
    
    public DataMigration getDataMigration( final UUID jobId )
    {
        return getServiceManager().getRetriever( DataMigration.class ).retrieve(
                Require.any(
                            Require.beanPropertyEquals( DataMigration.GET_JOB_ID, jobId),
                            Require.beanPropertyEquals( DataMigration.PUT_JOB_ID, jobId) ) );
    }
    
    
    public Set< Job > closeOldAggregatingJobs( final int minsOldRequiredToClearJobAppendability )
    {
        if ( !getServiceManager().isTransaction() )
        {
            throw new IllegalStateException( 
                    "Clearing job aggregation must always be called inside a transaction." );
        }
        
        final Set< Job > jobs = retrieveAll( Require.all( 
                Require.beanPropertyEquals( Job.AGGREGATING, Boolean.TRUE ),
                Require.beanPropertyLessThan( 
                        JobObservable.CREATED_AT,
                        new Date( System.currentTimeMillis()
                                  - minsOldRequiredToClearJobAppendability * 60 * 1000L ) ) ) ).toSet();
        if ( !jobs.isEmpty() )
        {
            getDataManager().updateBeans( 
                    CollectionFactory.toSet( Job.AGGREGATING ), 
                    BeanFactory.newBean( Job.class ).setAggregating( false ),
                    Require.beanPropertyEqualsOneOf( 
                            Identifiable.ID, BeanUtils.extractPropertyValues( jobs, Identifiable.ID ) ) );
        }
        
        return retrieveAll( BeanUtils.extractPropertyValues( jobs, Identifiable.ID ) ).toSet();
    }
    
    
    public Job closeAggregatingJob( final UUID jobId )
    {
        if ( !getServiceManager().isTransaction() )
        {
            throw new IllegalStateException( 
                    "Clearing job aggregation must always be called inside a transaction." );
        }
        final Job job = attain( jobId );
        if ( job.isAggregating() )
        {
            getDataManager().updateBean( CollectionFactory.toSet( Job.AGGREGATING ), job.setAggregating( false ) );
        }
        else
        {
            throw new DaoException(
                    GenericFailure.CONFLICT, 
                    "Job " + jobId + " is already closed or is not an aggregating job." );
        }
        return job;
    }
    
    
    public void cleanUpCompletedJobsAndJobChunks(
            final JobProgressManager jobProgressManager,
            final TapeEjector tapeEjector,
            final Object jobReshapingLock )
    {
        cleanUpCompletedJobs( jobProgressManager, tapeEjector, jobReshapingLock );
    }
    
    
    private void cleanUpCompletedJobs(
            final JobProgressManager jobProgressManager, 
            final TapeEjector tapeEjector,
            final Object jobReshapingLock )
    {
        synchronized ( jobReshapingLock )
        {
            final MonitoredWork work = new MonitoredWork( 
                    StackTraceLogging.SHORT, "Clean up completed jobs" );
            try
            {
                final Set< Job > jobs = retrieveAll( Require.not( Require.exists(
                        JobEntry.class, JobEntry.JOB_ID, Require.nothing() ) ) ).toSet();
                if ( jobs.isEmpty() )
                {
                    return;
                }
                
                jobProgressManager.flush();
                for ( final Job staleJob : jobs )
                {
                    final Job job = retrieve( staleJob.getId() );
                    if ( null == job )
                    {
                        continue;
                    }
                    autoEjectTapes( 
                            job.getBucketId(),
                            StorageDomain.AUTO_EJECT_UPON_JOB_COMPLETION,
                            tapeEjector );
                    getServiceManager().getNotificationEventDispatcher().fire( new JobNotificationEvent(
                            job,
                            getServiceManager().getRetriever( JobCompletedNotificationRegistration.class ),
                            new JobCompletedNotificationPayloadGenerator( 
                                    job.getId(),
                                    job.isTruncated(),
                                    getServiceManager().getRetriever( S3Object.class ),
                                    getServiceManager().getRetriever( Blob.class ) ) ) );
                    try
                    {
                        final Class< ? extends DatabasePersistable > cjType = ( job.isTruncated() ) ?
                                CanceledJob.class
                                : CompletedJob.class;
                        final DatabasePersistable cjBean = BeanFactory.newBean( cjType );
                        BeanCopier.copy( cjBean, job );
                        if ( job.isTruncated() )
                        {
                            ( (CanceledJob)cjBean ).setCanceledDueToTimeout( job.isTruncatedDueToTimeout() );
                        }
                        getDataManager().deleteBean( cjType, job.getId() );
                        getDataManager().createBean( cjBean );
                        delete( job.getId() );
                    }
                    catch ( final DaoException ex )
                    {
                        LOG.warn( "Failed to create completed job.  This can happen if multiple " 
                                  + "service instances attempt to complete the job concurrently.  " 
                                  + "Will assume that another instance is completing the job and " 
                                  + "continue on.", ex );
                    }
                }
                
                cleanUpOldJobs( CompletedJob.class, CompletedJob.DATE_COMPLETED );
                cleanUpOldJobs( CanceledJob.class, CanceledJob.DATE_CANCELED );
            }
            finally
            {
                work.completed();
            }
        }
    }
    
    
    private < T extends DatabasePersistable & JobObservable< T > >void cleanUpOldJobs(
            final Class< T > type, 
            final String dateProperty )
    {
        getDataManager().deleteBeans( 
                type,
                Require.beanPropertyLessThan(
                        dateProperty,
                        new Date( System.currentTimeMillis() 
                                - JobObservable.HOURS_TO_RETAIN_ONCE_DONE * 60L * 60 * 1000 ) ) );
        if ( JobObservable.MAX_COUNT_TO_RETAIN_ONCE_DONE * 1.1
                > getServiceManager().getRetriever( type ).getCount() )
        {
            return;
        }
    
        final BeanSQLOrdering ordering = new BeanSQLOrdering();
        ordering.add( dateProperty, SortBy.Direction.DESCENDING );
    
        final Set< UUID > uuids = getServiceManager().getRetriever( type )
                                                     .retrieveAll( Query.where( Require.all() )
                                                                        .orderBy( ordering )
                                                                        .offset(
                                                                                JobObservable
                                                                                        .MAX_COUNT_TO_RETAIN_ONCE_DONE ) )
                                                     .toSet()
                                                     .parallelStream()
                                                     .map( Identifiable::getId )
                                                     .collect( Collectors.toSet() );
    
        getDataManager().deleteBeans( type, Require.beanPropertyEqualsOneOf( Identifiable.ID, uuids ) );
    }
    
    
    public void autoEjectTapes(
            final UUID bucketId,
            final String storageDomainJobAutoEjectProperty, 
            final TapeEjector tapeEjector )
    {
        final Map< UUID, BlobStoreTaskPriority > tapeIds = new HashMap<>();
        final Bucket bucket = getServiceManager().getRetriever( Bucket.class ).attain( bucketId );
        for ( final DataPersistenceRule rule 
                : getServiceManager().getRetriever( DataPersistenceRule.class ).retrieveAll(
                        DataPlacement.DATA_POLICY_ID, bucket.getDataPolicyId() ).toSet() )
        {
            final StorageDomain storageDomain =
                    getServiceManager().getRetriever( StorageDomain.class ).attain( 
                            rule.getStorageDomainId() );
            try
            {
                final Method reader = 
                        BeanUtils.getReader( StorageDomain.class, storageDomainJobAutoEjectProperty );
                final boolean autoEject = ( Boolean ) reader.invoke( storageDomain );
                if ( !autoEject )
                {
                    continue;
                }
            }
            catch ( final Exception ex )
            {
                throw new RuntimeException( ex );
            }
            
            final Set< UUID > tIds;
            if ( DataIsolationLevel.STANDARD == rule.getIsolationLevel() )
            {
                tIds = getTapesRequiringAutoEject( storageDomain.getId(), null );
            }
            else
            {
                tIds = getTapesRequiringAutoEject( storageDomain.getId(), bucket.getId() );
            }
            for ( final UUID tapeId : tIds )
            {
                tapeIds.put( tapeId, storageDomain.getVerifyPriorToAutoEject() );
            }
        }
        
        LOG.info( tapeIds.size() + " tapes used by bucket " + bucket.getId() + " require auto-ejection." );
        for ( final Map.Entry< UUID, BlobStoreTaskPriority > e : tapeIds.entrySet() )
        {
            tapeEjector.ejectTape( 
                    e.getValue(),
                    e.getKey(), 
                    "Auto-exported since storage domain is " + storageDomainJobAutoEjectProperty,
                    null );
        }
    }
    
    
    private Set< UUID > getTapesRequiringAutoEject( final UUID storageDomainId, final UUID bucketId )
    {
        final Set< Tape > retval = new HashSet<>();
        retval.addAll( getServiceManager().getRetriever( Tape.class ).retrieveAll( Require.all(
                Require.not( Require.beanPropertyEqualsOneOf(
                        Tape.STATE,
                        TapeState.getStatesThatAreNotPhysicallyPresent() ) ),
                Require.exists(
                        PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, 
                        Require.beanPropertyEquals( StorageDomainMember.STORAGE_DOMAIN_ID, storageDomainId ) ),
                Require.beanPropertyEquals( PersistenceTarget.BUCKET_ID, bucketId ),
                Require.beanPropertyEquals( Tape.EJECT_PENDING, null ) ) ).toSet() );
        return BeanUtils.toMap( retval ).keySet();
    }
}
