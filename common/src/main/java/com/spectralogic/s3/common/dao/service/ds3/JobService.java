/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.ds3;

import java.util.Set;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.DataMigration;
import com.spectralogic.s3.common.dao.domain.ds3.Job;
import com.spectralogic.s3.common.platform.api.TapeEjector;
import com.spectralogic.util.db.service.api.BeanCreator;
import com.spectralogic.util.db.service.api.BeanDeleter;
import com.spectralogic.util.db.service.api.BeanUpdater;
import com.spectralogic.util.db.service.api.BeansRetriever;

public interface JobService 
    extends BeansRetriever< Job >, BeanCreator< Job >, BeanUpdater< Job >, BeanDeleter
{
    /**
     * Migrates the contents of the src job to the dest job, then deletes the src job
     */
    void migrate( final UUID destinationJobId, final UUID srcJobId );
    
    
    /**
     * @return Set <job id> of jobs that have been cleared of their reshapability and are now finalized,
     * ready for processing
     */
    Set< Job > closeOldAggregatingJobs( final int minsOldRequiredToClearJobAppendability );
    
    
    public Job closeAggregatingJob( final UUID jobId );
    
    
    void cleanUpCompletedJobsAndJobChunks( 
            final JobProgressManager jobProgressManager,
            final TapeEjector tapeEjector,
            final Object jobReshapingLock );
    
    
    void autoEjectTapes( 
            final UUID bucketId,
            final String storageDomainJobAutoEjectProperty, 
            final TapeEjector tapeEjector );
            
            
    UUID getPutJobComponentOfDataMigration( final UUID getJobComponent );
    
    
    UUID getGetJobComponentOfDataMigration( final UUID putJobComponent );
    
    
    boolean isIomJob( final UUID jobId );
    
    
    public DataMigration getDataMigration( final UUID jobId );
}
