/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.tape.processor.main.api;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.BlobStoreTaskPriority;
import com.spectralogic.s3.common.platform.api.TapeEjector;
import com.spectralogic.s3.dataplanner.backend.api.BlobStoreTaskSchedulingListener;
import com.spectralogic.s3.dataplanner.backend.tape.api.TapeTaskQueue;
import com.spectralogic.util.shutdown.Shutdownable;

public interface TapeBlobStoreProcessor extends Shutdownable
{
    /**
     * The processor will not process any tasks until it has been started.
     */
    void start();
    
    
    /**
     * @return null if the inspection was scheduled, or the reason why the inspection wasn't scheduled as a
     * string
     */
    String scheduleInspection( final BlobStoreTaskPriority priority, final UUID tapeId, final boolean force );
    
    
    /**
     * @return null if the cleaning was scheduled, or the reason why the cleaning wasn't scheduled as a
     * string
     */
    String scheduleCleaning( final UUID tapeDriveId );


    /**
     * @return null if the test was scheduled, or the reason why the test wasn't scheduled as a
     * string
     */
    String scheduleTest(final UUID tapeDriveId, final UUID tapeId, boolean cleanFirst);

    /**
     * Requests a drive dump and blocks until it is complete.
     */
    void driveDump(final UUID driveId);


    /**
     * @return all queued and in-progress miscellaneous tape tasks
     */
    TapeTaskQueue getTapeTasks();
    
    
    /**
     * @return the lock for task state
     */
    Object getTaskStateLock();
    

    void addTaskSchedulingListener( final BlobStoreTaskSchedulingListener listener );
    
    
    void taskSchedulingRequired();


    // NOTE: the ejector and processor must hold references to each other so only one can go into the constructor of the other,
    // the other must be set after the fact
    void setTapeEjector(final TapeEjector ejector);
}