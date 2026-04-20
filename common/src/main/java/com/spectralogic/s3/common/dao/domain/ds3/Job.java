/*******************************************************************************
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.ds3;

import com.spectralogic.util.bean.lang.DefaultBooleanValue;
import com.spectralogic.util.bean.lang.DefaultEnumValue;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.lang.Index;
import com.spectralogic.util.db.lang.Indexes;
import com.spectralogic.util.marshal.CustomMarshaledName;

/**
 * An in-progress I/O job (reading or writing {@link Blob}s).
 */
@Indexes( { @Index( Job.CREATED_AT ), @Index( Job.NAME ), @Index( Job.PRIORITY ) } )
public interface Job extends DatabasePersistable, JobObservable< Job >
{
    String AGGREGATING = "aggregating";
    
    /**
     * @return TRUE if the job can be reshaped (e.g. have additional work appended to it in order to 
     * aggregate work across multiple jobs) <br><br>
     * 
     * When jobs are created in an implicit manner (for example, via an S3 PUT rather than a DS3 create PUT
     * job, where the job creation is an implicit act, rather than an explicit one), the client does not have
     * any explicit dependency on the job, and thus, the job may aggregate new work coming in and be reshaped
     * up until we begin to process it.  Any jobs created in an explicit manner can never be safely reshaped, 
     * since we've reported back to the client what the job shape is and confusion could result if we reshape 
     * the job after reporting it to a client, unless of course, the client specifies when creating the job
     * that the job can be reshaped by asking to create an aggregating job.  <br><br>
     * 
     * When creating an aggregating job, a previously-created aggregating job may be used, in which case the
     * previous aggregating job will have the current job's work appended to it, and where there would have
     * been two jobs, there is now one.  Note that once we begin processing an aggregating job on the backend,
     * the job must first be marked as non-aggregating, at which point, no additional reshaping or appending
     * may occur to the job.
     */
    @DefaultBooleanValue( false )
    boolean isAggregating();
    
    Job setAggregating( final boolean value );
    
    
    String DEAD_JOB_CLEANUP_ALLOWED = "deadJobCleanupAllowed";
    
    /**
     * @return TRUE if this job can be cancelled or truncated automatically after a period of inactivity
     */
    @DefaultBooleanValue( true )
    boolean isDeadJobCleanupAllowed();
    
    Job setDeadJobCleanupAllowed( final boolean value );
    
    
    String IOM_TYPE = "iomType";

    /**
     * @return TRUE if this job is re-persisting objects that were already known to the database.
     */
    @DefaultEnumValue( "NONE" )
    @CustomMarshaledName( "Restore" )
    IomType getIomType();
    
    Job setIomType(final IomType value );
    
    
    String MINIMIZE_SPANNING_ACROSS_MEDIA = "minimizeSpanningAcrossMedia";
    
    @DefaultBooleanValue( false )
    boolean isMinimizeSpanningAcrossMedia();
    
    Job setMinimizeSpanningAcrossMedia( final boolean value );
    
    
    String TRUNCATED_DUE_TO_TIMEOUT = "truncatedDueToTimeout";
    
    @DefaultBooleanValue( false )
    boolean isTruncatedDueToTimeout();
    
    Job setTruncatedDueToTimeout( final boolean value );
    
    
    String IMPLICIT_JOB_ID_RESOLUTION = "implicitJobIdResolution";
    
    /**
     * @return TRUE if the job id does not have to be specified in requests to PUT or GET {@link Blob}s that
     * are part of this job; FALSE otherwise <br><br>
     * 
     * For example, naked GETs and PUTs have no concept of what a job is, so they can never reference a job
     * id and thus must use implicit job id resolution.  Clients who are job aware should whenever possible
     * include the job id in GETs and PUTs; however, if the client is unable to do so for any reason, the
     * client can enable implicit job id resolution, which will permit the client to send GETs and PUTs
     * without referencing the job id and have the job id implicitly resolved. <br><br> 
     * 
     * Implicitly resolving a GET or PUT to a job is not always reliable.  For example, if two clients GET
     * the same object via two different jobs, implicit resolution may result in the wrong job marking the
     * GET as having completed.  For this reason, clients who are job aware are strongly encouraged to always
     * explicitly provide the job id and leave implicit job id resolution disabled (the default behavior).
     */
    @DefaultBooleanValue( false )
    boolean isImplicitJobIdResolution();
    
    Job setImplicitJobIdResolution( final boolean value );
    
    
    String VERIFY_AFTER_WRITE = "verifyAfterWrite";
    
    /**
     * @return true if data should be verified after it is written (only applies to PUT jobs)
     */
    boolean isVerifyAfterWrite();
    
    Job setVerifyAfterWrite( final boolean value );
    
    
    String REPLICATING = "replicating";
    
    /**
     * @return TRUE if this job is being replicated
     */
    @DefaultBooleanValue( false )
    boolean isReplicating();
    
    Job setReplicating( final boolean value );


    String PROTECTED = "protected";

    /**
     * @return true if this is a protected job that cannot be canceled. This is used by clients as a safeguard to
     * prevent accidental cancellation of important jobs.
     */
    @DefaultBooleanValue( false )
    boolean isProtected();

    Job setProtected( final boolean value );
}
