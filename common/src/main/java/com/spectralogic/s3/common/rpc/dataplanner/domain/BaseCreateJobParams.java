/*
 *
 * Copyright C 2018, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.s3.common.rpc.dataplanner.domain;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.BlobStoreTaskPriority;
import com.spectralogic.s3.common.dao.domain.ds3.UserIdObservable;
import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.util.bean.lang.DefaultBooleanValue;
import com.spectralogic.util.bean.lang.DefaultEnumValue;
import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;

public interface BaseCreateJobParams< T extends BaseCreateJobParams< ? > > 
    extends SimpleBeanSafeToProxy, UserIdObservable< T >, NameObservable< T >
{
    String PRIORITY = "priority";
    
    @DefaultEnumValue( "NORMAL" )
    BlobStoreTaskPriority getPriority();
    
    T setPriority( final BlobStoreTaskPriority value );
    
    
    String AGGREGATING = "aggregating";
    
    @DefaultBooleanValue( false )
    boolean isAggregating();
    
    T setAggregating( final boolean value );
    
    
    String DEAD_JOB_CLEANUP_ALLOWED = "deadJobCleanupAllowed";
    
    @DefaultBooleanValue( true )
    boolean isDeadJobCleanupAllowed();
    
    T setDeadJobCleanupAllowed( final boolean value );
    
    
    String NAKED = "naked";
    
    @DefaultBooleanValue( false )
    boolean isNaked();
    
    T setNaked( final boolean value );
    
    
    String IMPLICIT_JOB_ID_RESOLUTION = "implicitJobIdResolution";
    
    @DefaultBooleanValue( false )
    boolean isImplicitJobIdResolution();
    
    T setImplicitJobIdResolution( final boolean value );
    
    
    T setUserId( final UUID value );
    
    
    // Currently force-pre-allocating get jobs is not implemented, just put jobs
    String PRE_ALLOCATE_JOB_SPACE = "preAllocateJobSpace";
    
    @DefaultBooleanValue( false )
    boolean isPreAllocateJobSpace();
    
    T setPreAllocateJobSpace( final boolean value );


    String PROTECTED = "protected";

    @DefaultBooleanValue( false )
    boolean isProtected();

    T setProtected(final boolean value );
    
    
    @Optional
    String getName();
}
