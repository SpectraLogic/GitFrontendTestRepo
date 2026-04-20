/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.target;

import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.s3.common.dao.domain.shared.Quiesced;
import com.spectralogic.util.bean.lang.DefaultBooleanValue;
import com.spectralogic.util.bean.lang.DefaultEnumValue;

public interface ReplicationTarget< T > extends NameObservable< T >
{
    String STATE = "state";
    
    @DefaultEnumValue( "ONLINE" )
    TargetState getState();
    
    T setState( final TargetState value );
    
    
    String QUIESCED = "quiesced";

    @DefaultEnumValue( "NO" )
    Quiesced getQuiesced();
    
    T setQuiesced( final Quiesced value );
    
    
    String DEFAULT_READ_PREFERENCE = "defaultReadPreference";
    
    @DefaultEnumValue( "LAST_RESORT" )
    TargetReadPreferenceType getDefaultReadPreference();
    
    T setDefaultReadPreference( final TargetReadPreferenceType value );
    
    
    String PERMIT_GOING_OUT_OF_SYNC = "permitGoingOutOfSync";
    
    @DefaultBooleanValue( false )
    boolean isPermitGoingOutOfSync();
    
    T setPermitGoingOutOfSync( final boolean value );
}
