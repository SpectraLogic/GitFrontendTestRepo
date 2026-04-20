/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.net.rpc.mockresource;

import java.util.UUID;

import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;

public interface Mortgage extends SimpleBeanSafeToProxy
{
    String CLIENT_IDS = "clientIds";
    
    UUID [] getClientIds();
    
    void setClientIds( final UUID [] value );
    
    
    String MORTGAGE_ID = "mortgageId";
    
    UUID getMortgageId();
    
    void setMortgageId( final UUID value );
    
    
    String TOTAL = "total";
    
    int getTotal();
    
    void setTotal( final int value );
    
    
    String REMAINING = "remaining";
    
    int getRemaining();
    
    void setRemaining( final int value );
    
    
    String IGNORED_PROP = "ignoredProp";
    
    @Optional
    String getIgnoredProp();
    
    void setIgnoredProp( final String value );
}