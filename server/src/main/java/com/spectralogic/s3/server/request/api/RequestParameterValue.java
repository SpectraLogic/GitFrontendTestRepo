/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.request.api;

import java.util.UUID;

import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.db.service.api.BeansRetriever;


public interface RequestParameterValue
{
    < B extends Identifiable > B getBean( final BeansRetriever< B > retrieverToDiscoverWith );
    
    
    UUID getUuid();
    
    
    int getInt();
    
    
    long getLong();
    
    
    String getString();
    
    
    < T > T getEnum( final Class< T > type );
}
