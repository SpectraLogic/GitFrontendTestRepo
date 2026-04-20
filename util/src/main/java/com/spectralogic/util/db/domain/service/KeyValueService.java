/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.domain.service;

import com.spectralogic.util.db.domain.KeyValue;
import com.spectralogic.util.db.service.api.BeansRetriever;

public interface KeyValueService extends BeansRetriever< KeyValue >
{
    void create( final KeyValue kv );
    
    
    String getString( final String keyName );
    
    
    boolean getBoolean( final String keyName );
    
    
    double getDouble( final String keyName );
    
    
    int getInt( final String keyName );
    
    
    long getLong( final String keyName );
    
    
    String getString( final String keyName, final String defaultValue );
    
    
    boolean getBoolean( final String keyName, final boolean defaultValue );
    
    
    double getDouble( final String keyName, final double defaultValue );
    
    
    int getInt( final String keyName, final int defaultValue );
    
    
    long getLong( final String keyName, final long defaultValue );
    
    
    KeyValue retrieve( final String keyName );
    
    
    KeyValue attain( final String keyName );
    
    
    void set( final String keyName, final long value );
    
    
    void set( final String keyName, final String value );
    
    
    void set( final String keyName, final boolean value );
    
    
    void set( final String keyName, final double value );
}
