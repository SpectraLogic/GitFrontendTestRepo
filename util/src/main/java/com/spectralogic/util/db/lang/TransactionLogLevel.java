/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.lang;

import org.apache.log4j.Level;

public interface TransactionLogLevel
{
    Level getLevel();
    
    
    void add( final int level );
}
