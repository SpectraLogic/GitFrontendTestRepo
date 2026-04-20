/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.lang;

import org.apache.log4j.Level;
import org.apache.log4j.Priority;

public final class TransactionLogLevelImpl implements TransactionLogLevel
{
    synchronized public Level getLevel()
    {
        return Level.toLevel( m_level );
    }
    
    
    synchronized public void add( final int level )
    {
        if ( level > m_level )
        {
            m_level = level;
        }
    }
    
    
    private int m_level = Priority.DEBUG_INT;
}
