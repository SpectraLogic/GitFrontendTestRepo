/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.lang;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Priority;

public enum SqlLogLevels
{
    DEFAULT(),
    UPDATES_LOGGED_AT_DEBUG_LEVEL(
           new CustomLogLevel( SqlOperation.UPDATE, Priority.DEBUG_INT ) ),
    ALL_OPERATIONS_LOGGED_AT_DEBUG_LEVEL(
           new CustomLogLevel( SqlOperation.SELECT, Priority.DEBUG_INT ),
           new CustomLogLevel( SqlOperation.INSERT, Priority.DEBUG_INT ),
           new CustomLogLevel( SqlOperation.UPDATE, Priority.DEBUG_INT ),
           new CustomLogLevel( SqlOperation.DELETE, Priority.DEBUG_INT ) ),
    ;
    
    private SqlLogLevels( final CustomLogLevel ... customLogLevels )
    {
        for ( final CustomLogLevel customLogLevel : customLogLevels )
        {
            m_customLogLevels.put( customLogLevel.m_operation, Integer.valueOf( customLogLevel.m_logLevel ) );
        }
    }
    
    
    public int getLogLevel( final SqlOperation operation )
    {
        if ( m_customLogLevels.containsKey( operation ) )
        {
            return m_customLogLevels.get( operation ).intValue();
        }
        return operation.getDefaultLogLevel();
    }
    
    
    private final static class CustomLogLevel
    {
        private CustomLogLevel( final SqlOperation operation, final int logLevel )
        {
            m_operation = operation;
            m_logLevel = logLevel;
        }
        
        private final SqlOperation m_operation;
        private final int m_logLevel;
    } // end inner class def
    
    
    private final Map< SqlOperation, Integer > m_customLogLevels = new HashMap<>();
}
