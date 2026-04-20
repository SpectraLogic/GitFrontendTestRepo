/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.manager.frmwrk;

import org.apache.log4j.Logger;

public final class DbLogger
{
    private DbLogger()
    {
        // singleton
    }
    
    
    public final static Logger DB_HOG_LOG = Logger.getLogger( "SpectraDbHogLog" );
    public final static Logger DB_SQL_LOG = Logger.getLogger( "SpectraDbSqlLog" );
}
