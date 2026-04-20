/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.domain.service;

import com.spectralogic.util.lang.Validations;

public final class MutexedRunnable implements Runnable
{
    public MutexedRunnable( final String lockName, final MutexService service, final Runnable r )
    {
        m_lockName = lockName;
        m_service = service;
        m_r = r;
        
        Validations.verifyNotNull( "Lock name", m_lockName );
        Validations.verifyNotNull( "Service", m_service );
        Validations.verifyNotNull( "Runnable", m_r );
    }
    
    
    synchronized public void run()
    {
        m_service.run( m_lockName, m_r );
    }
    

    private final String m_lockName;
    private final MutexService m_service;
    private final Runnable m_r;
}
