/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.thread.wp;

public final class SystemWorkPool
{
    private SystemWorkPool()
    {
        // singleton
    }
    
    
    public static WorkPool getInstance()
    {
        return INSTANCE;
    }
    
    
    private final static WorkPool INSTANCE = WorkPoolFactory.createWorkPool( 128, "SystemWorkPool" );
}
