/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.shutdown;

import java.util.List;

public interface Shutdownable extends ShutdownListener
{
    boolean isShutdown();
    
    
    void shutdown();
    
    
    void verifyNotShutdown();
    
    
    void addShutdownListener( final ShutdownListener listener );
    
    
    List< ShutdownListener > getShutdownListeners();
}
