/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.shutdown;

public interface ShutdownListener
{
    void shutdownOccurred();
    
    
    /**
     * @return TRUE if this listener must be run before the thing to shutdown is garbage collected in order
     * to clean up resources
     */
    boolean isShutdownListenerNotificationCritical();
}
