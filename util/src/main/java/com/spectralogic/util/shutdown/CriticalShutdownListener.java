/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.shutdown;

/**
 * If this shutdown listener doesn't run, necessary resource cleanup will not be performed.  If the shutdown
 * monitor sees that it's being garbage collected and shutdown has not been called and at least one required
 * listener is registered, a warning will be logged.
 */
public abstract class CriticalShutdownListener implements ShutdownListener
{
    final public boolean isShutdownListenerNotificationCritical()
    {
        return true;
    }
}
