/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.shutdown;

/**
 * If the thing that can be shutdown is disposed of having never been shutdown and thus this listener is
 * never called, it's ok i.e. this listener does not <b>have</b> to do any resource cleanup or anything
 * where things will be left in a bad state if the listener doesn't get called.
 */
public abstract class StandardShutdownListener implements ShutdownListener
{
    final public boolean isShutdownListenerNotificationCritical()
    {
        return false;
    }
}
