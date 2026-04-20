/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.shared;

import com.spectralogic.util.shutdown.Shutdownable;


/**
 * Allows a client to note all active failures of a certain classification and to commit the entire active
 * failure set for that classification, where any active failure that previously existed is retained, any 
 * failure that previously existed but no longer exists is deleted, and any new failure is recorded.
 */
public interface ActiveFailures extends Shutdownable
{
    void add( final Throwable t );
    
    
    void add( final String failure );
    
    
    void commit();
}
