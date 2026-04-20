/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.domain;

import java.util.List;

import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;

public interface S3ObjectsToDeleteApiBean extends SimpleBeanSafeToProxy
{
    /**
     * Per the Amazon S3 REST specification: "In quiet mode the response includes only keys
     * where the delete operation encountered an error."
     */
    String QUIET = "quiet";
    
    boolean isQuiet();
    
    void setQuiet( final boolean value );

    
    String OBJECTS_TO_DELETE = "objectsToDelete";

    List< S3ObjectToDeleteApiBean > getObjectsToDelete();
    
    void setObjectsToDelete( final List< S3ObjectToDeleteApiBean > value );
}
