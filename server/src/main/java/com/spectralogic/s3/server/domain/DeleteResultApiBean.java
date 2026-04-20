/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.domain;

import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;
import com.spectralogic.util.marshal.CustomMarshaledName;
import com.spectralogic.util.marshal.CustomMarshaledTypeName;

@CustomMarshaledTypeName( "DeleteResult" )
public interface DeleteResultApiBean extends SimpleBeanSafeToProxy
{
    String DELETED_OBJECTS = "deletedObjects";

    @CustomMarshaledName( value = "Deleted" )
    S3ObjectToDeleteApiBean [] getDeletedObjects();
    
    void setDeletedObjects( final S3ObjectToDeleteApiBean [] value );
    

    String ERRORS = "errors";
    
    @CustomMarshaledName( value = "Error" )
    DeleteObjectErrorResultApiBean [] getErrors();
    
    void setErrors( final DeleteObjectErrorResultApiBean [] value );
}
