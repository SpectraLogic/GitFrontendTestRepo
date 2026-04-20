/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.canhandledeterminer;

import java.util.List;

import com.spectralogic.s3.server.request.rest.RestActionType;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.s3.server.request.rest.RestOperationType;
import com.spectralogic.s3.server.request.rest.RestResourceType;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.marshal.CustomMarshaledName;
import com.spectralogic.util.marshal.CustomMarshaledName.CollectionNameRenderingMode;
import com.spectralogic.util.marshal.ExcludeFromMarshaler;
import com.spectralogic.util.marshal.ExcludeFromMarshaler.When;
import com.spectralogic.util.marshal.MarshalXmlAsAttribute;

public interface RequestHandlerRequestContract extends SimpleBeanSafeToProxy
{
    String ACTION = "action";

    @ExcludeFromMarshaler( When.VALUE_IS_NULL )
    @MarshalXmlAsAttribute
    RestActionType getAction();
    
    void setAction( final RestActionType value );
    
    
    String OBJECT_REQUIREMENT = "objectRequirement";

    @ExcludeFromMarshaler( When.VALUE_IS_NULL )
    @MarshalXmlAsAttribute
    S3ObjectRequirement getObjectRequirement();
    
    void setObjectRequirement( final S3ObjectRequirement value );
    
    
    String BUCKET_REQUIREMENT = "bucketRequirement";

    @ExcludeFromMarshaler( When.VALUE_IS_NULL )
    @MarshalXmlAsAttribute
    BucketRequirement getBucketRequirement();
    
    void setBucketRequirement( final BucketRequirement value );
    
    
    String HTTP_VERB = "httpVerb";

    @MarshalXmlAsAttribute
    RequestType getHttpVerb();
    
    void setHttpVerb( final RequestType value );
    
    
    String INCLUDE_ID_IN_PATH = "includeIdInPath";
    
    @MarshalXmlAsAttribute
    boolean isIncludeIdInPath();
    
    void setIncludeIdInPath( final boolean value );
    
    
    String RESOURCE = "resource";

    @MarshalXmlAsAttribute
    RestDomainType getResource();
    
    void setResource( final RestDomainType value );
    
    
    String RESOURCE_TYPE = "resourceType";

    @MarshalXmlAsAttribute
    RestResourceType getResourceType();
    
    void setResourceType( final RestResourceType value );
    
    
    String OPERATION = "operation";

    @ExcludeFromMarshaler( When.VALUE_IS_NULL )
    @MarshalXmlAsAttribute
    RestOperationType getOperation();
    
    void setOperation( final RestOperationType value );
    
    
    String REQUIRED_PARAMS = "requiredParams";

    @CustomMarshaledName( 
            collectionValue = "RequiredQueryParams", 
            value = "Param", 
            collectionValueRenderingMode = CollectionNameRenderingMode.SINGLE_BLOCK_FOR_ALL_ELEMENTS )
    List< RequestHandlerParamContract > getRequiredParams();
    
    void setRequiredParams( final List< RequestHandlerParamContract > value );
    
    
    String OPTIONAL_PARAMS = "optionalParams";

    @CustomMarshaledName( 
            collectionValue = "OptionalQueryParams", 
            value = "Param", 
            collectionValueRenderingMode = CollectionNameRenderingMode.SINGLE_BLOCK_FOR_ALL_ELEMENTS )
    List< RequestHandlerParamContract > getOptionalParams();
    
    void setOptionalParams( final List< RequestHandlerParamContract > value );
    
    
    interface RequestHandlerParamContract extends SimpleBeanSafeToProxy
    {
        String NAME = "name";

        @MarshalXmlAsAttribute
        String getName();
        
        void setName( final String value );
        
        
        String TYPE = "type";

        @MarshalXmlAsAttribute
        String getType();
        
        void setType( final String value );
    } // end inner class def
}
