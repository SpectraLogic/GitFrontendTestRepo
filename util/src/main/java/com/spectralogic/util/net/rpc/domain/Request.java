/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.net.rpc.domain;

import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;
import com.spectralogic.util.marshal.ExcludeFromMarshaler;
import com.spectralogic.util.marshal.ExcludeFromMarshaler.When;

public interface Request extends SimpleBeanSafeToProxy
{
    String ID = "id";
    
    /**
     * @return a unique identifier that the client can use to correlate the response to the request with the
     * original request, in the event that there are multiple concurrent requests being executed
     */
    long getId();
    
    Request setId( final long value );
    
    
    String TYPE = "type";
    
    /**
     * @return the type (or class) that the request is for
     */
    String getType();
    
    Request setType( final String value );
    
    
    String INSTANCE = "instance";
    
    /**
     * @return the instance of the type that this request is for, or null if the type this request is for is
     * a singleton
     */
    @Optional
    @ExcludeFromMarshaler( When.VALUE_IS_NULL )
    String getInstance();
    
    Request setInstance( final String value );
    
    
    String METHOD = "method";
    
    /**
     * @return the method to invoke
     */
    String getMethod();
    
    Request setMethod( final String value );
    
    
    String PARAMS = "params";
    
    /**
     * @return the parameters (arguments) to the method to invoke
     */
    String getParams();
    
    Request setParams( final String value );
}
