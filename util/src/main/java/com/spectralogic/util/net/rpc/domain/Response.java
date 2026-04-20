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

/**
 * Response for a {@link Request} previously issued.  <br><br>
 * 
 * Only a single response may be sent for any given request (for example, you cannot send a "success" response
 * for a request, then come back later and send a failure response for that same request).
 */
public interface Response extends SimpleBeanSafeToProxy
{
    String REQUEST_ID = "requestId";
    
    /**
     * @return a unique identifier that the client can use to correlate the response to the request with the
     * original request, in the event that there are multiple concurrent requests being executed
     */
    long getRequestId();
    
    Response setRequestId( final long value );
    
    
    String FAILURE = "failure";
    
    /**
     * @return the failure that occurred (if this is set, a return value cannot be set)
     */
    @Optional
    @ExcludeFromMarshaler( When.VALUE_IS_NULL )
    Failure getFailure();
    
    Response setFailure( final Failure value );
    
    
    String SUCCESS = "success";
    
    /**
     * @return the request invocation result for the request (if this is set, a failure cannot be set)
     */
    @Optional
    @ExcludeFromMarshaler( When.VALUE_IS_NULL )
    RequestInvocationResult getSuccess();
    
    Response setSuccess( final RequestInvocationResult value );
}
