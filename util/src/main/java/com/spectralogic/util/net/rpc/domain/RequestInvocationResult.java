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

public interface RequestInvocationResult extends SimpleBeanSafeToProxy
{
    String RETURN_VALUE = "returnValue";
    
    /**
     * @return the return value from the request
     */
    @Optional
    @ExcludeFromMarshaler( When.VALUE_IS_NULL )
    String getReturnValue();
    
    RequestInvocationResult setReturnValue( final String value );
}
