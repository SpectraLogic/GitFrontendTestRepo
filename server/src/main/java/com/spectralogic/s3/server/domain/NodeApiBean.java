/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.domain;

import java.util.UUID;

import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;
import com.spectralogic.util.marshal.ExcludeFromMarshaler;
import com.spectralogic.util.marshal.MarshalXmlAsAttribute;
import com.spectralogic.util.marshal.ExcludeFromMarshaler.When;

public interface NodeApiBean extends SimpleBeanSafeToProxy, Identifiable
{
    @MarshalXmlAsAttribute
    UUID getId();
            
            
    String END_POINT = "endPoint";

    @MarshalXmlAsAttribute
    String getEndPoint();
    
    NodeApiBean setEndPoint( final String value );
    
    
    String HTTP_PORT = "httpPort";

    @Optional
    @MarshalXmlAsAttribute
    @ExcludeFromMarshaler( When.VALUE_IS_NULL )
    Integer getHttpPort();
    
    NodeApiBean setHttpPort( final Integer value );
    
    
    String HTTPS_PORT = "httpsPort";
    
    @Optional
    @MarshalXmlAsAttribute
    @ExcludeFromMarshaler( When.VALUE_IS_NULL )
    Integer getHttpsPort();
    
    NodeApiBean setHttpsPort( final Integer value );
}
