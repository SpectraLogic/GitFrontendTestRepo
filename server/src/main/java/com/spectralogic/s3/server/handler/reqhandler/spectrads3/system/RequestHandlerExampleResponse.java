/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.system;

import com.spectralogic.util.marshal.BaseMarshalable;

public final class RequestHandlerExampleResponse extends BaseMarshalable
{
    public RequestHandlerExampleResponse( 
            final String test,
            final String httpRequest,
            final int httpResponseCode, 
            final String httpResponse,
            final String httpResponseType )
    {
        m_test = test;
        m_httpRequest = httpRequest;
        m_httpResponseCode = httpResponseCode;
        m_httpResponse = httpResponse;
        m_httpResponseType = httpResponseType;
    }

    
    public final static String TEST = "test";

    public String getTest()
    {
        return m_test;
    }

    
    public final static String HTTP_REQUEST = "httpRequest";

    public String getHttpRequest()
    {
        return m_httpRequest;
    }

    
    public final static String HTTP_RESPONSE_CODE = "httpResponseCode";

    public int getHttpResponseCode()
    {
        return m_httpResponseCode;
    }
    

    public final static String HTTP_RESPONSE = "httpResponse";

    public String getHttpResponse()
    {
        return m_httpResponse;
    }
    
    
    public final static String HTTP_RESPONSE_TYPE = "httpResponseType";
    
    public String getHttpResponseType()
    {
        return m_httpResponseType;
    }
    

    private final String m_test;
    private final String m_httpRequest;
    private final int m_httpResponseCode;
    private final String m_httpResponse;
    private final String m_httpResponseType;
}
