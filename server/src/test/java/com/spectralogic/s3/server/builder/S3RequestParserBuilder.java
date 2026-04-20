package com.spectralogic.s3.server.builder;

import java.util.HashMap;

import javax.servlet.http.HttpServletResponse;

import com.spectralogic.s3.common.testfrmwrk.MockHttpServletRequest;
import com.spectralogic.s3.server.request.DS3RequestImpl;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.mock.InterfaceProxyFactory;

public class S3RequestParserBuilder
{

    private final HashMap<String,String> m_headers = new HashMap<>();
    private final String m_url;

    private RequestType m_type = RequestType.GET;

    public S3RequestParserBuilder(final String url)
    {
        m_url = url;
    }

    public S3RequestParserBuilder withType(final RequestType type)
    {
        m_type = type;
        return this;
    }

    public S3RequestParserBuilder withHeader(final String key, final String value)
    {
        m_headers.put(key, value);
        return this;
    }

    public DS3Request build()
    {
        return new DS3RequestImpl(
                new MockHttpServletRequest( m_type, m_url ).setHeaders( m_headers ).generate(), 
                InterfaceProxyFactory.getProxy( HttpServletResponse.class, null ) );
    }
}
