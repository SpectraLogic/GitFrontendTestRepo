/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.http;


public enum HttpResponseFormatType
{
    DEFAULT,
    JSON,
    XML
    ;
    
    
    public static HttpResponseFormatType valueOf( final String pathInfo, final String contentType )
    {
        if ( pathInfo.toLowerCase().endsWith( ".json" ) )
        {
            return JSON;
        }
        if ( pathInfo.toLowerCase().endsWith( ".xml" ) )
        {
            return XML;
        }

        if ( null == contentType )
        {
            return DEFAULT;
        }
        if ( contentType.contains( "/json" ) )
        {
            return JSON;
        }
        if ( contentType.contains( "/xml" ) )
        {
            return XML;
        }
        
        return DEFAULT;
    }
}
