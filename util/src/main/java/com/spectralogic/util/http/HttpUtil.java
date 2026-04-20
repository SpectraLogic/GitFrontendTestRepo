/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.http;

import java.net.URLConnection;
import java.security.cert.X509Certificate;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import com.spectralogic.util.lang.Validations;

public final class HttpUtil
{
    private HttpUtil()
    {
        // singleton
    }
    
    
    public static void hackConnectionForBadSslCertificate( final URLConnection urlConnection )
    {
        try
        {
            hackConnectionForBadSslCertificateInternal( urlConnection );
        }
        catch ( final Exception ex )
        {
            if ( RuntimeException.class.isAssignableFrom( ex.getClass() ) )
            {
                throw (RuntimeException)ex;
            }
            throw new RuntimeException( ex );
        }
    }
    
    
    private static void hackConnectionForBadSslCertificateInternal( final URLConnection urlConnection )
        throws Exception
    {
        Validations.verifyNotNull( "URL connection", urlConnection );
        if ( !HttpsURLConnection.class.isAssignableFrom( urlConnection.getClass() ) )
        {
            return;
        }
        
        final HttpsURLConnection connection = (HttpsURLConnection)urlConnection;
        final TrustManager[] trustAllCerts = new TrustManager[] { new X509TrustManager() 
        {
            @Override
            public void checkClientTrusted( final X509Certificate[] chain, final String authType ) 
            {
                // empty
            }
            
            @Override
            public void checkServerTrusted( final X509Certificate[] chain, final String authType ) 
            {
                // empty
            }
            
            @Override
            public X509Certificate[] getAcceptedIssuers() 
            {
                return null;
            }
        } };
        
        // Install the all-trusting trust manager
        final SSLContext sslContext = SSLContext.getInstance( "SSL" );
        sslContext.init( null, trustAllCerts, new java.security.SecureRandom() );
        // Create an ssl socket factory with our all-trusting manager
        final SSLSocketFactory sslSocketFactory = sslContext.getSocketFactory();
        
        connection.setSSLSocketFactory( sslSocketFactory );
        connection.setHostnameVerifier( new HostnameVerifier()
        { 
            public boolean verify( final String hostname, final SSLSession sslSession) 
            { 
                if ( hostname.equals( "localhost" ) ) 
                { 
                    return true; 
                } 
                return false;
            } 
        } );
    }
    
    
    public static String formatForIpV6( final String address )
    {
    	if ( null == address )
    	{
    		return null;
    	}
    	String retval = address;
        if ( retval.matches(".*:.*:.*") && ! retval.contains("[") )
        {
        	retval = "[" + retval + "]";
        }
        return retval;
    }
}
