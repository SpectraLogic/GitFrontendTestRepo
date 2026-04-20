/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.platform.aws.security;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Logger;

import com.spectralogic.s3.common.platform.aws.AWSFailure;
import com.spectralogic.s3.common.platform.aws.S3HeaderType;
import com.spectralogic.util.exception.FailureTypeObservableException;
import com.spectralogic.util.http.HttpRequest;
import com.spectralogic.util.lang.Platform;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.marshal.DateMarshaler;
import com.spectralogic.util.security.ChecksumType;

/**
 * Validates the signature using the AWS S3 V2 specification, as documented at:
 * <a href = http://docs.aws.amazon.com/AmazonS3/latest/dev/RESTAuthentication.html>amazon spec</a>
 */
public final class V2AuthorizationSignatureValidator implements AuthorizationSignatureValidator
{
    public V2AuthorizationSignatureValidator( final HttpRequest request, final String secretKey )
    {
        m_request = request;
        m_secretKey = secretKey;
        Validations.verifyNotNull( "Request", m_request );
        Validations.verifyNotNull( "Secret key", m_secretKey );
    }
    
    
    public void validate( final String digest )
    {
        final String stringToSign = getStringToSign();
        final String loggableStringToSign = stringToSign.replace( Platform.SLASH_N, "\\n" );
        LOG.debug( "String to sign is: " + loggableStringToSign );
        final String correctDigest = sign( stringToSign );
        if ( !correctDigest.equals( digest ) )
        {
            LOG.info( "Authorization digest from client was incorrect.  Was '" + digest 
                      + "', but should have been: " + correctDigest );
            throw new IllegalArgumentException( 
                    "Authorization digest from client was incorrect.  Valid string to sign was:" 
                    + loggableStringToSign );
        }
    }

    
    private String sign( final String stringToSign ) 
    {
        try 
        {
            final byte[] secretyKeyBytes = m_secretKey.getBytes( UTF8_CHARSET );
            final SecretKeySpec secretKeySpec = new SecretKeySpec( secretyKeyBytes, HMAC_ALGORITHM );
            final Mac mac = Mac.getInstance( HMAC_ALGORITHM );
            mac.init(secretKeySpec);
            
            final byte[] data = stringToSign.getBytes( UTF8_CHARSET );
            final byte[] rawHmac = mac.doFinal( data );
            return Base64.encodeBase64String( rawHmac );
        } 
        catch ( final Exception ex ) 
        {
            throw new RuntimeException( "Failed to sign string.", ex );
        }
    }
    
    
    private String getStringToSign()
    {
        String checksumSignature = null;
        String contentType = null;
        String date = null;
        boolean useAmzDateHeaderOverride = false;
        for ( final String headerName : m_request.getHeaders().keySet() )
        {
            for ( final ChecksumType checksumType : ChecksumType.values() )
            {
                if ( headerName.equalsIgnoreCase( checksumType.getHttpHeaderName() ) )
                {
                    if ( null != checksumSignature )
                    {
                        throw new FailureTypeObservableException(
                            AWSFailure.MULTIPLE_CHECKSUM_HEADERS,
                            "Only one checksum type header may be specified." );
                    }
                    checksumSignature = m_request.getHeader( headerName );
                }
            }
            if ( headerName.equalsIgnoreCase(
                    S3HeaderType.CONTENT_TYPE.getHttpHeaderName() ) )
            {
                contentType = m_request.getHeader( headerName );
            }
            if ( headerName.equalsIgnoreCase(
                    S3HeaderType.DATE.getHttpHeaderName() ) )
            {
                if ( !useAmzDateHeaderOverride )
                {
                    date = m_request.getHeader( headerName );
                }
            }
            if ( headerName.equalsIgnoreCase( 
                    S3HeaderType.AMAZON_DATE.getHttpHeaderName() ) )
            {
                useAmzDateHeaderOverride = true;
            }
        }
        
        final StringBuilder retval = new StringBuilder();
        
        // Part 1
        retval.append( m_request.getType() + Platform.SLASH_N );
        
        // Part 2
        if ( null != checksumSignature )
        {
            retval.append( checksumSignature );
        }
        retval.append( Platform.SLASH_N );
        
        // Part 3
        if ( null != contentType )
        {
            retval.append( contentType );
        }
        retval.append( Platform.SLASH_N );
        
        // Part 4
        boolean dateValidationRequired = false;
        if ( !useAmzDateHeaderOverride )
        {
            if ( null != date )
            {
                retval.append( date );
                dateValidationRequired = true;
            }
            else
            {
                throw new IllegalArgumentException(
                        "Security vulnerability detected in client.  "
                        + "Client is required to send the date HTTP header.  " 
                        + "Not doing so would allow authorization signatures to be re-used over time." );
            }
        }
        retval.append( Platform.SLASH_N );
        
        // Part 5
        retval.append( getCanonicalizedAmzHeaders() );
        
        // Part 6
        retval.append( getCanonicalizedResource() );
        
        // Final validation and return
        if ( dateValidationRequired )
        {
            validateDate( date );
        }
        return retval.toString();
    }
    
    
    private void validateDate( final String date )
    {
        final Date javaDate = DateMarshaler.unmarshal( date );
        if ( null == javaDate )
        {
            throw new IllegalArgumentException( "Failed to parse date: " + date );
        }
        
        final long maxSkewInMillis = MAXIMUM_TIME_SKEW_PERMITTED_IN_SECONDS * 1000;
        final long additionalSkewIntoPast = 
                ( null == m_customAuthTimeout ) ? 0 : m_customAuthTimeout.intValue() * 1000L;
        if ( javaDate.getTime() > new Date().getTime() + maxSkewInMillis
                || javaDate.getTime() < new Date().getTime() - maxSkewInMillis
                   - additionalSkewIntoPast  )
        {
            throw new FailureTypeObservableException(
                    AWSFailure.REQUEST_TIME_TOO_SKEWED,
                    "Client clock is not synchronized with server clock, " 
                    + "or time of request signing is too far away from when request was sent.  " 
                    + "Client said the time was " 
                    + javaDate + ", but server has a time of " + new Date() 
                    + ".  " + ( ( 0 == additionalSkewIntoPast ) ?
                            "The maximum time skew allowed was " + MAXIMUM_TIME_SKEW_PERMITTED_IN_SECONDS 
                            + " seconds."
                            : "This request was granted an additional grace of " 
                              + additionalSkewIntoPast / 1000 + " secs (for a total of " 
                              + ( MAXIMUM_TIME_SKEW_PERMITTED_IN_SECONDS + m_customAuthTimeout.intValue() )
                              + " secs) to process its request." ) );
        }
    }
    
    
    private String getCanonicalizedResource()
    {
        final StringBuilder sb = new StringBuilder();
        sb.append( getRawPathInfo() );

        final TreeMap< String, String > requestParameters = new TreeMap<>();
        for ( final String key : m_request.getQueryParams().keySet() )
        {
            requestParameters.put( key, m_request.getQueryParam( key ) );
        }
        
        int index = -1;
        for ( final Map.Entry< String, String > e : requestParameters.entrySet() )
        {
            if ( !INCLUDED_QUERY_PARAMETERS.contains( e.getKey() ) )
            {
                continue;
            }
            
            ++index;
            if ( 0 < index )
            {
                sb.append( "&" );
            }
            else
            {
                sb.append( "?" );
            }
            
            sb.append( percentEncodeRfc3986( e.getKey() ) );
            if ( null != e.getValue() && e.getValue().length() > 0 )
            {
                sb.append( "=" + percentEncodeRfc3986( e.getValue() ) );
            }
        }
        
        return sb.toString();
    }
    
    
    private String getRawPathInfo()
    {
        String retval = m_request.getOriginalClientRequestUrl();
        final int slashSlashIndex = retval.indexOf( "//" );
        if ( 0 <= slashSlashIndex )
        {
            retval = retval.substring( slashSlashIndex + 2 );
        }
        
        final int slashIndex = retval.indexOf( '/' );
        if ( 0 <= slashIndex )
        {
            return retval.substring( slashIndex );
        }
        return "";
    }
    
    
    private String getCanonicalizedAmzHeaders()
    {
        final StringBuilder sb = new StringBuilder();
        
        final SortedMap< String, String > amazonHeaders = new TreeMap<>();
        for ( final String originalHeaderName : m_request.getHeaders().keySet() )
        {
            final String headerName = originalHeaderName.toLowerCase();
            if ( !headerName.startsWith( "x-amz" ) )
            {
                continue;
            }
            
            final String value = m_request.getHeader( originalHeaderName );
            if ( S3HeaderType.AUTH_DATE_GRACE.getHttpHeaderName().toLowerCase().equals( headerName ) )
            {
                m_customAuthTimeout = Integer.valueOf( Integer.parseInt( value ) );
            }
            
            if ( amazonHeaders.containsKey( headerName ) )
            {
                amazonHeaders.put(
                        headerName.toLowerCase(),
                        amazonHeaders.get( headerName ) + "," + value );
            }
            else
            {
                amazonHeaders.put( headerName, value );
            }
        }
        
        for ( final Map.Entry< String, String > e : amazonHeaders.entrySet() )
        {
            // TODO need to "unfold" long headers that span multiple lines by replacing the folding whitespace
            // (including newline) by a single space
            sb.append( e.getKey() + ":" + e.getValue() + Platform.SLASH_N );
        }
        
        return sb.toString();
    }

    
    private String percentEncodeRfc3986( final String s) 
    {
        try 
        {
            return URLEncoder.encode( s, UTF8_CHARSET )
                    .replace( "+", "%20" )
                    .replace( "*", "%2A" )
                    .replace( "%7E", "~" );
        } 
        catch ( final UnsupportedEncodingException ex ) 
        {
            throw new RuntimeException( "Failed to encode: " + s, ex );
        }
    }
    
    
    private Integer m_customAuthTimeout;
    private final HttpRequest m_request;
    private final String m_secretKey;
    
    private final static String UTF8_CHARSET = "UTF8";
    private static final String HMAC_ALGORITHM = "HmacSHA1";
    private final static Logger LOG = Logger.getLogger( V2AuthorizationSignatureValidator.class );
    
    private final static int MAXIMUM_TIME_SKEW_PERMITTED_IN_SECONDS = 60 * 15;
    private final static Set< String > INCLUDED_QUERY_PARAMETERS;
    static
    {
        INCLUDED_QUERY_PARAMETERS = new HashSet<>();
        INCLUDED_QUERY_PARAMETERS.add( "acl" );
        INCLUDED_QUERY_PARAMETERS.add( "lifecycle" );
        INCLUDED_QUERY_PARAMETERS.add( "location" );
        INCLUDED_QUERY_PARAMETERS.add( "logging" );
        INCLUDED_QUERY_PARAMETERS.add( "notification" );
        INCLUDED_QUERY_PARAMETERS.add( "partNumber" );
        INCLUDED_QUERY_PARAMETERS.add( "requestPayment" );
        INCLUDED_QUERY_PARAMETERS.add( "torrent" );
        INCLUDED_QUERY_PARAMETERS.add( "uploadId" );
        INCLUDED_QUERY_PARAMETERS.add( "uploads" );
        INCLUDED_QUERY_PARAMETERS.add( "versionId" );
        INCLUDED_QUERY_PARAMETERS.add( "versioning" );
        INCLUDED_QUERY_PARAMETERS.add( "versions" );
        INCLUDED_QUERY_PARAMETERS.add( "website" );
        INCLUDED_QUERY_PARAMETERS.add( "delete" );
        INCLUDED_QUERY_PARAMETERS.add( "response-content-type" );
        INCLUDED_QUERY_PARAMETERS.add( "response-content-language" );
        INCLUDED_QUERY_PARAMETERS.add( "response-expires" );
        INCLUDED_QUERY_PARAMETERS.add( "response-cache-control" );
        INCLUDED_QUERY_PARAMETERS.add( "response-content-disposition" );
        INCLUDED_QUERY_PARAMETERS.add( "response-content-encoding" );
    }
}
