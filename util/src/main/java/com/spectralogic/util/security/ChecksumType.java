/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.security;

import com.spectralogic.util.http.HttpHeaderType;
import com.spectralogic.util.http.HttpRequest;

/**
 * Checksum types / hash functions that we support, from least secure to most secure.  <br><br>
 * 
 * Performance numbers below are stated per data stream for the latest SuperMicro architecture we will be
 * shipping on, or in other words, as the performance that can be achieved via a single stream of data, which 
 * will utilize a single logical CPU.  <br><br>
 * 
 * For example, assuming performance is 100MB/sec per data stream on a single 6-core CPU with hyperthreading, 
 * resulting in 12 logical CPUs:
 * <ul>
 * <li> For a single data stream, the throughput per data stream is no more than 100MB/sec and the aggregate
 * throughput is no more than 100MB/sec (1:12 logical CPUs must be dedicated to checksum calculations)
 * <li> For 10 data streams, the throughput per data stream is no more than 100MB/sec and the aggregate
 * throughput is no more than 1000MB/sec (10:12 logical CPUs must be dedicated to checksum calculations)
 * <li> For 100 data streams, the throughput per data stream is no more than 100MB/sec and the aggregate
 * throughput is no more than 1200MB/sec (12:12 logical CPUs must be dedicated to checksum calculations)
 * </ul>
 */
public enum ChecksumType implements HttpHeaderType
{
    /**
     * The CRC32 algorithm described in RFC 1952, using polynomial 0x04C11DB7, which is a non-cryptographic 
     * checksum (32 bits) limited to about 800MB/sec per data stream.
     */
    CRC_32,
    

    /**
     * The CRC32C algorithm described in RFC 3720 section B4, using polynomial 0x1EDC6F41, which is a 
     * non-cryptographic checksum (32 bits) limited to about 300MB/sec per data stream.
     */
    CRC_32C,
    
    
    /**
     * The MD5 cryptographic checksum (128 bits), which is limited to about 400MB/sec per data stream.
     */
    MD5,
    
    
    /**
     * The SHA cryptographic checksum (256 bits), which is limited to about 75MB/sec per data stream.
     */
    SHA_256,
    
    
    /**
     * The SHA cryptographic checksum (512 bits), which is limited to about 100MB/sec per data stream.
     */
    SHA_512,
    ;
    
    
    private ChecksumType()
    {
        m_algorithmName = getAlgorithmName( name() );
        m_httpHeaderName = getHttpHeaderName( name() );
    }
    
    
    private static String getAlgorithmName( final String enumName )
    {
        return enumName.replace( "_", "-" );
    }
    
    
    private static String getHttpHeaderName( final String enumName )
    {
        return "Content-" + enumName.replace( "_", "" );
    }
    
    
    public String getAlgorithmName()
    {
        return m_algorithmName;
    }
    
    
    public String getHttpHeaderName()
    {
        return m_httpHeaderName;
    }
    
    
    public static ChecksumType fromHttpRequest( final HttpRequest request )
    {
        ChecksumType retval = null;
        
        for ( final ChecksumType algorithm : values() )
        {
            if ( null != request.getHeader( algorithm ) )
            {
                retval = algorithm;
            }
        }
        
        return retval;
    }
    
    
    private final String m_algorithmName;
    private final String m_httpHeaderName;
}
