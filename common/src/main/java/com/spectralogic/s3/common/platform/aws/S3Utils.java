package com.spectralogic.s3.common.platform.aws;

import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;

import com.spectralogic.s3.common.dao.domain.ds3.S3ObjectProperty;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.exception.DaoException;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.http.HttpRequest;
import com.spectralogic.util.lang.Validations;

public class S3Utils 
{
    public static List< S3ObjectProperty > buildObjectPropertiesFromAmzCustomMetadataHeaders(
            final HttpRequest request )
    {
        Validations.verifyNotNull( "Request", request );

        final List< S3ObjectProperty > objectProperties = new ArrayList<>();
        for ( String headerName : request.getHeaders().keySet() )
        {
            final String value = request.getHeader( headerName );
            if ( S3HeaderType.ETAG.getHttpHeaderName().equalsIgnoreCase( headerName ) )
            {
                headerName = S3HeaderType.ETAG.getHttpHeaderName();
            }
            else if ( !S3HeaderType.isCustomMetadata( headerName ) )
            {
                continue;
            }
            
        if ( LTFS_METADATA_LIMIT <= value.getBytes().length  )
        {
        	throw new DaoException( 
                    GenericFailure.CONFLICT, 
                    "Metadata for value " + headerName + " exceeds the limit of "
                    		+ LTFS_METADATA_LIMIT + " bytes.");
        }
            
        /*The reason we set the id and object id to a random id is because the request payload would be
        rejected otherwise when we send it down in an RPC call to the data planner. The data planner will
        set the correct object id when creating the metadata. */
            objectProperties.add( (S3ObjectProperty)BeanFactory.newBean( S3ObjectProperty.class )
                    .setKey( headerName )
                    .setValue( value )
                    .setObjectId( UUID.randomUUID() )
                    .setId( UUID.randomUUID() ) );
        }
        return objectProperties;
    } 
    
    
    /**
     * @param checksums (base-64 encoded)
     * @return ETag
     */
    public static String getObjectETag( final List< String > checksums )
    {
        Validations.verifyNotNull( "Checksums", checksums );
        if ( 1 == checksums.size() )
        {
            return Hex.encodeHexString( Base64.decodeBase64( checksums.get( 0 ) ) );
        }
        
        final MessageDigest messageDigest;
        try
        {
            messageDigest = MessageDigest.getInstance( "MD5" );
        }
        catch ( final Exception ex )
        {
            throw new UnsupportedOperationException( ex );
        }
        
        for ( final String checksum : checksums )
        {
            final byte [] decodedByteArray = Base64.decodeBase64( checksum );
            messageDigest.update( decodedByteArray );
        }
        
        return Hex.encodeHexString( messageDigest.digest() ) + "-" + checksums.size();
    }
    

    private final static int LTFS_METADATA_LIMIT = 4096;
    public final static String REST_REQUEST_REQUIRED_PREFIX = "_REST_";
}
