/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.ds3;

import java.util.List;
import java.util.Set;
import java.util.UUID;

import javax.servlet.http.HttpServletResponse;

import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.S3ObjectProperty;
import com.spectralogic.s3.common.dao.domain.shared.KeyValueObservable;
import com.spectralogic.s3.common.platform.aws.S3HeaderType;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.BaseService;
import com.spectralogic.util.exception.DaoException;
import com.spectralogic.util.exception.GenericFailure;

final class S3ObjectPropertyServiceImpl extends BaseService< S3ObjectProperty >
    implements S3ObjectPropertyService
{
    S3ObjectPropertyServiceImpl()
    {
        super( S3ObjectProperty.class );
    }

    
    public void createProperties( final UUID objectId, final List< S3ObjectProperty > properties )
    {
        for ( final S3ObjectProperty property : properties )
        {
            if ( property.getKey().startsWith( KeyValueObservable.SPECTRA_KEY_NAMESPACE ) )
            {
                throw new DaoException(
                        GenericFailure.BAD_REQUEST, 
                        "Key name collides with spectra key namespace: " + property.getKey() );
            }
        }
        
        for ( final S3ObjectProperty property : properties )
        {
            getDataManager().createBean( property.setObjectId( objectId ) );
        }
    }
    
    
    public void populateAllHttpHeaders( final UUID blobId, final HttpServletResponse response )
    {
        final Blob blob = getServiceManager().getRetriever( Blob.class ).attain( blobId );
        if ( null != blob.getChecksumType() )
        {
            if ( ! response.containsHeader( blob.getChecksumType().getHttpHeaderName() ) )
            {
                response.addHeader( blob.getChecksumType().getHttpHeaderName(), blob.getChecksum() );
            }
        }
        populateObjectHttpHeaders( blob.getObjectId(), response );
    }
    
    
    public void populateObjectHttpHeaders( final UUID objectId, final HttpServletResponse response )
    {
        final Set< S3ObjectProperty > objectProperties = 
                retrieveAll( S3ObjectProperty.OBJECT_ID, objectId ).toSet();
        for ( final S3ObjectProperty objectProperty : objectProperties )
        {
            final String quote = ( S3HeaderType.ETAG.getHttpHeaderName().equals( 
                    objectProperty.getKey() ) ) ? "\"" : "";
            response.addHeader( objectProperty.getKey(), quote + objectProperty.getValue() + quote );
        }
    }
    
    
    public void deleteTemporaryCreationDates( final Set< UUID > objectIds )
    {
        getDataManager().deleteBeans( getServicedType(), Require.all( 
                Require.beanPropertyEqualsOneOf( S3ObjectProperty.OBJECT_ID, objectIds ),
                Require.beanPropertyEquals( KeyValueObservable.KEY, KeyValueObservable.CREATION_DATE ) ) );
    }
}
