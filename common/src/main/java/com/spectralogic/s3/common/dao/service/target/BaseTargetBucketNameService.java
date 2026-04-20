/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.target;

import java.lang.reflect.Method;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.target.PublicCloudReplicationTarget;
import com.spectralogic.s3.common.dao.domain.target.PublicCloudTargetBucketName;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.lang.References;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.BaseService;
import com.spectralogic.util.exception.DaoException;
import com.spectralogic.util.exception.GenericFailure;

abstract class BaseTargetBucketNameService
    < T extends PublicCloudTargetBucketName< T > & DatabasePersistable > extends BaseService< T >
{
    protected BaseTargetBucketNameService( final Class< T > clazz )
    {
        super( clazz );
    }
    
    
    public final String attainTargetBucketName( final UUID bucketId, final UUID targetId )
    {
        final T customName = retrieve( Require.all( 
                Require.beanPropertyEquals( PublicCloudTargetBucketName.BUCKET_ID, bucketId ),
                Require.beanPropertyEquals( PublicCloudTargetBucketName.TARGET_ID, targetId ) ) );
        if ( null != customName )
        {
            return customName.getName();
        }
        
        final Bucket bucket = getServiceManager().getRetriever( Bucket.class ).attain( bucketId );
        return generateTargetBucketName( bucket.getName(), targetId );
    }
    
    
    public final String generateTargetBucketName( final String bucketName, final UUID targetId )
    {
        final Method targetReader = 
                BeanUtils.getReader( getServicedType(), PublicCloudTargetBucketName.TARGET_ID );
        final PublicCloudReplicationTarget< ? > target = 
                (PublicCloudReplicationTarget< ? >)
                getServiceManager().getRetriever( targetReader.getAnnotation( References.class ).value() )
                .attain( targetId );

        if ( !bucketName.toLowerCase().matches( "^(?:[a-z0-9\\.]|(-(?!-))){0,61}$" ) )
        {
            throw new DaoException(
                    GenericFailure.CONFLICT,
                    "Bucket name " + bucketName + " is not valid." );
        }
        
        final String targetBucketName = ( target.getCloudBucketPrefix() + bucketName
                + target.getCloudBucketSuffix() ).toLowerCase();
        if ( !targetBucketName.matches( "^[a-z0-9](?:[a-z0-9\\.]|(-(?!-))){0,61}[a-z0-9\\.]$" ) )
        {
            throw new DaoException( 
                    GenericFailure.CONFLICT, 
                    "Bucket name " + bucketName + " is no longer a valid bucket name after adding the prefix \""
                    + target.getCloudBucketPrefix() + "\" and/or suffix \"" + target.getCloudBucketSuffix() +"\"." );
        } 
        return targetBucketName;
    }
}
