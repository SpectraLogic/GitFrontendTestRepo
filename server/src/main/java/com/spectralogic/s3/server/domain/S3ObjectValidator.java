/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.domain;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.DataPersistenceRule;
import com.spectralogic.s3.common.dao.domain.ds3.DataPlacement;
import com.spectralogic.s3.common.dao.domain.ds3.DataPolicy;
import com.spectralogic.s3.common.dao.domain.ds3.LtfsFileNamingMode;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomain;
import com.spectralogic.s3.common.dao.service.ds3.DataPolicyService;
import com.spectralogic.s3.common.platform.domain.LtfsNameUtils;
import com.spectralogic.s3.server.exception.S3RestException;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.lang.Validations;

public final class S3ObjectValidator
{
    private S3ObjectValidator()
    {
        //empty
    }
    
    
    public static void verify( 
            final BeansServiceManager serviceManager, 
            final UUID bucketId, 
            final String objectName )
    {
        Validations.verifyNotNull( "Service manager", serviceManager );
        Validations.verifyNotNull( "Bucket id", bucketId );
        Validations.verifyNotNull( "Object name", objectName );
        verifyBasic( objectName );
        final String error = LtfsNameUtils.getLtfsValidationErrorMessage( objectName );
        if ( null == error )
        {
            return;
        }
        
        final DataPolicy dataPolicy = serviceManager.getRetriever( DataPolicy.class ).attain( Require.exists( 
                Bucket.class,
                Bucket.DATA_POLICY_ID,
                Require.beanPropertyEquals( Identifiable.ID, bucketId ) ) );
        verify( serviceManager, dataPolicy, objectName );
    }
    
    
    public static void verify( 
            final BeansServiceManager serviceManager, 
            final DataPolicy dataPolicy, 
            final String objectName )
    {
        Validations.verifyNotNull( "Service manager", serviceManager );
        Validations.verifyNotNull( "Data policy", dataPolicy );
        Validations.verifyNotNull( "Object name", objectName );
        verifyBasic( objectName );
        final String error = LtfsNameUtils.getLtfsValidationErrorMessage( objectName );     
        
        final DataPolicyService dataPolicyService = serviceManager.getService( DataPolicyService.class );
        if ( error != null && dataPolicyService
                .hasAnyStorageDomainsUsingObjectNamesOnLtfs(dataPolicy.getId() ) )
        {
            if ( 0 < serviceManager.getRetriever( StorageDomain.class ).getCount( Require.all(
                    Require.beanPropertyEquals(
                            StorageDomain.LTFS_FILE_NAMING,
                            LtfsFileNamingMode.OBJECT_NAME ),
                    Require.exists( 
                            DataPersistenceRule.class,
                            DataPersistenceRule.STORAGE_DOMAIN_ID, 
                            Require.beanPropertyEquals(
                                    DataPlacement.DATA_POLICY_ID,
                                    dataPolicy.getId() ) ) ) ) )
            {
                throw new S3RestException(
                        GenericFailure.BAD_REQUEST,
                        "Could not create object '" + objectName +
                        "' because the name was invalid: " + error );
            }
        }
    }
      
    
    private static void verifyBasic( final String objectName )
    {
        final String error = getValidationErrorMessage( objectName );
        if ( null == error )
        {
            return;
        }
        
        throw new S3RestException(
                GenericFailure.BAD_REQUEST,
                "Could not create object '" + objectName + "' because the name was invalid: " + error );
    }
    
    
    private static String getValidationErrorMessage( final String objectName )
    {
        if ( 0 == objectName.length() )
        {
            return "Object name cannot be empty.";
        }
        return null;
    }
}
