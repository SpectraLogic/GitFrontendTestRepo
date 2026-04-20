/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.target;

import java.util.UUID;

import org.apache.log4j.Logger;

import com.spectralogic.s3.common.dao.domain.target.AzureTarget;
import com.spectralogic.s3.common.dao.domain.target.S3Target;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;

public final class PublicCloudSupport
{
    public static String getTestBucketName()
    {
        return ( getTestBucketPrefix()
                 + UUID.randomUUID().toString().replace( "-", "" ).substring( 0, 20 ) ).toLowerCase();
    }
    

    public static String getTestBucketPrefix()
    {
        return "publiccloudsupport";
    }
    

    public static boolean isPublicCloudSupported()
    {
        synchronized ( LOCK )
        {
            if ( null == s_internetConnectionPresent )
            {
                initializeCloudSupported();
            }
            if ( !s_internetConnectionPresent.booleanValue() )
            {
                LOG.warn( "Public cloud connectivity not supported." );
            }
            else
            {
                LOG.info( "Public cloud connectivity supported." );
            }
            return s_internetConnectionPresent.booleanValue();
        }
    }
    
    
    private static void initializeCloudSupported()
    {
        final String ping = System.getProperty( "os.name" )
                                          .contains( "Windows" ) ?
                                                     "ping 8.8.8.8 -n 1" :
                                                     "ping 8.8.8.8 -n -c 1";
        try
        {
            if ( 0 != Runtime.getRuntime().exec( ping ).waitFor() )
            {
                throw new RuntimeException( "Status code from ping was non-zero." );
            }
            s_internetConnectionPresent = Boolean.TRUE;
        }
        catch ( final Exception ex )
        {
            LOG.warn( "No internet connectivity.", ex );
            s_internetConnectionPresent = Boolean.FALSE;
        }
    }
    
    
    public static AzureTarget createAzureTarget( final MockDaoDriver mockDaoDriver )
    {
        final AzureTarget retval =
                mockDaoDriver.createAzureTarget( "realtarg-by-" + PublicCloudSupport.class.getSimpleName() );
        mockDaoDriver.updateBean( 
                retval.setAccountName( AZURE_ACCOUNT_NAME ).setAccountKey( AZURE_ACCOUNT_KEY ),
                AzureTarget.ACCOUNT_NAME, AzureTarget.ACCOUNT_KEY );
        return retval;
    }
    
    
    public static S3Target createS3Target( final MockDaoDriver mockDaoDriver )
    {
        final S3Target retval = mockDaoDriver
                .createS3TargetToAmazon( "realtarg-by-" + PublicCloudSupport.class.getSimpleName() );
        mockDaoDriver.updateBean( 
                retval.setAccessKey( S3_ACCESS_KEY ).setSecretKey( S3_SECRET_KEY ),
                S3Target.ACCESS_KEY, S3Target.SECRET_KEY );
        return retval;
    }
    
    
    private static Boolean s_internetConnectionPresent;
    
    private final static Object LOCK = new Object();
    private final static Logger LOG = Logger.getLogger( PublicCloudSupport.class );
    public final static String AZURE_403_FAILURE = "ACCESS_DENIED";

    //Azurite credentials for testing
    public final static String AZURE_ACCOUNT_NAME = "devstoreaccount1";//
    public final static String AZURE_ACCOUNT_KEY = 
            "Ss0sk4dZsuH0Cji92F1Ye2kuoEhv+mmYCLfLzGrdw0A1zQagbiBBbnHJNiALudX5nXXZkc4lxT0nFREbg8lpAQ==";
    public final static String AZURE_BAD_ACCOUNT_KEY = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDx0OdJ+ENH9bL7eR1F1lxRrH5xM7XAyvXUOwFfrLNRhQDG9MEUsJh4FTOkw==";

    public final static String S3_403_FAILURE = "SignatureDoesNotMatch";
    public final static String S3_403_AUTHENTICATION = "InvalidAccessKeyId";
    // AWS localstack
    public final static String S3_ACCESS_KEY = "test";
    public final static String S3_SECRET_KEY = "test";
    public final static String S3_ENDPOINT = "s3.localhost.localstack.cloud:4566";

    public final static String S3_BAD_SECRET_KEY = "qJqgFwDCXpGyb4oBUhh52khb6gDhp+nDxK2T4lwE";
}
