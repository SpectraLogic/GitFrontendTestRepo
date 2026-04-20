/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.target.azuretarget;

import com.microsoft.azure.storage.CloudStorageAccount;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.target.AzureTarget;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.target.PublicCloudConnectionIntegration_Test;
import com.spectralogic.s3.target.PublicCloudSupport;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.net.URISyntaxException;

import static com.microsoft.azure.storage.CloudStorageAccount.getDevelopmentStorageAccount;

/**
 * This class is mostly tested via {@link PublicCloudConnectionIntegration_Test}
 */
@Tag( "public-cloud-integration" )
public final class AzureConnectionImpl_Test
{
    @Test
    public void testConnectionFailsIfBadCredentials() throws URISyntaxException {
        if ( !PublicCloudSupport.isPublicCloudSupported() )
        {
            return;
        }
        DatabaseSupport dbSupport = DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);
        final AzureTarget target = BeanFactory.newBean( AzureTarget.class );
        target.setAccountName( PublicCloudSupport.AZURE_ACCOUNT_NAME );
        target.setAccountKey( PublicCloudSupport.AZURE_BAD_ACCOUNT_KEY );
        target.setHttps(false);

        TestUtil.assertThrows(
                null,
                AzureSdkFailure.valueOf( PublicCloudSupport.AZURE_403_FAILURE, 403 ),
                new BlastContainer()
                {
                    public void test() throws Throwable
                    {
                        new AzureConnectionImpl( target, dbSupport.getServiceManager() );
                    }
                } );
        CloudStorageAccount storageAccount = getDevelopmentStorageAccount();
        new AzureConnectionImpl( storageAccount, dbSupport.getServiceManager() ).shutdown();
    }
}
