/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.ds3;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.DataPersistenceRule;
import com.spectralogic.s3.common.dao.domain.ds3.DataPlacement;
import com.spectralogic.s3.common.dao.domain.ds3.DataPolicy;
import com.spectralogic.s3.common.dao.domain.ds3.DataReplicationRule;
import com.spectralogic.s3.common.dao.domain.ds3.DataReplicationRuleType;
import com.spectralogic.s3.common.dao.domain.ds3.Ds3DataReplicationRule;
import com.spectralogic.s3.common.dao.domain.ds3.LtfsFileNamingMode;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomain;
import com.spectralogic.s3.common.platform.domain.LtfsNameUtils;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.BaseService;
import com.spectralogic.util.lang.iterate.EnhancedIterable;

final class DataPolicyServiceImpl extends BaseService< DataPolicy > implements DataPolicyService
{
    DataPolicyServiceImpl()
    {
        super( DataPolicy.class );
    }
    
    
    public boolean isReplicated( final UUID dataPolicyId )
    {
        final int rrCount = getDataManager().getCount( Ds3DataReplicationRule.class, Require.all( 
                Require.beanPropertyEquals( DataPlacement.DATA_POLICY_ID, dataPolicyId ),
                Require.beanPropertyEquals( 
                        DataReplicationRule.TYPE, 
                        DataReplicationRuleType.PERMANENT ) ) );
        return ( 0 < rrCount );
    }
    
    
    public boolean hasAnyStorageDomainsUsingObjectNamesOnLtfs( final UUID dataPolicyId )
    {
        return ( 0 < getServiceManager().getRetriever( StorageDomain.class ).getCount( 
                Require.all( 
                        Require.beanPropertyEquals(
                                StorageDomain.LTFS_FILE_NAMING,
                                LtfsFileNamingMode.OBJECT_NAME ),
                        Require.exists( 
                                DataPersistenceRule.class,
                                DataPersistenceRule.STORAGE_DOMAIN_ID, 
                                Require.beanPropertyEquals(
                                        DataPlacement.DATA_POLICY_ID,
                                        dataPolicyId ) ) ) ) );                
    }
       
    
    public boolean areStorageDomainsWithObjectNamingAllowed( final DataPolicy dp )
    {
        return hasAnyStorageDomainsUsingObjectNamesOnLtfs( dp.getId() ) || 
                ( dp.getVersioning().isLtfsObjectNamingAllowed() &&
                        !hasAnyObjectsWithIllegalLtfsNames( dp.getId() ) );          
    }
    
    
    private boolean hasAnyObjectsWithIllegalLtfsNames( UUID dataPolicyId )
    {
        
        try ( final EnhancedIterable< S3Object > objects =
                getServiceManager().getRetriever( S3Object.class ).retrieveAll( 
                        Require.exists(
                                S3Object.BUCKET_ID, 
                                Require.beanPropertyEquals(
                                        Bucket.DATA_POLICY_ID,
                                        dataPolicyId ) ) ).toIterable() )
        {
            for ( S3Object o : objects )
            {
                if ( LtfsNameUtils.getLtfsValidationErrorMessage( o.getName() ) != null )
                {
                    return true;
                }
            }
        }
        return false;
    }
}
