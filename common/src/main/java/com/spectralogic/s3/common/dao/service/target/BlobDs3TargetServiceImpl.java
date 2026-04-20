/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.target;

import java.util.Set;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.DegradedBlob;
import com.spectralogic.s3.common.dao.domain.ds3.Ds3DataReplicationRule;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomainMember;
import com.spectralogic.s3.common.dao.domain.pool.BlobPool;
import com.spectralogic.s3.common.dao.domain.shared.BlobObservable;
import com.spectralogic.s3.common.dao.domain.shared.PersistenceTarget;
import com.spectralogic.s3.common.dao.domain.tape.BlobTape;
import com.spectralogic.s3.common.dao.domain.target.BlobDs3Target;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.db.query.Require;

final class BlobDs3TargetServiceImpl
    extends BaseBlobTargetService< BlobDs3Target, Ds3DataReplicationRule >
    implements BlobDs3TargetService
{
    BlobDs3TargetServiceImpl()
    {
        super( BlobDs3Target.class, Ds3DataReplicationRule.class, DegradedBlob.DS3_REPLICATION_RULE_ID );
    }
    
    
    @Override
    public void migrate( final UUID storageDomainId, final Set< BlobDs3Target > blobTargets )
    {
        verifyInsideTransaction();
        
        final Set< UUID > blobIds = BeanUtils.extractPropertyValues( blobTargets, BlobObservable.BLOB_ID );
        getDataManager().deleteBeans(
                BlobPool.class, 
                Require.all( 
                        Require.beanPropertyEqualsOneOf( BlobObservable.BLOB_ID, blobIds ),
                        Require.exists(
                                BlobPool.POOL_ID,
                                Require.exists(
                                        PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID,
                                        Require.beanPropertyEquals(
                                                StorageDomainMember.STORAGE_DOMAIN_ID, 
                                                storageDomainId ) ) ) ) );
        getDataManager().deleteBeans(
                BlobTape.class, 
                Require.all( 
                        Require.beanPropertyEqualsOneOf( BlobObservable.BLOB_ID, blobIds ),
                        Require.exists(
                                BlobTape.TAPE_ID,
                                Require.exists(
                                        PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID,
                                        Require.beanPropertyEquals(
                                                StorageDomainMember.STORAGE_DOMAIN_ID, 
                                                storageDomainId ) ) ) ) );
        super.create( blobTargets );
    }
}
