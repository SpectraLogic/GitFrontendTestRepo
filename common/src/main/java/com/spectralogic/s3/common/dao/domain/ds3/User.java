/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.ds3;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.util.bean.lang.DefaultIntegerValue;
import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.bean.lang.Secret;
import com.spectralogic.util.db.lang.CascadeDelete;
import com.spectralogic.util.db.lang.CascadeDelete.WhenReferenceIsDeleted;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.lang.References;
import com.spectralogic.util.db.lang.Unique;
import com.spectralogic.util.db.lang.UniqueIndexes;

@UniqueIndexes(
{
    @Unique( User.AUTH_ID ),
    @Unique( NameObservable.NAME )
})
public interface User extends DatabasePersistable, NameObservable< User >
{
    String AUTH_ID = "authId";
    
    String getAuthId();
    
    User setAuthId( final String value );
    
    
    String SECRET_KEY = "secretKey";
    
    @Secret
    String getSecretKey();
    
    User setSecretKey( final String value );
    
    
    String DEFAULT_DATA_POLICY_ID = "defaultDataPolicyId";
    
    @Optional
    @References( DataPolicy.class )
    @CascadeDelete( WhenReferenceIsDeleted.SET_NULL )
    UUID getDefaultDataPolicyId();
    
    User setDefaultDataPolicyId( final UUID value );
    
    String MAX_BUCKETS = "maxBuckets";
    
    @DefaultIntegerValue( 10000 )
    int getMaxBuckets();
    
    User setMaxBuckets( final int value );
}
