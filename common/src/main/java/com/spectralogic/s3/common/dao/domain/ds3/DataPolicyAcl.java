/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.ds3;

import java.util.UUID;

import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.db.lang.CascadeDelete;
import com.spectralogic.util.db.lang.CascadeDelete.WhenReferenceIsDeleted;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.lang.References;
import com.spectralogic.util.db.lang.Unique;
import com.spectralogic.util.db.lang.UniqueIndexes;

/**
 * Provides permission to use a data policy to a specified {@link Group} or {@link User}.
 */
@UniqueIndexes(
{
    @Unique({ DataPolicyAcl.DATA_POLICY_ID, DataPolicyAcl.GROUP_ID }),
    @Unique({ DataPolicyAcl.DATA_POLICY_ID, UserIdObservable.USER_ID })
})
public interface DataPolicyAcl extends UserIdObservable< DataPolicyAcl >, DatabasePersistable
{
    String DATA_POLICY_ID = "dataPolicyId";
    
    /**
     * If null, the ACL applies globally across all data policies.  If not null, the ACL applies only to the 
     * data policy specified.
     */
    @Optional
    @References( DataPolicy.class )
    @CascadeDelete( WhenReferenceIsDeleted.DELETE_THIS_BEAN )
    UUID getDataPolicyId();
    
    DataPolicyAcl setDataPolicyId( final UUID value );
    
    
    /**
     * The user that receives the access.  If the user is specified, the group cannot be specified and vice
     * versa.  Either the user or group must be set.
     */
    @Optional
    @References( User.class )
    @CascadeDelete( WhenReferenceIsDeleted.DELETE_THIS_BEAN )
    UUID getUserId();
    
    
    String GROUP_ID = "groupId";

    /**
     * The user that receives the access.  If the user is specified, the group cannot be specified and vice
     * versa.  Either the user or group must be set.
     */
    @Optional
    @References( Group.class )
    @CascadeDelete( WhenReferenceIsDeleted.DELETE_THIS_BEAN )
    UUID getGroupId();
    
    DataPolicyAcl setGroupId( final UUID value );
}
