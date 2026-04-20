/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.ds3;

import java.util.UUID;

import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.db.lang.CascadeDelete;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.lang.References;
import com.spectralogic.util.db.lang.Unique;
import com.spectralogic.util.db.lang.UniqueIndexes;
import com.spectralogic.util.db.lang.CascadeDelete.WhenReferenceIsDeleted;

@UniqueIndexes(
{
    @Unique({ GroupMember.GROUP_ID, GroupMember.MEMBER_GROUP_ID }),
    @Unique({ GroupMember.GROUP_ID, GroupMember.MEMBER_USER_ID })
})
public interface GroupMember extends DatabasePersistable
{
    String GROUP_ID = "groupId";
    
    /**
     * The group that has a member.
     */
    @References( Group.class )
    @CascadeDelete( WhenReferenceIsDeleted.DELETE_THIS_BEAN )
    UUID getGroupId();
    
    GroupMember setGroupId( final UUID value );
    
    
    String MEMBER_USER_ID = "memberUserId";

    /**
     * Either a {@link #MEMBER_GROUP_ID} or {@link #MEMBER_USER_ID} must be specified (and be non-null); the
     * other must be null.
     */
    @Optional
    @References( User.class )
    @CascadeDelete( WhenReferenceIsDeleted.DELETE_THIS_BEAN )
    UUID getMemberUserId();
    
    GroupMember setMemberUserId( final UUID value );
    
    
    String MEMBER_GROUP_ID = "memberGroupId";

    /**
     * Either a {@link #MEMBER_GROUP_ID} or {@link #MEMBER_USER_ID} must be specified (and be non-null); the
     * other must be null.
     */
    @Optional
    @References( Group.class )
    @CascadeDelete( WhenReferenceIsDeleted.DELETE_THIS_BEAN )
    UUID getMemberGroupId();
    
    GroupMember setMemberGroupId( final UUID value );
}
