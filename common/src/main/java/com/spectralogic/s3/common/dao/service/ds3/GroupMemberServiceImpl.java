/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.ds3;

import com.spectralogic.s3.common.dao.domain.ds3.GroupMember;
import com.spectralogic.util.db.service.BaseService;

final class GroupMemberServiceImpl extends BaseService< GroupMember > implements GroupMemberService
{
    GroupMemberServiceImpl()
    {
        super( GroupMember.class );
    }
}
