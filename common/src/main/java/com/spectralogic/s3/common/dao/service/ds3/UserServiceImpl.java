/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.ds3;

import com.spectralogic.s3.common.dao.domain.ds3.BuiltInGroup;
import com.spectralogic.s3.common.dao.domain.ds3.GroupMember;
import com.spectralogic.s3.common.dao.domain.ds3.User;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.db.service.BaseService;

final class UserServiceImpl extends BaseService< User > implements UserService
{
    private UserServiceImpl()
    {
        super( User.class );
    }
    
    
    @Override
    public void create( final User user )
    {
        super.create( user );
        
        getDataManager().createBean( 
                BeanFactory.newBean( GroupMember.class ).setMemberUserId( user.getId() ).setGroupId(
                    getServiceManager().getService( GroupService.class )
                    .getBuiltInGroup( BuiltInGroup.EVERYONE ).getId() ) );
    }
}
