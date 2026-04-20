/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.ds3;

import java.util.HashMap;
import java.util.Map;

import com.spectralogic.s3.common.dao.domain.ds3.BucketAcl;
import com.spectralogic.s3.common.dao.domain.ds3.BucketAclPermission;
import com.spectralogic.s3.common.dao.domain.ds3.BuiltInGroup;
import com.spectralogic.s3.common.dao.domain.ds3.DataPolicyAcl;
import com.spectralogic.s3.common.dao.domain.ds3.Group;
import com.spectralogic.s3.common.dao.domain.ds3.GroupMember;
import com.spectralogic.s3.common.dao.domain.ds3.User;
import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.db.domain.service.KeyValueService;
import com.spectralogic.util.db.query.Query;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.BaseService;
import com.spectralogic.util.db.service.api.BeansRetrieverInitializer;

final class GroupServiceImpl extends BaseService< Group > implements GroupService
{
    GroupServiceImpl()
    {
        super( Group.class );
        addInitializer( new GroupServiceInitializer() );
    }
    
    
    private final class GroupServiceInitializer implements BeansRetrieverInitializer
    {
        public void initialize()
        {
            for ( final BuiltInGroup g : BuiltInGroup.values() )
            {
                final Group group = retrieve( NameObservable.NAME, g.getName() );
                m_builtInGroups.put( 
                        g, 
                        ( null != group ) ? group : createGroup( g.getName() ) );
            }
            
            populateEveryoneGroup();
            createDefaultAccess();
        }
    } // end inner class def
    
    
    private void populateEveryoneGroup()
    {
        final Group group = getBuiltInGroup( BuiltInGroup.EVERYONE );
        
        if ( 0 < getDataManager().getCount( GroupMember.class, Require.nothing() ) )
        {
            return;
        }
        
        for ( final User user 
                : getDataManager().getBeans( User.class, Query.where( Require.nothing() ) ).toSet() )
        {
            getDataManager().createBean( BeanFactory.newBean( GroupMember.class )
                    .setGroupId( group.getId() ).setMemberUserId( user.getId() ) );
        }
    }
    
    
    private Group createGroup( final String name )
    {
        final Group retval = BeanFactory.newBean( Group.class );
        retval.setBuiltIn( true ).setName( name );
        create( retval );
        return retval;
    }
    
    
    public Group getBuiltInGroup( final BuiltInGroup group )
    {
        final Group retval = m_builtInGroups.get( group );
        if ( null == retval )
        {
            throw new IllegalStateException( group + " has not been created yet." );
        }
        return retval;
    }
    
    
    private void createDefaultAccess()
    {
        if ( !getServiceManager().getService( KeyValueService.class ).getBoolean( 
                DEFAULT_ACCESS_CREATED_KEY_1, false ) )
        {
            getServiceManager().getService( DataPolicyAclService.class ).create( 
                    BeanFactory.newBean( DataPolicyAcl.class )
                    .setGroupId( getBuiltInGroup( BuiltInGroup.EVERYONE ).getId() ) );
            getServiceManager().getService( BucketAclService.class ).create( 
                    BeanFactory.newBean( BucketAcl.class )
                    .setPermission( BucketAclPermission.OWNER )
                    .setGroupId( getBuiltInGroup( BuiltInGroup.ADMINISTRATORS ).getId() ) );
            getServiceManager().getService( KeyValueService.class ).set(
                    DEFAULT_ACCESS_CREATED_KEY_1, true );
        }
    }
    
    
    private final Map< BuiltInGroup, Group > m_builtInGroups = new HashMap<>();
    private final static String DEFAULT_ACCESS_CREATED_KEY_1 = 
            GroupService.class.getSimpleName() + "-DefaultAccessCreated1";
}
