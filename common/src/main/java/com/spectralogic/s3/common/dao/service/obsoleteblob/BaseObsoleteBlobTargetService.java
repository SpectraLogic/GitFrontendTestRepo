/*******************************************************************************
 *
 * Copyright C 2018, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.obsoleteblob;

import java.util.Set;
import java.util.UUID;

import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.BaseService;

public abstract class BaseObsoleteBlobTargetService< T extends DatabasePersistable > extends BaseService< T >
{
    protected BaseObsoleteBlobTargetService( final Class< T > clazz )
    {
        super( clazz );
    }


    @Override
	public void delete( final Set< UUID > ids )
    {
        verifyInsideTransaction();
        getDataManager().deleteBeans(
                getServicedType(),
                Require.beanPropertyEqualsOneOf( Identifiable.ID, ids ) );
    }


    @Override
    public void create( final T bean )
    {
        super.create( bean );
    }


    @Override
    public void create( final Set< T > beans )
    {
        removeExistentPersistedBeansFromSet( beans );
        super.create( beans );
    }
}
