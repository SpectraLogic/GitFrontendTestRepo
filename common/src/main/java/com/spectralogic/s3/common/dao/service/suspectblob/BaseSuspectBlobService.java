/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.suspectblob;

import java.util.Set;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.shared.PersistenceTarget;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.lang.CollectionFactory;

public abstract class BaseSuspectBlobService
    < T extends DatabasePersistable, P extends DatabasePersistable & PersistenceTarget< P > >
    extends BaseSuspectBlobTargetService< T >
{
    protected BaseSuspectBlobService(
            final Class< T > clazz, 
            final Class< P > persistenceTargetType,
            final String blobPersistenceTargetProperty,
            final String ... propertiesOnPersistenceTargetToClear )
    {
        super( clazz );
        m_persistenceTargetType = persistenceTargetType;
        m_blobPersistenceTargetProperty = blobPersistenceTargetProperty;
        m_propertiesOnPersistenceTargetToClear = 
                CollectionFactory.toSet( propertiesOnPersistenceTargetToClear );
    }
    
    
    @Override
    public void delete( final Set< UUID > ids )
    {
        verifyInsideTransaction();
        getDataManager().updateBeans( 
                m_propertiesOnPersistenceTargetToClear,
                BeanFactory.newBean( m_persistenceTargetType ), 
                Require.exists( 
                        getServicedType(),
                        m_blobPersistenceTargetProperty,
                        ( null == ids ) ? 
                                Require.nothing()
                                : Require.beanPropertyEqualsOneOf( Identifiable.ID, ids ) ) );
        super.delete( ids );
    }
    
    
    private final Class< P > m_persistenceTargetType;
    private final String m_blobPersistenceTargetProperty;
    private final Set< String > m_propertiesOnPersistenceTargetToClear;
}
