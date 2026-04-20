/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.platform.notification.generator;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import com.spectralogic.s3.common.platform.notification.domain.payload.GenericDaoNotificationPayload;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.db.lang.SqlOperation;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.notification.domain.NotificationPayload;
import com.spectralogic.util.notification.domain.NotificationPayloadGenerator;

public final class GenericDaoNotificationPayloadGenerator implements NotificationPayloadGenerator
{
    public GenericDaoNotificationPayloadGenerator( 
            final SqlOperation sqlOperation,
            final Class< ? > daoType,
            final Set< UUID > ids )
    {
        Validations.verifyNotNull( "Sql operation", sqlOperation );
        Validations.verifyNotNull( "Dao type", daoType );
        Validations.verifyNotEmptyCollection( "Ids", ids );
        
        m_sqlOperation = sqlOperation;
        m_daoType = daoType;
        m_ids = new HashSet<>( ids );
    }

    
    public NotificationPayload generateNotificationPayload()
    {
        final GenericDaoNotificationPayload retval = 
                BeanFactory.newBean( GenericDaoNotificationPayload.class );
        retval.setDaoType( m_daoType.getName() );
        retval.setIds( CollectionFactory.toArray( UUID.class, m_ids ) );
        retval.setSqlOperation( m_sqlOperation );
        return retval;
    }
    

    private final SqlOperation m_sqlOperation;
    private final Class< ? > m_daoType;
    private final Set< UUID > m_ids;
}
