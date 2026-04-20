/*
 *
 * Copyright C 2018, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.util.bean;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.spectralogic.util.lang.Validations;

final class BeanInfoCacheImpl implements BeanInfoCache
{
    public BeanInfo getBeanInfo( Class< ? > clazz )
    {
        Validations.verifyNotNull( "Class", clazz );
        clazz = BeanFactory.getType( clazz );
        if ( m_beanInfoCache.containsKey( clazz ) )
        {
            return m_beanInfoCache.get( clazz );
        }
        m_beanInfoCache.put( clazz, new BeanInfo( clazz, this ) );
        return getBeanInfo( clazz );
    }
    
    
    private final Map< Class< ? >, BeanInfo > m_beanInfoCache = new ConcurrentHashMap<>();
}
