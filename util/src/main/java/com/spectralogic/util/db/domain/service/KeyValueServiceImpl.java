/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.domain.service;

import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.db.domain.KeyValue;
import com.spectralogic.util.db.lang.DatabaseUtils;
import com.spectralogic.util.db.service.BaseService;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.Validations;

final class KeyValueServiceImpl extends BaseService< KeyValue > implements KeyValueService
{
    KeyValueServiceImpl()
    {
        super( KeyValue.class );
    }
    
    
    public String getString( final String keyName )
    {
        return attain( keyName ).getStringValue();
    }
    
    
    public boolean getBoolean( final String keyName )
    {
        return attain( keyName ).getBooleanValue().booleanValue();
    }
    
    
    public double getDouble( final String keyName )
    {
        return attain( keyName ).getDoubleValue().doubleValue();
    }
    
    
    public int getInt( final String keyName )
    {
        return attain( keyName ).getLongValue().intValue();
    }
    
    
    public long getLong( final String keyName )
    {
        return attain( keyName ).getLongValue().longValue();
    }
    
    
    public String getString( final String keyName, final String defaultValue )
    {
        final KeyValue retval = retrieve( keyName );
        if ( null == retval )
        {
            return defaultValue;
        }
        return retval.getStringValue();
    }
    
    
    public boolean getBoolean( final String keyName, final boolean defaultValue )
    {
        final KeyValue retval = retrieve( keyName );
        if ( null == retval )
        {
            return defaultValue;
        }
        return retval.getBooleanValue().booleanValue();
    }
    
    
    public double getDouble( final String keyName, final double defaultValue )
    {
        final KeyValue retval = retrieve( keyName );
        if ( null == retval )
        {
            return defaultValue;
        }
        return retval.getDoubleValue().doubleValue();
    }
    
    
    public int getInt( final String keyName, final int defaultValue )
    {
        final KeyValue retval = retrieve( keyName );
        if ( null == retval )
        {
            return defaultValue;
        }
        return retval.getLongValue().intValue();
    }
    
    
    public long getLong( final String keyName, final long defaultValue )
    {
        final KeyValue retval = retrieve( keyName );
        if ( null == retval )
        {
            return defaultValue;
        }
        return retval.getLongValue().longValue();
    }
    
    
    public KeyValue retrieve( final String keyName )
    {
        return super.retrieve( KeyValue.KEY, keyName );
    }
    
    
    public KeyValue attain( final String keyName )
    {
        return super.attain( KeyValue.KEY, keyName );
    }
    
    
    public void set( final String keyName, final long value )
    {
        set( BeanFactory.newBean( KeyValue.class ).setKey( keyName ).setLongValue( 
                Long.valueOf( value ) ) );
    }
    
    
    public void set( final String keyName, final String value )
    {
        Validations.verifyNotNull( "Value", value );
        set( BeanFactory.newBean( KeyValue.class ).setKey( keyName ).setStringValue( 
                value ) );
    }
    
    
    public void set( final String keyName, final boolean value )
    {
        set( BeanFactory.newBean( KeyValue.class ).setKey( keyName ).setBooleanValue(
                Boolean.valueOf( value ) ) );
    }
    
    
    public void set( final String keyName, final double value )
    {
        set( BeanFactory.newBean( KeyValue.class ).setKey( keyName ).setDoubleValue(
                Double.valueOf( value ) ) );
    }
    
    
    synchronized private void set( final KeyValue keyValue )
    {
        Validations.verifyNotNull( "Key", keyValue.getKey() );
        final KeyValue existingKeyValue = retrieve( keyValue.getKey() );
        if ( null == existingKeyValue )
        {
            create( keyValue );
        }
        else
        {
            keyValue.setId( existingKeyValue.getId() );
            update( keyValue, 
                    CollectionFactory.toArray(
                            String.class, 
                            DatabaseUtils.getPersistablePropertyNames( getServicedType() ) ) );
        }
    }
}
