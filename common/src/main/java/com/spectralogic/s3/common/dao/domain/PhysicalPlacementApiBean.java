/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain;

import java.util.ArrayList;
import java.util.List;

import com.spectralogic.s3.common.dao.domain.pool.Pool;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.domain.target.AzureTarget;
import com.spectralogic.s3.common.dao.domain.target.Ds3Target;
import com.spectralogic.s3.common.dao.domain.target.ReplicationTarget;
import com.spectralogic.s3.common.dao.domain.target.S3Target;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.marshal.BaseMarshalable;
import com.spectralogic.util.marshal.CustomMarshaledName;
import com.spectralogic.util.marshal.CustomMarshaledName.CollectionNameRenderingMode;

public final class PhysicalPlacementApiBean extends BaseMarshalable
{
    public final static String TAPES = "tapes";

    @CustomMarshaledName(
            value = "Tape",
            collectionValue = "Tapes",
            collectionValueRenderingMode = CollectionNameRenderingMode.SINGLE_BLOCK_FOR_ALL_ELEMENTS )
    public Tape [] getTapes()
    {
        return CollectionFactory.toArray( Tape.class, m_tapes );
    }
    
    public PhysicalPlacementApiBean setTapes( final Tape [] value )
    {
        m_tapes = ( null == value ) ? new ArrayList< Tape >() : CollectionFactory.toList( value );
        return this;
    }
    
    public List< Tape > tapesList()
    {
        return m_tapes;
    }
    
    
    public final static String POOLS = "pools";

    @CustomMarshaledName(
            value = "Pool",
            collectionValue = "Pools",
            collectionValueRenderingMode = CollectionNameRenderingMode.SINGLE_BLOCK_FOR_ALL_ELEMENTS )
    public Pool [] getPools()
    {
        return CollectionFactory.toArray( Pool.class, m_pools );
    }
    
    public PhysicalPlacementApiBean setPools( final Pool [] value )
    {
        m_pools = ( null == value ) ? new ArrayList< Pool >() : CollectionFactory.toList( value );
        return this;
    }
    
    public List< Pool > poolsList()
    {
        return m_pools;
    }
    
    
    public final static String DS3_TARGETS = "ds3Targets";

    @CustomMarshaledName(
            value = "Ds3Target",
            collectionValue = "Ds3Targets",
            collectionValueRenderingMode = CollectionNameRenderingMode.SINGLE_BLOCK_FOR_ALL_ELEMENTS )
    public Ds3Target [] getDs3Targets()
    {
        return CollectionFactory.toArray( Ds3Target.class, m_ds3Targets );
    }
    
    public PhysicalPlacementApiBean setDs3Targets( final Ds3Target [] value )
    {
        m_ds3Targets = ( null == value ) ? new ArrayList< Ds3Target >() : CollectionFactory.toList( value );
        return this;
    }
    
    
    public final static String AZURE_TARGETS = "azureTargets";

    @CustomMarshaledName(
            value = "AzureTarget",
            collectionValue = "AzureTargets",
            collectionValueRenderingMode = CollectionNameRenderingMode.SINGLE_BLOCK_FOR_ALL_ELEMENTS )
    public AzureTarget [] getAzureTargets()
    {
        return CollectionFactory.toArray( AzureTarget.class, m_azureTargets );
    }
    
    public PhysicalPlacementApiBean setAzureTargets( final AzureTarget [] value )
    {
        m_azureTargets = ( null == value ) ?
                new ArrayList< AzureTarget >() 
                : CollectionFactory.toList( value );
        return this;
    }
    
    
    public final static String S3_TARGETS = "s3Targets";

    @CustomMarshaledName(
            value = "S3Target",
            collectionValue = "S3Targets",
            collectionValueRenderingMode = CollectionNameRenderingMode.SINGLE_BLOCK_FOR_ALL_ELEMENTS )
    public S3Target [] getS3Targets()
    {
        return CollectionFactory.toArray( S3Target.class, m_s3Targets );
    }
    
    public PhysicalPlacementApiBean setS3Targets( final S3Target [] value )
    {
        m_s3Targets = ( null == value ) ?
                new ArrayList< S3Target >() 
                : CollectionFactory.toList( value );
        return this;
    }
    
    
    public < T extends ReplicationTarget< T > & DatabasePersistable > 
    List< T > targetList( final Class< T > targetType )
    {
        final List< ? > retval;
        if ( Ds3Target.class.isAssignableFrom( targetType ) )
        {
            retval = m_ds3Targets;
        }
        else if ( AzureTarget.class.isAssignableFrom( targetType ) )
        {
            retval = m_azureTargets;
        }
        else if ( S3Target.class.isAssignableFrom( targetType ) )
        {
            retval = m_s3Targets;
        }
        else
        {
            throw new UnsupportedOperationException( "No code to support " + targetType + "." );
        }
        
        @SuppressWarnings( "unchecked" )
        final List< T > castedRetval = (List< T >)retval;
        return castedRetval;
    }
    
    
    private volatile List< Tape > m_tapes = new ArrayList<>();
    private volatile List< Pool > m_pools = new ArrayList<>();
    private volatile List< Ds3Target > m_ds3Targets = new ArrayList<>();
    private volatile List< AzureTarget > m_azureTargets = new ArrayList<>();
    private volatile List< S3Target > m_s3Targets = new ArrayList<>();
}
