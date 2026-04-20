/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.ds3;

import com.spectralogic.s3.common.dao.domain.shared.EnumWithNonUserSpecifiableConstants;

import java.util.Set;

public enum BlobStoreTaskPriority implements EnumWithNonUserSpecifiableConstants
{
    CRITICAL( false ),
    URGENT( true ),
    HIGH( true ),
    NORMAL( true ),
    LOW( true ),
    BACKGROUND( false ),
    ;
    
    
    private BlobStoreTaskPriority( final boolean priorityMayBeSpecifiedByUser )
    {
        m_priorityMayBeSpecifiedByUser = priorityMayBeSpecifiedByUser;
    }
    
    
    public boolean isSpecifiableByUser()
    {
        return m_priorityMayBeSpecifiedByUser;
    }
    
    
    public boolean isHigherPriorityThan( final BlobStoreTaskPriority p )
    {
    	return this.ordinal() < p.ordinal(); 
    }

    public boolean isHigherOrEqualPriorityTo( final BlobStoreTaskPriority p )
    {
        return this.ordinal() <= p.ordinal();
    }
    
    public boolean isLowerPriorityThan( final BlobStoreTaskPriority p )
    {
    	return this.ordinal() > p.ordinal();
    }

    public boolean isLowerOrEqualPriorityTo( final BlobStoreTaskPriority p )
    {
        return this.ordinal() >= p.ordinal();
    }

    public boolean isUnconditionallyHigherPriorityThan( final BlobStoreTaskPriority p ) {
        return this.isHigherPriorityThan(p) && (this == CRITICAL || this == URGENT);
    }

    public static BlobStoreTaskPriority max(final BlobStoreTaskPriority p1, final BlobStoreTaskPriority p2 )
    {
        return p1.isHigherPriorityThan(p2) ? p1 : p2;
    }

    public static Set<BlobStoreTaskPriority> prioritiesOfAtLeast(final BlobStoreTaskPriority p ) {
        return Set.of( values() ).stream().filter( x -> x.isHigherOrEqualPriorityTo(p) ).collect( java.util.stream.Collectors.toSet() );
    }


    public static Set<BlobStoreTaskPriority> prioritiesLessThan(final BlobStoreTaskPriority p ) {
        return Set.of( values() ).stream().filter( x -> x.isLowerPriorityThan(p) ).collect( java.util.stream.Collectors.toSet() );
    }


    public static Set<BlobStoreTaskPriority> prioritiesLessThanAndEqualTo(final BlobStoreTaskPriority p ) {
        return Set.of( values() ).stream().filter( x -> x.isLowerOrEqualPriorityTo(p) ).collect( java.util.stream.Collectors.toSet() );
    }


    public static BlobStoreTaskPriority min(final BlobStoreTaskPriority p1, final BlobStoreTaskPriority p2 )
    {
        return p1.isLowerPriorityThan(p2) ? p1 : p2;
    }


    private final boolean m_priorityMayBeSpecifiedByUser;
}
