/*******************************************************************************
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import com.spectralogic.util.db.lang.DatabaseView;
import org.junit.jupiter.api.Test;
import org.reflections.Reflections;

import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.db.lang.DatabasePersistable;

public class DatabasePersistableSortIndex_Test
{
    @Test
     public void verifyAllBeanSortIndexes() throws Exception
    {
        Reflections reflections = new Reflections( "com.spectralogic.s3.common.dao.domain" );
        Set< Class< ? extends DatabasePersistable > > subTypes = reflections.getSubTypesOf( DatabasePersistable
 .class );
        subTypes.removeIf(t -> DatabaseView.class.isAssignableFrom(t));
        final AtomicInteger failures = new AtomicInteger( 0 );
        final List< String > failureMessages = new ArrayList<>();

        subTypes.stream()
                .map( Class::getName )
                .sorted()
                .forEach( x -> {
                    try
                    {
                        BeanUtils.getColumnIndexes( Class.forName( x ) );
                    }
                    catch ( Exception e )
                    {
                        failures.addAndGet( 1 );
                        failureMessages.add( e.getMessage() );
                        e.printStackTrace();
                    }
                } );

        if ( failures.get() > 0 )
        {
            String stuff = failures.get() + " beans have missing indexes for @SortBy annotations\n";
            for ( String msg : failureMessages )
            {
                stuff += msg + "\n";
            }
            throw new Exception( stuff );
        }
    }
}
