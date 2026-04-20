/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.testfrmwrk;

import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.*;

import com.spectralogic.util.db.lang.DatabaseView;
import org.apache.log4j.Logger;

import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.db.domain.KeyValue;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.lang.References;
import com.spectralogic.util.db.manager.DataManager;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.exception.FailureTypeObservable;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.thread.wp.SystemWorkPool;

public final class DatabaseResetter implements Runnable
{
    public DatabaseResetter( final DataManager dataManager )
    {
        m_dataManager = dataManager;
        initializeDependencies();
        populateDeleteGroups();
        Validations.verifyNotNull( "Data manager", m_dataManager );
    }

    public void run() {
        for (final Set<Class<?>> groupToDelete: groupsToDelete) {
            final Map<Future<?>, String> futures = new HashMap<>();
            for (final Class<?> type : groupToDelete) {
                futures.put(SystemWorkPool.getInstance().submit( () -> {
                    clearTable((Class<? extends DatabasePersistable>) type);
                }), type.getSimpleName());
            }
            String classBeingReset = null;
            try {
                for (final Future<?> future : futures.keySet()) {
                    classBeingReset = futures.get(future);
                    future.get(5, TimeUnit.MINUTES);
                }
            } catch (final Exception ex) {
                throw new RuntimeException(
                        "Spent too long resetting table: " + classBeingReset, ex);
            }
        }
    }

    private void clearTable(Class<? extends DatabasePersistable> type) {
        final int count = m_dataManager.getCount(type, Require.nothing());
        if (0 == count) return;

        try {
            if (KeyValue.class == type) {
                m_dataManager.deleteBeans(
                        type,
                        Require.not(Require.beanPropertyEquals(KeyValue.KEY, "checksum")));
            } else {
                if (1000 < count) {
                    m_dataManager.truncate(type);
                } else {
                    m_dataManager.deleteBeans(type, Require.nothing());
                }
            }
        } catch (final Exception ex) {
            if (!FailureTypeObservable.class.isAssignableFrom(ex.getClass())
                    || GenericFailure.FORBIDDEN != ((FailureTypeObservable) ex).getFailureType()) {
                throw new RuntimeException(
                        "Failed to reset the database in the correct dependency-order.", ex);
            }
        }
    }

    private void initializeDependencies() {
        final Map< Class< ? >, Set< Class< ? > > > dependencies = new HashMap<>();
        for ( final Class< ? extends DatabasePersistable > type : m_dataManager.getSupportedTypes() )
        {
            m_dependenciesOnType.put( type, new HashSet< Class< ? > >() );
            dependencies.put( type, getDependencies( type ) );
        }
        for ( final Map.Entry< Class< ? >, Set< Class< ? > > > dependency : dependencies.entrySet() )
        {
            for ( final Class< ? > clazz : dependency.getValue() )
            {
                m_dependenciesOnType.get( clazz ).add( dependency.getKey() );
            }
        }
    }

    private static Set< Class< ? > > getDependencies( final Class< ? > type )
    {
        final Set< Class< ? > > retval = new HashSet<>();
        if ( DatabaseView.class.isAssignableFrom( type ) )
        {
            //For now we always delete views first so we will treat them as having no dependencies
            return retval;
        }
        for ( final String prop : BeanUtils.getPropertyNames( type ) )
        {
            final Method reader = BeanUtils.getReader( type, prop );
            final References references = reader.getAnnotation( References.class );
            if ( null != references )
            {
                retval.add( references.value() );
            }
        }
        return retval;
    }

    private void populateDeleteGroups() {
        groupsToDelete.clear();
        final Set<Class < ? >> databaseViews = new HashSet<>();
        final HashMap<Class<?>, Set<Class<?>>> dependenciesOnType = new HashMap<>();
        for (final Map.Entry<Class<?>, Set<Class<?>>> entry : m_dependenciesOnType
                .entrySet()) {
            if (DatabaseView.class.isAssignableFrom(entry.getKey())) {
                databaseViews.add(entry.getKey());
            } else {
                dependenciesOnType.put(entry.getKey(), new HashSet<>(entry.getValue()));
            }
        }
        //delete all database views first since they are never depended on and don't always declare their dependencies
        groupsToDelete.add(databaseViews);
        while (!dependenciesOnType.isEmpty()) {
            final Set<Class < ? >> removed = new HashSet<>();
            for (final Class<? > type : new HashSet<>(dependenciesOnType.keySet())) {
                final Set<Class<?>> dependencies = dependenciesOnType.get(type);
                if (dependencies.isEmpty()) {
                    dependenciesOnType.remove(type);
                    removed.add(type);
                }
            }
            groupsToDelete.add(removed);
            for (final Set<Class<?>> deps : dependenciesOnType.values()) {
                deps.removeAll(removed);
            }
        }
    }
    
    
    private final DataManager m_dataManager;
    private final static Logger LOG = Logger.getLogger( DatabaseResetter.class );
    //for each key class in this map, the value is the set of classes that depend on it
    private final Map< Class< ? >, Set< Class< ? > > > m_dependenciesOnType = new HashMap<>();
    private final List<Set<Class<?>>> groupsToDelete = new ArrayList<>();
}
