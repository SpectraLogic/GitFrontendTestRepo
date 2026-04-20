/*
 *
 * Copyright C 2018, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.util.db.manager;

import java.io.File;
import java.util.Set;
import java.util.UUID;

import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.query.Query.Retrievable;
import com.spectralogic.util.db.query.WhereClause;
import com.spectralogic.util.lang.iterate.EnhancedIterable;
import com.spectralogic.util.shutdown.Shutdownable;

public interface DataManager extends Shutdownable
{
    /**
     * @return all supported data types by this manager.
     */
    Set< Class< ? extends DatabasePersistable > > getSupportedTypes();
    
    
    /**
     * @return the number of records in the database with the type specified that meet the 
     * <code>retrievable</code> criteria.
     */
    int getCount( final Class< ? extends DatabasePersistable > type, final Retrievable retrievable );
    
    
    /**
     * @return the number of records in the database with the type specified that meet the
     * <code>whereClause</code> criteria.
     */
    int getCount( final Class< ? extends DatabasePersistable > type, final WhereClause whereClause );
    
    
    /**
     * @return the max of <code>aggregatePropertyName</code> that meets the <code>whereClause</code> criteria.
     */
    long getMax( final Class< ? extends DatabasePersistable > type, final String aggregatePropertyName,
            final WhereClause whereClause );
    
    
    /**
     * @return the min of <code>aggregatePropertyName</code> that meets the <code>whereClause</code> criteria.
     */
    long getMin( final Class< ? extends DatabasePersistable > type, final String aggregatePropertyName,
            final WhereClause whereClause );
    
    
    /**
     * @return the sum of all <code>aggregatePropertyName</code> that meets the <code>whereClause</code> 
     * criteria.
     */
    long getSum( final Class< ? extends DatabasePersistable > type, final String aggregatePropertyName,
            final WhereClause whereClause );
    
    
    /**
     * @return all beans of the specified type in the database that match <code>retrievable</code>
     */
    < T extends DatabasePersistable > EnhancedIterable< T > getBeans( final Class< T > type,
            final Retrievable retrievable );
    
    
    /**
     * @return the bean from the database of the type specified that has a bean property value of 
     * <code>someBeanPropertyValue</code>, per the bean-property-value-to-bean resolution contract 
     * stated below.
     * <br><br>
     * Resolution will be performed in 3 stages.  If a single bean is found in the current stage, that bean 
     * will be returned and no future stages will occur.  If multiple beans are found in the current state,
     * an exception will be thrown that there were multiple results.  The bean resolution stages are: <br>
     * <br> 1) Beans whose {@link Identifiable#ID} matches <code>someBeanPropertyValue</code>
     * <br> 2) Beans that have one or more {@link com.spectralogic.util.db.lang.Unique} 
     *         indexes with a single bean property whose value matches <code>someBeanPropertyValue</code>
     * <br> 3) Beans that have one or more bean properties whose value matches 
     *         <code>someBeanPropertyValue</code>
     * 
     * @throws com.spectralogic.util.exception.DaoException if multiple results are found, or if no
     * results are found
     */
    < T extends DatabasePersistable > T discover( final Class< T > type, final Object someBeanPropertyValue );
    
    
    /**
     * Creates <code>bean</code>, auto-generating the {@link Identifiable#ID} property value if necessary.
     * 
     * Note: If you want to create multiple beans, consider using {@link #createBeans} instead.
     */
    < T extends DatabasePersistable > void createBean( final T bean );
    
    
    /**
     * Creates <code>beans</code> using a COPY SQL command, auto-generating {@link Identifiable#ID} as 
     * necessary. <br><br>
     * 
     * Note: It is far more performance to create multiple beans using this method than it is to create those
     * same beans one at a time using {@link #createBean}, even if the individual calls are being made using
     * a transaction.
     */
    < T extends DatabasePersistable > void createBeans( final Set< T > beans );
    

    /**
     * Updates <code>propertiesToUpdate</code> on <code>bean</code>.  
     * If null is specified as <code>propertiesToUpdate</code>, all properties will be updated.
     * No exception or other indication is provided if nothing was updated.
     * 
     * @throws com.spectralogic.util.exception.DaoException if {@link Identifiable#ID} is null
     */
    < T extends DatabasePersistable > void updateBean( final Set< String > propertiesToUpdate, final T bean );
    
    
    /**
     * Updates <code>propertiesToUpdate</code> on all beans that match the <code>whereClause</code>.  
     * No exception or other indication is provided if nothing was updated.
     */
    < T extends DatabasePersistable > void updateBeans( final Set< String > propertiesToUpdate, final T bean,
            final WhereClause whereClause );
    
    
    /**
     * Deletes the bean with the id and type specified, if it exists.
     */
    < T extends DatabasePersistable > void deleteBean( final Class< T > type, final UUID id );
    
    
    /**
     * Deletes all beans that match the <code>whereClause</code>.  
     * No exception or other indication is provided if nothing was deleted.
     */
    < T extends DatabasePersistable > void deleteBeans( final Class< T > type, final WhereClause whereClause );
    
    
    /**
     * Deletes all beans of the type specified, including any records that reference the beans deleted
     * (cascade delete semantics are used).
     */
    < T extends DatabasePersistable > void truncate( final Class< T > type );
    
    
    /**
     * The data source can only be set once and will be tested when set to verify that it is valid
     */
    void setDataSource( final DataSource dataSource );
    
    
    /**
     * If the data source has been set and has been found to be incompatible, an exception will be raised
     * and the data manager will shut down to prevent damage to the data source.  To override this behavior,
     * you can call this method to get an unsafe instance of the data manager that will attempt to work with
     * the incompatible database.
     * <br><br>
     * <font color = red><b>Warning: In unsafe mode, the data manager will attempt to work with an
     * incompatible database, which may result in unexpected behavior. </b></font>
     */
    DataManager toUnsafeModeForIncompatibleDataSource();
    
    
    /**
     * @return DataManager that will not commit anything until {@link #commitTransaction} is called
     */
    DataManager startTransaction();
    
    
    /**
     * Can only be called on data managers that were returned by a {@link #startTransaction} call
     * <br><br>
     * This method can only be called on a transaction that has not yet been committed or cancelled.
     */
    void commitTransaction();
    

    /**
     * Can only be called on data managers that were returned by a {@link #startTransaction} call
     * <br><br>
     * A transaction is either open or closed, and if it's closed, it's either been committed or rolled back.
     * <br><br>
     * It is safe to call this method on a transaction that has already been closed, in which case this method
     * will have no side effects.  If the transaction has not been committed or rolled back, calling this 
     * method will cause a rollback.
     */
    void closeTransaction();
    
    
    /**
     * @return the directory where this database resides
     */
    File getDataDirectory();
    
    
    /**
     * @return the free to total disk space state, that can affect the state of the database
     */
    DatabasePhysicalSpaceState getFreeToTotalDiskSpaceRatioState();


    /**
     * @return the free to total disk space state, that can affect the state of the database
     */
    DatabasePhysicalSpaceState getMaxTableToFreeRatioState();
    
    
    /**
     * @return the overall disk space state, regardless of cause of state
     */
    DatabasePhysicalSpaceState getDatabaseSpaceState();
    
    
    /**
     * Client code that must open SQL connections in a nested manner from within an open SQL connection must
     * use this method to reserve all connections that may be needed in advance, to ensure no live lock is
     * introduced.  In general, such nested designs should be avoided.  <br><br>
     * 
     * @param numberOfAutoCommitConnections     - The number of SQL connections to reserve for this thread on
     *                                            the auto-commit connection pool
     *                                            
     * @param numberOfExplicitCommitConnections - The number of SQL connections to reserve for this thread on
     *                                            the explicit-commit connection pool
     */
    void reserveConnections(
            final int numberOfAutoCommitConnections,
            final int numberOfExplicitCommitConnections );
    
    
    /**
     * Release the reserved SQL connections for this thread
     */
    void releaseReservedConnections();
}
