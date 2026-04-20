/*******************************************************************************
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.service.api;

import java.util.Set;
import java.util.UUID;

import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;
import com.spectralogic.util.db.manager.DataManager;
import com.spectralogic.util.db.query.Query;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.query.WhereClause;
import com.spectralogic.util.db.query.Query.Retrievable;
import com.spectralogic.util.lang.iterate.EnhancedIterable;
import com.spectralogic.util.notification.dispatch.NotificationEventDispatcher;

public interface BeansRetriever< T extends SimpleBeanSafeToProxy >
{
    /**
     * To be called by the beans retriever's service manager to initialize the beans retriever before
     * it's used.
     */
    public void setInitParams( 
            final BeansServiceManager bsm,
            final DataManager dataManager,
            final NotificationEventDispatcher notificationEventDispatcher );
    
    
    public void initialize();
    

    /**
     * @return the bean that has a bean property value of <code>someBeanPropertyValue</code>, per the 
     * bean-property-value-to-bean resolution contract stated below.
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
    public T discover( final Object someBeanPropertyValue );
    
    
    /**
     * @return the total number of beans in the database of this type
     */
    public int getCount();
    
    
    /**
     * @return the total number of beans in the database that match the <code>retrievable</code>.
     */
    public int getCount( final Retrievable retrievable );
    
    
    /**
     * @return the total number of beans in the database of this type
     */
    public int getCount( final WhereClause whereClause );


    /**
     * @return whether any matching beans exist
     */
    public default boolean any( final WhereClause whereClause ) {
        final Query.OffsettableRetrievable query = Query.where(whereClause).orderByNone().limit(1);
        return retrieveAll(query).toList().size() > 0;
    }


    public default boolean all( final WhereClause whereClause ) {
        return !any(Require.not(whereClause));
    }
    
    
    /**
     * @return the total number of beans in the database of this type such that the 
     * <code>beanPropertyName</code> equals <code>requiredBeanPropertyValue</code>.
     */
    public int getCount( 
            final String beanPropertyName, 
            final Object requiredBeanPropertyValue );
    
    
    /**
     * @return the minimum <code>propertyName</code> that meets the <code>whereClause</code> criteria.
     */
    public long getMin( final String propertyName, final WhereClause whereClause );
    
    
    /**
     * @return the maximum <code>propertyName</code> that meets the <code>whereClause</code> criteria.
     */
    public long getMax( final String propertyName, final WhereClause whereClause );
    
    
    /**
     * @return the sum of all <code>propertyName</code> that meets the <code>whereClause</code> criteria.
     */
    public long getSum( final String propertyName, final WhereClause whereClause );
    
    
    /**
     * Retrieves the one bean that has id <code>id</code>, or throws an exception if either (i) no bean or 
     * (ii) many beans are found.
     */
    public T attain( final UUID id );
    
    
    /**
     * Retrieves the one bean that has <code>valueForPropertyName</code> for property 
     * <code>propertyName</code>, or throws an exception if either (i) no bean or (ii) many beans are found.
     */
    public T attain(
            final String propertyName, 
            final Object valueForPropertyName );
    
    
    /**
     * Retrieves the one bean that matches <code>whereClause</code>, or throws an exception if either 
     * (i) no bean or (ii) many beans are found.
     */
    public T attain( final WhereClause whereClause );
    

    /**
     * Retrieves the one bean that has id <code>id</code>, or returns null if either (i) no bean or (ii) 
     * many beans are found.
     */
    public T retrieve( final UUID id );
    

    /**
     * Retrieves the one bean that has <code>valueForPropertyName</code> for property 
     * <code>propertyName</code>, or returns null if either (i) no bean or (ii) many beans are found.
     */
    public T retrieve(
            final String propertyName, 
            final Object valueForPropertyName );
    

    /**
     * Retrieves the one bean that matches <code>whereClause</code>, or returns null if either 
     * (i) no bean or (ii) many beans are found.
     */
    public T retrieve( final WhereClause whereClause );
    
    
    /**
     * Retrieves all beans of this type.
     */
    public RetrieveBeansResult< T > retrieveAll();
    
    
    /**
     * Retrieves all beans of this type that match the <code>retrievable</code>.
     */
    public RetrieveBeansResult< T > retrieveAll( final Retrievable retrievable );
    
    /**
     * @return An iterable of beans in the database that match the <code>retrievable</code>.
     */
     public EnhancedIterable< T > retrieveIterable( final Retrievable retrievable );

    /**
     * Retrieves all beans of this type that match the <code>whereClause</code>.
     */
    public RetrieveBeansResult< T > retrieveAll( final WhereClause whereClause );
    
    
    /**
     * Retrieves the beans that have ids matching the <code>ids</code> specified.
     */
    public RetrieveBeansResult< T > retrieveAll( final Set< UUID > ids );

    /**
     * @return the first bean of this type in default sort order, or null if none exist.
     */
    public T firstOrNull();
    
    /**
     * Retrieves all beans of this type such that the <code>beanPropertyName</code> equals 
     * <code>requiredBeanPropertyValue</code>.
     */
    public RetrieveBeansResult< T > retrieveAll( 
            final String beanPropertyName, 
            final Object requiredBeanPropertyValue );
    
    
    /**
     * @return the domain type that this retriever services
     */
    public Class< T > getServicedType();
}
