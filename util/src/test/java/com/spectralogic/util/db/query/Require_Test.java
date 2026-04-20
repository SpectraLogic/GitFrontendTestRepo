/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.query;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.db.manager.DataManager;
import com.spectralogic.util.db.mockdomain.School;
import com.spectralogic.util.db.mockdomain.SchoolType;
import com.spectralogic.util.db.mockdomain.Teacher;
import com.spectralogic.util.db.mockdomain.TeacherType;
import com.spectralogic.util.db.mockservice.CountyService;
import com.spectralogic.util.db.service.api.RetrieveBeansResult;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;

public final class Require_Test 
{
    @Test
    public void testRequireAnyWhereClauseCollectionReturnsCorrectly()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( Teacher.class, CountyService.class );
        final DataManager dataManager = dbSupport.getDataManager();
        
        final School s1 = BeanFactory.newBean( School.class )
                .setName( "Alpha" ).setType( SchoolType.CHARTER );
        final School s2= BeanFactory.newBean( School.class )
                .setName( "Bravo" ).setType( SchoolType.PUBLIC );
        final School s3= BeanFactory.newBean( School.class )
                .setName( "Charlie" ).setType( SchoolType.PRIVATE );
        dataManager.createBean( s1 );
        dataManager.createBean( s2 );
        dataManager.createBean( s3 );

        final List< WhereClause > filters = new ArrayList<>();
        filters.add( Require.beanPropertyEquals(
                School.NAME,
                "Alpha" ) );
        filters.add( Require.beanPropertyEquals(
                School.NAME,
                "Bravo" ) );
        final RetrieveBeansResult< School > result = dbSupport.getServiceManager()
            .getRetriever( School.class ).retrieveAll( Query
                .where( Require.any( filters ) ) );
        assertEquals( result.toList().size(),  2);
    }
    
    
    @Test
    public void testRequireBeanPropertyLessThanReturnsCorrectly()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( Teacher.class, CountyService.class );
        final DataManager dataManager = dbSupport.getDataManager();
        
        final School s1 = BeanFactory.newBean( School.class )
                .setName( "Alpha" ).setType( SchoolType.CHARTER );
        final School s2= BeanFactory.newBean( School.class )
                .setName( "Bravo" ).setType( SchoolType.PUBLIC );
        final School s3= BeanFactory.newBean( School.class )
                .setName( "Charlie" ).setType( SchoolType.PRIVATE );
        dataManager.createBean( s1 );
        dataManager.createBean( s2 );
        dataManager.createBean( s3 );

        final RetrieveBeansResult< School > result = dbSupport.getServiceManager()
            .getRetriever( School.class ).retrieveAll( Query
                .where( Require.beanPropertyLessThan( School.NAME, "Bravo" ) ) );
        assertEquals(result.toIterable().toList().get( 0 ).getName(),  "Alpha");
    }
    
    
    @Test
    public void testRequireBeanPropertyGreaterThanReturnsCorrectly()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( Teacher.class, CountyService.class );
        final DataManager dataManager = dbSupport.getDataManager();
        
        final School s1 = BeanFactory.newBean( School.class )
                .setName( "Alpha" ).setType( SchoolType.CHARTER );
        final School s2= BeanFactory.newBean( School.class )
                .setName( "Bravo" ).setType( SchoolType.PUBLIC );
        final School s3= BeanFactory.newBean( School.class )
                .setName( "Charlie" ).setType( SchoolType.PRIVATE );
        dataManager.createBean( s1 );
        dataManager.createBean( s2 );
        dataManager.createBean( s3 );

        final RetrieveBeansResult< School > result = dbSupport.getServiceManager()
            .getRetriever( School.class ).retrieveAll( Query
                .where( Require.beanPropertyGreaterThan( School.NAME, "Bravo" ) ) );
        assertEquals( result.toIterable().toList().get( 0 ).getName(),  "Charlie");
    }
    
    
    @Test
    public void testRequireBeanPropertySumGreaterThanReturnsCorrectly()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( Teacher.class, CountyService.class );
        final DataManager dataManager = dbSupport.getDataManager();
        
        final Teacher t1 = BeanFactory.newBean( Teacher.class )
                .setName( "Mark" )
                .setType( TeacherType.TEACHER )
                .setDateOfBirth( new Date( 10 ) )
                .setWarningsIssued( 1 )
                .setYearsOfService( 10 );
        final Teacher t2 = BeanFactory.newBean( Teacher.class )
                .setName( "Jason" )
                .setType( TeacherType.TEACHER )
                .setDateOfBirth( new Date( 102 ) )
                .setWarningsIssued( 2 )
                .setYearsOfService( 20 );
        final Teacher t3 = BeanFactory.newBean( Teacher.class )
                .setName( "Barry" )
                .setType( TeacherType.TEACHER )
                .setDateOfBirth( new Date( 103 ) )
                .setWarningsIssued( 3 )
                .setYearsOfService( 20 );
        dataManager.createBean( t1 );
        dataManager.createBean( t2 );
        dataManager.createBean( t3 );

        final Object expected1 = CollectionFactory.toSet( t2.getId(), t3.getId() );
        assertEquals(expected1, BeanUtils.toMap( dbSupport.getServiceManager().getRetriever( Teacher.class ).retrieveAll(
                        Require.beanPropertiesSumGreaterThan(
                                Teacher.WARNINGS_ISSUED,
                                Teacher.YEARS_OF_SERVICE,
                                Integer.valueOf( 15 ) ) ).toSet() ).keySet(), "Shoulda returned correctly.");
        final Object expected = CollectionFactory.toSet( t1.getId(), t2.getId(), t3.getId() );
        assertEquals(expected, BeanUtils.toMap( dbSupport.getServiceManager().getRetriever( Teacher.class ).retrieveAll(
                        Require.beanPropertiesSumGreaterThan(
                                Teacher.WARNINGS_ISSUED,
                                Teacher.YEARS_OF_SERVICE,
                                Integer.valueOf( 5 ) ) ).toSet() ).keySet(), "Shoulda returned correctly.");
    }
    
    
    @Test
    public void testRequireBeanPropertySumLessThanReturnsCorrectly()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( Teacher.class, CountyService.class );
        final DataManager dataManager = dbSupport.getDataManager();
        
        final Teacher t1 = BeanFactory.newBean( Teacher.class )
                .setName( "Mark" )
                .setType( TeacherType.TEACHER )
                .setDateOfBirth( new Date( 10 ) )
                .setWarningsIssued( 4 )
                .setYearsOfService( 10 );
        final Teacher t2 = BeanFactory.newBean( Teacher.class )
                .setName( "Jason" )
                .setType( TeacherType.TEACHER )
                .setDateOfBirth( new Date( 102 ) )
                .setWarningsIssued( 2 )
                .setYearsOfService( 20 );
        final Teacher t3 = BeanFactory.newBean( Teacher.class )
                .setName( "Barry" )
                .setType( TeacherType.TEACHER )
                .setDateOfBirth( new Date( 103 ) )
                .setWarningsIssued( 5 )
                .setYearsOfService( 20 );
        dataManager.createBean( t1 );
        dataManager.createBean( t2 );
        dataManager.createBean( t3 );

        final Object expected1 = CollectionFactory.toSet( t1.getId(), t2.getId() );
        assertEquals(expected1, BeanUtils.toMap( dbSupport.getServiceManager().getRetriever( Teacher.class ).retrieveAll(
                        Require.beanPropertiesSumLessThan(
                                Teacher.WARNINGS_ISSUED,
                                Teacher.YEARS_OF_SERVICE,
                                Integer.valueOf( 25 ) ) ).toSet() ).keySet(), "Shoulda returned correctly.");
        final Object expected = CollectionFactory.toSet( t1.getId(), t2.getId(), t3.getId() );
        assertEquals(expected, BeanUtils.toMap( dbSupport.getServiceManager().getRetriever( Teacher.class ).retrieveAll(
                        Require.beanPropertiesSumLessThan(
                                Teacher.WARNINGS_ISSUED,
                                Teacher.YEARS_OF_SERVICE,
                                Integer.valueOf( 35 ) ) ).toSet() ).keySet(), "Shoulda returned correctly.");
    }
}
