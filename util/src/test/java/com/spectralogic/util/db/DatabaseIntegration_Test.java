/*******************************************************************************
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db;

import java.io.File;
import java.io.FileWriter;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.spectralogic.util.bean.BeanComparator;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.db.domain.service.KeyValueService;
import com.spectralogic.util.db.domain.service.MutexService;
import com.spectralogic.util.db.manager.DataManager;
import com.spectralogic.util.db.manager.postgres.PostgresDataManager;
import com.spectralogic.util.db.mockdomain.County;
import com.spectralogic.util.db.mockdomain.Principal;
import com.spectralogic.util.db.mockdomain.School;
import com.spectralogic.util.db.mockdomain.SchoolType;
import com.spectralogic.util.db.mockdomain.Teacher;
import com.spectralogic.util.db.mockdomain.TeacherSchool;
import com.spectralogic.util.db.mockdomain.TeacherType;
import com.spectralogic.util.tunables.Tunables;
import com.spectralogic.util.db.mockdomain.TestNotificationRegistration;
import com.spectralogic.util.db.mockservice.CountyService;
import com.spectralogic.util.db.mockservice.PrincipalService;
import com.spectralogic.util.db.mockservice.SchoolService;
import com.spectralogic.util.db.mockservice.TeacherService;
import com.spectralogic.util.db.query.Query;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.BeansServiceManagerImpl;
import com.spectralogic.util.db.service.api.BeansRetriever;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.exception.DaoException;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.iterate.EnhancedIterable;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.mock.NullInvocationHandler;
import com.spectralogic.util.notification.dispatch.NotificationEventDispatcher;
import com.spectralogic.util.predicate.UnaryPredicate;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;


public final class DatabaseIntegration_Test 
{
    @Test
    public void testCreateBeanWhenBeanAlreadyExistsThrowsConflictException()
    {
        final DatabaseSupport dbSuprt = DatabaseSupportFactory.getSupport(
                                           Teacher.class, CountyService.class );
        final DataManager dataManager = dbSuprt.getDataManager();

        dataManager.createBean( BeanFactory.newBean( County.class ).setName( "foo" ) );

        TestUtil.assertThrows(
                "Shoulda thrown a conflict exception.",
                GenericFailure.CONFLICT,
                new BlastContainer()
                {
                    public void test() throws Throwable
                    {
                        dataManager.createBean( BeanFactory.newBean( County.class ).setName( "foo" ) );
                    }
                });
    }
    
    
    @Test
    public void testCreateBeansWhenBeanAlreadyExistsThrowsConflictException()
    {
        final DatabaseSupport dbSuprt = DatabaseSupportFactory.getSupport(
                                           Teacher.class, CountyService.class );
        final DataManager dataManager = dbSuprt.getDataManager();

        dataManager.createBean( BeanFactory.newBean( County.class ).setName( "foo" ) );

        final DataManager transaction = dataManager.startTransaction();
        TestUtil.assertThrows(
                "Shoulda thrown a conflict exception.",
                GenericFailure.CONFLICT,
                new BlastContainer()
                {
                    public void test() throws Throwable
                    {
                        transaction.createBeans( CollectionFactory.toSet(
                                BeanFactory.newBean( County.class ).setName( "foo" ) ) );
                    }
                });
        transaction.closeTransaction();
    }
    
    
    @Test
    public void testTransactionCommitWorks()
    {
        final DatabaseSupport dbSuprt = DatabaseSupportFactory.getSupport(
                                           Teacher.class, CountyService.class );
        final DataManager dataManager = dbSuprt.getDataManager();

        assertEquals(0,  dataManager.getCount(School.class, Require.nothing()), "Should notta been any data initially.");

        final School school = BeanFactory.newBean( School.class );
        school.setName( "Skyline" ).setType( SchoolType.CHARTER );
        
        final DataManager transaction = dataManager.startTransaction();
        transaction.createBean( school );
        
        final Teacher teacher = BeanFactory.newBean( Teacher.class )
                .setName( "Bob" ).setDateOfBirth( new Date() ).setType( TeacherType.TEACHER );
        transaction.createBean( teacher );
        
        final TeacherSchool teacherSchool = BeanFactory.newBean( TeacherSchool.class )
                .setSchoolId( school.getId() ).setTeacherId( teacher.getId() );
        transaction.createBean( teacherSchool );

        assertEquals(1,  transaction.getCount(Teacher.class, Require.nothing()), "Transaction data managers shoulda seen its changes.");
        assertEquals(0,  dataManager.getCount(Teacher.class, Require.nothing()), "Other data managers should notta seen uncommitted changes.");

        transaction.commitTransaction();

        assertEquals(1,  dataManager.getCount(Teacher.class, Require.nothing()), "Committed changes shoulda been seen by everyone.");
        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            public void test()
                {
                    transaction.getCount( Teacher.class, Require.nothing() );
                }
            } );
    }
    
    
    @Test
    public void testCreateBeansWorks()
    {
        final DatabaseSupport dbSuprt = DatabaseSupportFactory.getSupport(
                                           Teacher.class, CountyService.class );
        final DataManager dataManager = dbSuprt.getDataManager();
        
        final DataManager transDM = dataManager.startTransaction();

        assertEquals(0,  transDM.getCount(School.class, Require.nothing()), "Should notta been any data initially.");

        final Set< School > schools = new HashSet<>();
        final int count = 5050;
        for ( int i = 0; i < count; ++i )
        {
            final School school = BeanFactory.newBean( School.class );
            school.setName( "School" + i ).setType( SchoolType.CHARTER );
            schools.add( school );
        }
        
        transDM.createBeans( schools );
        assertEquals(count,  transDM.getCount(School.class, Require.nothing()), "Shoulda created all beans.");
        transDM.commitTransaction();
    }
    
    
    
    @Test
    public void testCreateBeansWorksWithNonAlphabeticalColumns()
    {
        final DatabaseSupport dbSuprt = DatabaseSupportFactory.getSupport(
                                           Teacher.class, CountyService.class );
        final DataManager dataManager = dbSuprt.getDataManager();
        
        final File sqlFile = moveColumnsInCountyTable();
        sqlFile.deleteOnExit();
        dbSuprt.executeSql( sqlFile );
        sqlFile.delete();
        LOG.info( "Altered database to put column name after population" );
        
        final DataManager transDM = dataManager.startTransaction();

        assertEquals(0,  transDM.getCount(School.class, Require.nothing()), "Should not been any data initially.");

        final County county1 = BeanFactory.newBean( County.class );
        county1.setName( "Morris" ).setPopulation( 500000 );
        final County county2 = BeanFactory.newBean( County.class );
        county2.setName( "Sussex" ).setPopulation( 150000 );
        final Set <County> inCounties = new HashSet<>();
        inCounties.add( county1 );
        inCounties.add( county2 );
        transDM.createBeans( inCounties );

        assertEquals(2,  transDM.getCount(County.class, Require.nothing()), "Shoulda created all beans.");
        transDM.commitTransaction();
    }
    
    
    @Test
    public void testTransactionRollbackWorks()
    {
        final DatabaseSupport dbSuprt = DatabaseSupportFactory.getSupport(
                                           Teacher.class, CountyService.class );
        final DataManager dataManager = dbSuprt.getDataManager();

        assertEquals(0,  dataManager.getCount(School.class, Require.nothing()), "Should notta been any data initially.");

        final School school = BeanFactory.newBean( School.class );
        school.setName( "Skyline" ).setType( SchoolType.CHARTER );
        
        final DataManager transDM = dataManager.startTransaction();
        
        dataManager.createBean( school );
        
        final Teacher teacher = BeanFactory.newBean( Teacher.class )
                .setName( "Bob" ).setDateOfBirth( new Date() ).setType( TeacherType.TEACHER );
        transDM.createBean( teacher );

        assertEquals(1,  transDM.getCount(Teacher.class, Require.nothing()), "Transaction data managers shoulda seen its changes.");
        assertEquals(0,  dataManager.getCount(Teacher.class, Require.nothing()), "Other data managers should notta seen uncommitted changes.");

        transDM.closeTransaction();

        assertEquals(0,  dataManager.getCount(Teacher.class, Require.nothing()), "Rolled back changes should notta been seen by anyone.");
    }
    
    
    @Test
    public void testDataManagerCorrectness()
    {
        final DatabaseSupport dbSuprt = DatabaseSupportFactory.getSupport(
                                           Teacher.class, CountyService.class );
        final DataManager dataManager = dbSuprt.getDataManager();

        assertNotNull(
                dataManager.getDataDirectory(),
                "Shoulda determined data directory."
                 );
        assertEquals(0,  dataManager.getCount(School.class, Require.nothing()), "Should notta been any data initially.");

        final School school = BeanFactory.newBean( School.class ).setName( "Skyline" );
        TestUtil.assertThrows( null, DaoException.class, new BlastContainer()
        {
            public void test()
            {
                dataManager.createBean( school );
            }
        } );
        school.setType( SchoolType.PRIVATE );
        
        TestUtil.assertThrows( null, DaoException.class, new BlastContainer()
        {
            public void test()
            {
                assertNull(
                        dataManager.discover( School.class, SchoolType.PRIVATE ),
                        "Should notta been created yet."
                         );
            }
        } );
        dataManager.createBean( school );
        assertNotNull(
                dataManager.discover( School.class, SchoolType.PRIVATE ),
                "Shoulda been created."
                 );

        assertEquals(1,  dataManager.getCount(School.class, Require.nothing()), "Shoulda been single created school.");
        assertEquals("Skyline", dataManager.discover( School.class, SchoolType.PRIVATE ).getName(), "Shoulda found single bean by any of its attributes.");
        assertEquals("Skyline", dataManager.discover( School.class, "Skyline" ).getName(), "Shoulda found single bean by any of its attributes.");
        dataManager.updateBean(
                CollectionFactory.toSet( School.ADDRESS ), 
                dataManager.discover( School.class, "Skyline" ).setAddress( "Skyline" ) );
        assertEquals("Skyline", dataManager.discover( School.class, "Skyline" ).getName(), "Shoulda found single bean by any of its attributes.");
        TestUtil.assertThrows(
                null, 
                DaoException.class, new BlastContainer()
                {
                    public void test()
                    {
                        assertEquals("Skyline", dataManager.discover( School.class, "Who knows" ).getName(), "Shoulda found single bean by any of its attributes.");
                    }
                } );
        
        
        TestUtil.assertThrows( null, DaoException.class, new BlastContainer()
        {
            public void test()
            {
                dataManager.updateBean( null, BeanFactory.newBean( School.class ).setName( "Skyline" ) );
            }
        } );
        
        school.setAddress( "new address" );
        dataManager.updateBean( null, school );
        
        dataManager.createBean( 
                BeanFactory.newBean( County.class ).setName( "somecounty" ).setPopulation( 10 ) );

        assertEquals(1,  dataManager.getCount(School.class, Require.nothing()), "Shoulda been single created school.");
        try ( final EnhancedIterable< School > schoolIterable =
                dataManager.getBeans( School.class, Query.where( Require.nothing() ) ) )
        {
            assertEquals("new address", schoolIterable.iterator().next().getAddress(), "Shoulda set new address.");
        }
        
        dataManager.createBean( 
                BeanFactory.newBean( School.class ).setName( "IF" ).setType( SchoolType.CHARTER ) );
        dataManager.createBean( 
                BeanFactory.newBean( School.class ).setName( "Pillar" ).setType( SchoolType.PUBLIC ) );

        assertEquals(3,  dataManager.getCount(School.class, Require.nothing()), "Shoulda been 3 schools.");
        assertEquals(1,  dataManager.getCount(
                School.class,
                Require.beanPropertyEquals(School.NAME, "IF")), "Shoulda filtered results correctly.");
        assertEquals(0,  dataManager.getCount(
                School.class,
                Require.beanPropertyEquals(School.NAME, "if")), "Shoulda filtered results correctly (and be case sensitive).");
        assertEquals(1,  dataManager.getCount(
                School.class,
                Require.all(Require.beanPropertyEquals(School.NAME, "Pillar"),
                        Require.beanPropertyEquals(School.TYPE, SchoolType.PUBLIC))), "Shoulda filtered results correctly.");
        assertEquals(0,  dataManager.getCount(
                School.class,
                Require.all(Require.beanPropertyEquals(School.NAME, "Pillar"),
                        Require.beanPropertyEquals(School.TYPE, SchoolType.CHARTER))), "Shoulda filtered results correctly.");
        assertEquals(2,  dataManager.getCount(
                School.class,
                Require.any(Require.beanPropertyEquals(School.NAME, "Pillar"),
                        Require.beanPropertyEquals(School.NAME, "IF"))), "Shoulda filtered results correctly.");
        dataManager.createBean(
                BeanFactory.newBean( School.class ).setName( "Oracle" ).setType( SchoolType.PUBLIC ) );
        dataManager.createBean( 
                BeanFactory.newBean( School.class ).setName( "Spectra" ).setType( SchoolType.PUBLIC ) );
        assertEquals(5,  dataManager.getCount(School.class, Require.nothing()), "Shoulda been 5 schools.");
        assertEquals(2,  dataManager.getCount(
                School.class,
                Require.any(
                        Require.beanPropertyEquals(School.NAME, "Skyline"),
                        Require.all(Require.beanPropertyEquals(School.TYPE, SchoolType.PUBLIC),
                                Require.beanPropertyEquals(School.NAME, "Spectra")))), "Shoulda filtered results correctly.");
        dataManager.updateBean(
                null,
                dataManager.discover( School.class, "Skyline" ).setType( SchoolType.CHARTER ) );
        TestUtil.assertThrows(
                null, 
                DaoException.class, new BlastContainer()
                {
                    public void test()
                    {
                        dataManager.deleteBean(
                                School.class,
                                dataManager.discover( School.class, SchoolType.CHARTER ).getId() );
                    }
                } );
        dataManager.deleteBean(
                School.class,
                dataManager.discover( School.class, "Oracle" ).getId() );
        assertEquals(4,  dataManager.getCount(School.class, Require.nothing()), "Shoulda been 4 schools.");
        dataManager.deleteBeans(
                School.class, 
                Require.beanPropertyEquals( School.TYPE, SchoolType.CHARTER ) );
        assertEquals(2,  dataManager.getCount(School.class, Require.nothing()), "Shoulda been 2 schools.");
        dataManager.updateBeans(
                CollectionFactory.toSet( School.NAME ), 
                BeanFactory.newBean( School.class ).setName( "blah" ).setType( SchoolType.CHARTER ), 
                Require.nothing() );
        dataManager.deleteBeans( 
                School.class, 
                Require.beanPropertyEquals( School.TYPE, SchoolType.CHARTER ) );
        assertEquals(2,  dataManager.getCount(School.class, Require.nothing()), "Shoulda been 2 schools.");
        dataManager.updateBeans(
                CollectionFactory.toSet( School.TYPE ), 
                BeanFactory.newBean( School.class ).setName( "blah" ).setType( SchoolType.CHARTER ), 
                Require.nothing() );
        dataManager.deleteBeans( 
                School.class, 
                Require.beanPropertyEquals( School.TYPE, SchoolType.CHARTER ) );
        assertEquals(0,  dataManager.getCount(School.class, Require.nothing()), "Shoulda been no schools.");

        final Date dob = new Date();
        dataManager.createBean( 
                BeanFactory.newBean( Teacher.class )
                .setName( "Jason" )
                .setDateOfBirth( dob )
                .setType( TeacherType.TEACHER ) );
        dataManager.createBean( 
                BeanFactory.newBean( Teacher.class )
                .setName( "Justin" )
                .setDateOfBirth( dob )
                .setType( TeacherType.TEACHER ) );
        TestUtil.assertThrows(
                null,
                DaoException.class, new BlastContainer()
                {
                    public void test()
                    {
                        dataManager.createBean( 
                                BeanFactory.newBean( Teacher.class )
                                .setName( "Justin" )
                                .setDateOfBirth( dob )
                                .setType( TeacherType.TEACHER ) );
                    }
                } );
        TestUtil.assertThrows(
                "SSN format was invalid / did not match required regex.",
                DaoException.class, new BlastContainer()
                {
                    public void test()
                    {
                        dataManager.createBean( 
                                BeanFactory.newBean( Teacher.class )
                                .setName( "Barry" )
                                .setDateOfBirth( dob )
                                .setSsn( "11122-3333" )
                                .setPassword( "something" )
                                .setWarningsIssued( 1 )
                                .setType( TeacherType.TEACHER ) );
                    }
                } );
        final UUID btuid = UUID.randomUUID();
        dataManager.createBean( 
                BeanFactory.newBean( Teacher.class )
                .setName( "Barry" )
                .setDateOfBirth( dob )
                .setSsn( "111-22-3333" )
                .setPassword( "something" )
                .setWarningsIssued( 1 )
                .setType( TeacherType.TEACHER )
                .setId( btuid ) );
        TestUtil.assertThrows(
                "SSN format was invalid / did not match required regex.",
                DaoException.class, new BlastContainer()
                {
                    public void test()
                    {
                        dataManager.updateBean( 
                                CollectionFactory.toSet( Teacher.SSN ),
                                BeanFactory.newBean( Teacher.class )
                                .setName( "Barry" )
                                .setDateOfBirth( dob )
                                .setSsn( "113333" )
                                .setPassword( "something" )
                                .setWarningsIssued( 1 )
                                .setType( TeacherType.TEACHER )
                                .setId( btuid ) );
                    }
                } );
        dataManager.updateBean( 
                CollectionFactory.toSet( Teacher.TYPE ),
                BeanFactory.newBean( Teacher.class )
                .setName( "Barry" )
                .setDateOfBirth( dob )
                .setSsn( "113333" )
                .setPassword( "something" )
                .setWarningsIssued( 1 )
                .setType( TeacherType.TEACHER )
                .setId( btuid ) );
        TestUtil.assertThrows(
                null,
                DaoException.class, new BlastContainer()
                {
                    public void test()
                    {
                        dataManager.createBean( 
                                BeanFactory.newBean( Teacher.class )
                                .setName( "Marina" )
                                .setYearsOfService( 1 )
                                .setDateOfBirth( dob )
                                .setSsn( "111-22-3333" )
                                .setType( TeacherType.TEACHER ) );
                    }
                } );
        dataManager.createBean( 
                BeanFactory.newBean( Teacher.class )
                .setName( "Marina" )
                .setYearsOfService( 4 )
                .setDateOfBirth( dob )
                .setType( TeacherType.TEACHER ) );
        TestUtil.assertThrows(
                null,
                DaoException.class, new BlastContainer()
                {
                    public void test()
                    {
                        dataManager.createBean( 
                                BeanFactory.newBean( Teacher.class )
                                .setName( "Marina" )
                                .setYearsOfService( 5 )
                                .setDateOfBirth( dob )
                                .setType( TeacherType.TEACHER ) );
                    }
                } );
        dataManager.createBean( 
                BeanFactory.newBean( Teacher.class )
                .setName( "Marina" )
                .setDateOfBirth( new Date( 100000 ) )
                .setYearsOfService( 2 )
                .setWarningsIssued( 3 )
                .setType( TeacherType.TEACHER ) );
        assertEquals(0,  dataManager.getMin(
                Teacher.class,
                Teacher.YEARS_OF_SERVICE,
                Require.nothing()), "Shoulda taken aggregate.");
        assertEquals(4,  dataManager.getMax(
                Teacher.class,
                Teacher.YEARS_OF_SERVICE,
                Require.nothing()), "Shoulda taken aggregate.");
        assertEquals(2,  dataManager.getMin(
                Teacher.class,
                Teacher.YEARS_OF_SERVICE,
                Require.beanPropertyEquals(Teacher.NAME, "Marina")), "Shoulda taken aggregate.");
        assertEquals(0,  dataManager.getMin(
                Teacher.class,
                Teacher.WARNINGS_ISSUED,
                Require.beanPropertyEquals(Teacher.NAME, "Marina")), "Shoulda taken aggregate.");
        assertEquals(3,  dataManager.getMax(
                Teacher.class,
                Teacher.WARNINGS_ISSUED,
                Require.beanPropertyEquals(Teacher.NAME, "Marina")), "Shoulda taken aggregate.");
        assertEquals(4,  dataManager.getSum(
                Teacher.class,
                Teacher.WARNINGS_ISSUED,
                Require.nothing()), "Shoulda taken aggregate.");
        assertEquals(0,  dataManager.getSum(
                Teacher.class,
                Teacher.WARNINGS_ISSUED,
                Require.beanPropertyEquals(Identifiable.ID, UUID.randomUUID())), "Shoulda taken aggregate.");
        assertEquals(0,  dataManager.getMin(
                Teacher.class,
                Teacher.YEARS_OF_SERVICE,
                Require.beanPropertyEquals(Teacher.NAME, "Non-existant")), "Shoulda taken aggregate.");
        assertEquals(0,  dataManager.getMax(
                Teacher.class,
                Teacher.WARNINGS_ISSUED,
                Require.beanPropertyEquals(Teacher.NAME, "Non-existant")), "Shoulda taken aggregate.");

        final School school2 = BeanFactory.newBean( School.class ).setAddress( "a" )
                .setName( "Spectra" ).setType( SchoolType.CHARTER );
        dataManager.createBean( school2 );
        final UUID teacherId = dataManager.discover( Teacher.class, "Justin" ).getId();
        final UUID schoolId = school2.getId();
        
        TestUtil.assertThrows( null, DaoException.class, new BlastContainer()
        {
            public void test()
            {
                dataManager.createBean( BeanFactory.newBean( TeacherSchool.class )
                        .setSchoolId( UUID.randomUUID() )
                        .setTeacherId( teacherId ) );
            }
        } );
        TestUtil.assertThrows( null, DaoException.class, new BlastContainer()
        {
            public void test()
            {
                dataManager.createBean( BeanFactory.newBean( TeacherSchool.class )
                        .setSchoolId( schoolId )
                        .setTeacherId( UUID.randomUUID() ) );
            }
        } );
        dataManager.createBean( BeanFactory.newBean( TeacherSchool.class )
                .setSchoolId( schoolId )
                .setTeacherId( teacherId ) );
        school2.setId( UUID.randomUUID() );
        dataManager.updateBeans( 
                CollectionFactory.toSet( Identifiable.ID ), 
                school2, 
                Require.beanPropertyEquals( Identifiable.ID, schoolId ) );
        assertEquals(0,  dataManager.getCount(
                School.class,
                Require.beanPropertyEquals(Identifiable.ID, schoolId)), "Shoulda changed the school's id.");
        assertEquals(1,  dataManager.getCount(
                School.class,
                Require.beanPropertyEquals(Identifiable.ID, school2.getId())), "Shoulda changed the school's id.");
        dataManager.createBean( BeanFactory.newBean( County.class ).setName(
          "alsfjkdsn9p3w84t932842j83vh09428grhvu99327509324759740937593275409284ghcm847h847m5h987m857m2" ) );
        try ( final EnhancedIterable< Teacher > teacherIterable = dataManager.getBeans(
                        Teacher.class,
                        Query.where( Require.beanPropertyEquals( Teacher.NAME, "Marina" ) ) ) )
        {
            dataManager.updateBean( 
                    BeanUtils.getPropertyNames( Teacher.class ), 
                    teacherIterable.iterator().next().setPassword( "blah" ) );
        }
    }
    
    
    @Test
    public void testLongAggregationQueryCorrectness()
    {
        final DatabaseSupport dbSuprt = DatabaseSupportFactory.getSupport(
                                           Teacher.class, CountyService.class );
        final DataManager dataManager = dbSuprt.getDataManager();

        final County c1 = BeanFactory.newBean( County.class )
                .setName( "Boulder" ).setPopulation( Integer.MAX_VALUE + 1L );
        final County c2 = BeanFactory.newBean( County.class )
                .setName( "Weld" ).setPopulation( Integer.MAX_VALUE + 2L );
        final County c3 = BeanFactory.newBean( County.class )
                .setName( "Ada" ).setPopulation( Integer.MAX_VALUE + 3L );
        dataManager.createBean( c1 );
        dataManager.createBean( c2 );
        dataManager.createBean( c3 );

        assertEquals(Integer.MAX_VALUE * 3L + 6,  dataManager.getSum(
                County.class, County.POPULATION, Require.nothing()), "Shoulda summed long type up without downcast to int.");
    }
    
    
    @Test
    public void testWhereClauseCorrectness()
    {
        final DatabaseSupport dbSuprt = DatabaseSupportFactory.getSupport(
                                           Teacher.class, CountyService.class );
        final DataManager dataManager = dbSuprt.getDataManager();

        final County c1 = BeanFactory.newBean( County.class ).setName( "Boulder" ).setPopulation( 11 );
        final County c2 = BeanFactory.newBean( County.class ).setName( "Weld" ).setPopulation( 11 );
        final County c3 = BeanFactory.newBean( County.class ).setName( "Ada" ).setPopulation( 11 );
        dataManager.createBean( c1 );
        dataManager.createBean( c2 );
        dataManager.createBean( c3 );
        
        final School s1 = BeanFactory.newBean( School.class )
                .setCountyId( c1.getId() ).setAddress( "a" ).setName( "Skyline" )
                .setType( SchoolType.values()[ 0 ] );
        final School s2 = BeanFactory.newBean( School.class )
                .setCountyId( c1.getId() ).setAddress( "b" ).setName( "Jefferson" )
                .setType( SchoolType.values()[ 0 ] );
        final School s3 = BeanFactory.newBean( School.class )
                .setCountyId( c2.getId() ).setAddress( "c" ).setName( "George Washington" )
                .setType( SchoolType.values()[ 0 ] );
        dataManager.createBean( s1 );
        dataManager.createBean( s2 );
        dataManager.createBean( s3 );

        assertEquals(2,  dataManager.getBeans(School.class, Query.where(Require.exists(
                School.COUNTY_ID,
                Require.beanPropertyEquals(County.NAME, "Boulder")))).toSet().size(), "Shoulda filtered results correctly.");
        assertEquals(1,  dataManager.getBeans(School.class, Query.where(Require.exists(
                School.COUNTY_ID,
                Require.beanPropertyEquals(County.NAME, "Weld")))).toSet().size(), "Shoulda filtered results correctly.");
        assertEquals(0,  dataManager.getBeans(School.class, Query.where(Require.exists(
                School.COUNTY_ID,
                Require.beanPropertyEquals(County.NAME, "Ada")))).toSet().size(), "Shoulda filtered results correctly.");
        assertEquals(0,  dataManager.getBeans(School.class, Query.where(Require.exists(
                School.COUNTY_ID,
                Require.beanPropertyEquals(County.NAME, "Blah")))).toSet().size(), "Shoulda filtered results correctly.");

        assertEquals(2,  dataManager.getBeans(County.class, Query.where(Require.exists(
                        School.class,
                        School.COUNTY_ID,
                        Require.beanPropertyEquals(School.TYPE, SchoolType.values()[0]))))
                .toSet().size(), "Shoulda filtered results correctly.");
        assertEquals(1,  dataManager.getBeans(County.class, Query.where(Require.exists(
                School.class,
                School.COUNTY_ID,
                Require.beanPropertyEquals(School.NAME, "Skyline")))).toSet().size(), "Shoulda filtered results correctly.");
        assertEquals(0,  dataManager.getBeans(County.class, Query.where(Require.exists(
                School.class,
                School.COUNTY_ID,
                Require.beanPropertyEquals(School.NAME, "Invalid")))).toSet().size(), "Shoulda filtered results correctly.");
    }
    
    
    @Test
    public void testDataServiceCorrectness()
    {
        final DatabaseSupport dbSuprt = DatabaseSupportFactory.getSupport(
                                           Teacher.class, CountyService.class );
        final DataManager dataManager = dbSuprt.getDataManager();

        final BeansServiceManager bsm = BeansServiceManagerImpl.create( 
                dbSuprt.getServiceManager().getNotificationEventDispatcher(),
                dataManager, 
                CollectionFactory.< Class< ? > >toSet( CountyService.class ) );
        assertNotNull(
                bsm.getService( CountyService.class ).retrieveAll().toSet(),
                "Shoulda executed successfull."
                 );
        final School newSchool = BeanFactory.newBean( School.class )
                .setName( "Eagle" ).setType( SchoolType.CHARTER );
        bsm.getService( SchoolService.class ).createSchool( newSchool );
        assertNotNull(
                newSchool.getId(),
                "Bean shoulda contained new id."
                 );
        bsm.getService( SchoolService.class ).updateSchoolAddress(
                newSchool.getId(), "eagleAddress" );
        assertNull(
                newSchool.getAddress(),
                "The way the service was written, couldn't have updated address on instance."
                 );
        final Object expected1 = bsm.getService( SchoolService.class ).attain(
                School.ADDRESS, "eagleAddress" ).getId();
        assertEquals(expected1, newSchool.getId(), "Shoulda found school.");

        final County autoLoadTest1 = BeanFactory.newBean( County.class ).setName( "autoloadtest1" );
        final County autoLoadTest2 = BeanFactory.newBean( County.class ).setName( "autoloadtest2" );
        dataManager.createBean( autoLoadTest2 );
        dataManager.createBean( autoLoadTest1 );

        assertEquals(2,  bsm.getService(CountyService.class).retrieveAll(
                Require.beanPropertyMatches(County.NAME, "autoloadtest%")).toSet().size(), "Shoulda used the search string to filter results correctly.");
        assertEquals(2,  bsm.getService(CountyService.class).retrieveAll(
                        Require.beanPropertyMatchesInsensitive(County.NAME, "AuToLoAdtEsT%"))
                .toSet()
                .size(), "Shoulda used the search string to filter results correctly.");
        assertEquals(2,  bsm.getService(CountyService.class).retrieveAll(
                Require.beanPropertyMatches(County.NAME, "autoloa%test%")).toSet().size(), "Shoulda used the search string to filter results correctly.");
        assertEquals(2,  bsm.getService(CountyService.class).retrieveAll(
                        Require.beanPropertyMatchesInsensitive(County.NAME, "AutOloa%tEst%"))
                .toSet()
                .size(), "Shoulda used the search string to filter results correctly.");
        assertEquals(0,  bsm.getService(CountyService.class).retrieveAll(
                Require.beanPropertyMatches(County.NAME, "autoloadtest")).toSet().size(), "Shoulda used the search string to filter results correctly.");
        assertEquals(0,  bsm.getService(CountyService.class).retrieveAll(
                        Require.beanPropertyMatchesInsensitive(County.NAME, "aUtoLoadTesT"))
                .toSet()
                .size(), "Shoulda used the search string to filter results correctly.");
        assertEquals(1,  bsm.getService(CountyService.class).retrieveAll(
                Require.beanPropertyMatches(County.NAME, "%autoloadtest1%")).toSet().size(), "Shoulda used the search string to filter results correctly.");
        assertEquals(1,  bsm.getService(CountyService.class).retrieveAll(
                        Require.beanPropertyMatchesInsensitive(County.NAME, "%auTOloADtest1%"))
                .toSet()
                .size(), "Shoulda used the search string to filter results correctly.");
        dataManager.updateBean(
                CollectionFactory.toSet( School.COUNTY_ID ),
                newSchool.setCountyId( autoLoadTest2.getId() ) );
        assertEquals(autoLoadTest2.getId(),
                bsm.getRetriever( School.class ).attain(
                        School.ADDRESS, "eagleAddress" ).getCountyId(),
                "Shoulda loaded county id property.");
    }
    
    
    @Test
    public void testCustomBeanPopulatorGetsCalled()
    {
        final Principal principal = BeanFactory.newBean( Principal.class );
        principal.setName( "Charles" );
        principal.setType( TeacherType.TEACHER );
        
        final DatabaseSupport dbSuprt = DatabaseSupportFactory.getSupport(
                                           Teacher.class, CountyService.class );
        final DataManager dataManager = dbSuprt.getDataManager();
        dataManager.createBean( principal );
        
        final Principal retrievedPrincipal = dbSuprt.getServiceManager()
                .getService( PrincipalService.class )
                .retrieve( principal.getId() );
        assertEquals("Prince-Charles-ipal", retrievedPrincipal.getName(), "Shoulda futzed with the name before returning it from the database");
    }
    
    
    @Test
    public void testInClauseWorksWithEnum()
    {

        final DatabaseSupport dbSuprt = DatabaseSupportFactory.getSupport(
                                           Teacher.class, CountyService.class );
        final DataManager dataManager = dbSuprt.getDataManager();
        
        dataManager.createBean( BeanFactory.newBean( School.class )
                .setName( "Oklutnonia, Academy Of The Arcane" )
                .setAddress( "4294967296 Space Road, Milky Way" )
                .setType( SchoolType.CHARTER ) );
        dataManager.createBean( BeanFactory.newBean( School.class )
                .setName( "Slonrerry, Institute Of Wizardy" )
                .setAddress( "742 Evergreen Terrace" )
                .setType( SchoolType.PRIVATE ) );
        dataManager.createBean( BeanFactory.newBean( School.class )
                .setName( "Clounrarths, Academy Of Magics" )
                .setAddress( "804 Rustic Cider Cape, Bear Town, Kansas, 67675-1727" )
                .setType( SchoolType.PUBLIC ) );
        
        final Collection< ? > types = CollectionFactory.toSet( SchoolType.CHARTER, SchoolType.PRIVATE );
        final Set< School > schools = dbSuprt.getServiceManager().getRetriever( School.class )
                .retrieveAll( Require.beanPropertyEqualsOneOf( School.TYPE, types ) )
                .toSet();
        
        final List< String > matchingSchools = new ArrayList<>();
        for ( final School school : schools )
        {
            matchingSchools.add( school.getName() );
        }
        Collections.sort( matchingSchools );
        assertEquals(CollectionFactory.toList(
                "Oklutnonia, Academy Of The Arcane",
                "Slonrerry, Institute Of Wizardy" ),
                matchingSchools,
                "Shoulda returned the schools that had types in the list of values.");
    }
    
    
    @Test
    public void testInClauseWorksWithLong()
    {
        final DatabaseSupport dbSuprt = DatabaseSupportFactory.getSupport(
                                           Teacher.class, CountyService.class );
        final DataManager dataManager = dbSuprt.getDataManager();
        
        dataManager.createBean( BeanFactory.newBean( County.class )
                .setName( "Boulder County" )
                .setPopulation( 170123L ) );
        dataManager.createBean( BeanFactory.newBean( County.class )
                .setName( "Jefferson County" )
                .setPopulation( 70124L ) );
        dataManager.createBean( BeanFactory.newBean( County.class )
                .setName( "Summit County" )
                .setPopulation( 70124L ) );
        
        final Set< Long > populationsToMatch = new HashSet<>();
        for ( int i = 0; i < 100000L; ++i )
        {
            populationsToMatch.add( Long.valueOf( i ) );
        }
        final Set< County > counties = dbSuprt.getServiceManager().getRetriever( County.class )
                .retrieveAll( Require.beanPropertyEqualsOneOf( County.POPULATION, populationsToMatch ) )
                .toSet();
        
        final List< String > matchingCounties = new ArrayList<>();
        for ( final County county : counties )
        {
            matchingCounties.add( county.getName() );
        }
        Collections.sort( matchingCounties );
        assertEquals(CollectionFactory.toList( "Jefferson County", "Summit County" ),
                matchingCounties,
                "Shoulda returned the counties that had populations in the list of values.");
    }
    
    
    @Test
    public void testInClauseWorksWithString()
    {
        final DatabaseSupport dbSuprt = DatabaseSupportFactory.getSupport(
                                           Teacher.class, CountyService.class );
        final DataManager dataManager = dbSuprt.getDataManager();

        dataManager.createBean( BeanFactory.newBean( School.class )
                .setName( "Oklutnonia, Academy Of The Arcane" )
                .setAddress( "4294967296 Space Road, Milky Way" )
                .setType( SchoolType.CHARTER ) );
        dataManager.createBean( BeanFactory.newBean( School.class )
                .setName( "Slonrerry, Institute Of Wizardy" )
                .setAddress( "742 Evergreen Terrace" )
                .setType( SchoolType.PRIVATE ) );
        dataManager.createBean( BeanFactory.newBean( School.class )
                .setName( "Clounrarths, Academy Of Magics" )
                .setAddress( "804 Rustic Cider Cape, Bear Town, Kansas, 67675-1727" )
                .setType( SchoolType.PUBLIC ) );
        
        final Collection< ? > names = CollectionFactory.toSet(
                "Oklutnonia, Academy Of The Arcane",
                "Slonrerry, Institute Of Wizardy" );
        final Set< School > schools = dbSuprt.getServiceManager().getRetriever( School.class )
                .retrieveAll( Require.beanPropertyEqualsOneOf( School.NAME, names ) )
                .toSet();
        
        final List< String > matchingSchools = new ArrayList<>();
        for ( final School school : schools )
        {
            matchingSchools.add( school.getName() );
        }
        Collections.sort( matchingSchools );
        assertEquals(CollectionFactory.toList(
                "Oklutnonia, Academy Of The Arcane",
                "Slonrerry, Institute Of Wizardy" ),
                matchingSchools,
                "Shoulda returned the schools that had types in the list of values.");
    }
    
    
    @Test
    public void testGiganticWhereClauseDoesNotExceedStackMaxDepthDueToUseOfInSqlConstruct()
    {
        final DatabaseSupport dbSuprt = DatabaseSupportFactory.getSupport(
                                           Teacher.class, CountyService.class );
        final DataManager dataManager = dbSuprt.getDataManager();
        
        final String boulderName = "Boulder County";
        final String jeffersonName = "Jefferson County";
        final County county1 = BeanFactory.newBean( County.class )
                .setName( boulderName )
                .setPopulation( 305318L );
        final County county2 = BeanFactory.newBean( County.class )
                .setName( jeffersonName )
                .setPopulation( 405318L );
        final County county3 = BeanFactory.newBean( County.class )
                .setName( "Summit County" )
                .setPopulation( 505318L );
        dataManager.createBean( county1 );
        dataManager.createBean( county2 );
        dataManager.createBean( county3 );
        
        final Set< UUID > idsToRetrieve = new HashSet<>();
        idsToRetrieve.add( county1.getId() );
        for ( int i = 0; i < 50000; ++i )
        {
            idsToRetrieve.add( UUID.randomUUID() );
        }
        idsToRetrieve.add( county3.getId() );
        assertEquals(2,  dbSuprt.getServiceManager().getRetriever(County.class)
                .retrieveAll(idsToRetrieve).toSet().size(), "Shoulda retrieved the 2 counties whose ids were included.");
        assertEquals(0,  dbSuprt.getServiceManager().getRetriever(County.class)
                .retrieveAll(new HashSet<UUID>()).toSet().size(), "Shoulda retrieved nothing.");
        assertEquals(0,  dbSuprt.getServiceManager().getRetriever(County.class)
                .retrieveAll(Require.beanPropertyEqualsOneOf(
                        Identifiable.ID, new HashSet<UUID>())).toSet().size(), "Shoulda retrieved nothing.");

        final int maxClausesForSplit = 10000;
        final Collection< String > candidateNames = new ArrayList<>();
        // Note that we have to make sure the different splits return disjoint result sets.
        // Since we're just repeating the conditions over and over to get the split to happen we really do
        // need to add exactly maxClausesForSplit of the first type.
        for ( int i = 0; i < maxClausesForSplit; ++i )
        {
            candidateNames.add( boulderName + ( ( 0 == i ) ? "" : String.valueOf( i ) ) );
        }
        for ( int i = 0; i < maxClausesForSplit / 2; ++i )
        {
            candidateNames.add( jeffersonName + ( ( 0 == i ) ? "" : String.valueOf( i ) ) );
        }
        final List< County > discoveredCounties = dbSuprt.getServiceManager()
                .getService( CountyService.class )
                .retrieveAll( Require.beanPropertyEqualsOneOf( County.NAME, candidateNames ) )
                .toList();
        Collections.sort( discoveredCounties, new BeanComparator<>( County.class, County.NAME ) );
        assertEquals(2,  discoveredCounties.size(), "Shoulda returned two counties since exactly two should match.");
        assertEquals(boulderName, discoveredCounties.get( 0 ).getName(), "Shoulda returned the first name.");
        assertEquals(jeffersonName, discoveredCounties.get( 1 ).getName(), "Shoulda returned the second name.");
    }
    
    
    @Test
    public void testDataManagerSupportsUnboundedConnectionPools()
    {
        final DatabaseSupport dbSuprt = DatabaseSupportFactory.getSupport(
                                           Teacher.class, CountyService.class );

        final DataManager dataManager = new PostgresDataManager(
                Integer.MAX_VALUE,
                new HashSet< Class< ? > >( dbSuprt.getDataManager().getSupportedTypes() ) );
        dataManager.setDataSource( dbSuprt.getDataSource() );

        final String boulderName = "Boulder County";
        final String jeffersonName = "Jefferson County";
        final long commonPopulation = 305318L;
        final County county1 = BeanFactory.newBean( County.class )
                .setName( boulderName )
                .setPopulation( commonPopulation );
        final County county2 = BeanFactory.newBean( County.class )
                .setName( jeffersonName )
                .setPopulation( commonPopulation );
        
        dataManager.createBean( county1 );
        dataManager.createBean( county2 );

        final CountyService countyService =
                dbSuprt.getServiceManager().getService( CountyService.class );
        
        final County queriedCounty = countyService.discover( county1.getId() );
        
        assertNotNull( queriedCounty,
                "Shoulda returned a county." );
        assertEquals(boulderName, queriedCounty.getName(), "Shoulda returned the name of the first county.");
        assertEquals(commonPopulation,  queriedCounty.getPopulation(), "Shoulda returned the population of the first county.");
        dataManager.shutdown();
    }
    
    
    @Test
    public void testDiscoverFindsRowByPrimaryKey()
    {
        final DatabaseSupport dbSuprt = DatabaseSupportFactory.getSupport(
                                           Teacher.class, CountyService.class );
        final DataManager dataManager = dbSuprt.getDataManager();

        final String boulderName = "Boulder County";
        final String jeffersonName = "Jefferson County";
        final long commonPopulation = 305318L;
        final County county1 = BeanFactory.newBean( County.class )
                .setName( boulderName )
                .setPopulation( commonPopulation );
        final County county2 = BeanFactory.newBean( County.class )
                .setName( jeffersonName )
                .setPopulation( commonPopulation );
        
        dataManager.createBean( county1 );
        dataManager.createBean( county2 );

        final CountyService countyService =
                dbSuprt.getServiceManager().getService( CountyService.class );
        
        final County queriedCounty = countyService.discover( county1.getId() );
        
        assertNotNull( queriedCounty,
                "Shoulda returned a county." );
        assertEquals(boulderName, queriedCounty.getName(), "Shoulda returned the name of the first county.");
        assertEquals(commonPopulation,  queriedCounty.getPopulation(), "Shoulda returned the population of the first county.");
    }
    
    
    @Test
    public void testGetCountReturnsExpectedValuesWhenEmptyOverloadUsed()
    {
        final DatabaseSupport dbSuprt = DatabaseSupportFactory.getSupport(
                                           Teacher.class, CountyService.class );

        final DataManager dataManager = dbSuprt.getDataManager();

        final CountyService countyService =
                dbSuprt.getServiceManager().getService( CountyService.class );

        assertEquals(0,  countyService.getCount(), "Shoulda returned zero because we haven't put anything there yet.");

        dataManager.createBean( BeanFactory.newBean( County.class )
                .setName( "Boulder County" )
                .setPopulation( 305318L ) );
        assertEquals(1,  countyService.getCount(), "Shoulda returned one because we just put a bean in.");

        dataManager.createBean( BeanFactory.newBean( County.class )
                .setName( "Jefferson County" )
                .setPopulation( 305318L ) );
        assertEquals(2,  countyService.getCount(), "Shoulda returned two because we just put another bean in.");
    }
    
    
    @Test
    public void testGetMinGetMaxAndGetSumReturnExpectedPopulationValues()
    {
        final DatabaseSupport dbSuprt = DatabaseSupportFactory.getSupport(
                                           Teacher.class, CountyService.class );
        final DataManager dataManager = dbSuprt.getDataManager();
        
        final long expectedMinPopulation = 305318L;
        final long middlePopulationValue = 405318L;
        final long expectedMaxPopulationValue = 605318L;
        dataManager.createBean( BeanFactory.newBean( County.class )
                .setName( "Boulder County" )
                .setPopulation( expectedMinPopulation ) );
        dataManager.createBean( BeanFactory.newBean( County.class )
                .setName( "Summit County" )
                .setPopulation( middlePopulationValue ) );
        dataManager.createBean( BeanFactory.newBean( County.class )
                .setName( "Jefferson County" )
                .setPopulation( expectedMaxPopulationValue ) );

        final CountyService countyService =
                dbSuprt.getServiceManager().getService( CountyService.class );
        assertEquals(expectedMinPopulation,  countyService.getMin(County.POPULATION, Require.nothing()), "Shoulda returned the expected minimum population.");
        assertEquals(expectedMaxPopulationValue,  countyService.getMax(County.POPULATION, Require.nothing()), "Shoulda returned the expected maximum population.");
        assertEquals(expectedMinPopulation + middlePopulationValue + expectedMaxPopulationValue,  countyService.getSum(County.POPULATION, Require.nothing()), "Shoulda returned the expected maximum population.");
    }
    
    
    @Test
    public void testDiscoverWhenNoRowsExistNotAllowed()
    {
        final DatabaseSupport dbSuprt = DatabaseSupportFactory.getSupport(
                                           Teacher.class, CountyService.class );

        final CountyService countyService =
                dbSuprt.getServiceManager().getService( CountyService.class );
        TestUtil.assertThrows(
                "Shoulda thrown a not found exception from discover when it couldn't find a row.",
                GenericFailure.NOT_FOUND,
                new BlastContainer()
                {
                    public void test() throws Throwable
                    {
                        countyService.discover( UUID.randomUUID() );
                    }
                });
    }
    
    
    @Test
    public void testRetrieveReturnsNullWhenNoRowsExist()
    {
        final DatabaseSupport dbSuprt = DatabaseSupportFactory.getSupport(
                                           Teacher.class, CountyService.class );

        final CountyService countyService =
                dbSuprt.getServiceManager().getService( CountyService.class );
        assertNull(
                countyService.retrieve( UUID.randomUUID() ),
                "Shoulda returned null if there weren't any results."
                 );
    }
    
    
    @Test
    public void testRetrieveAllReturnsMatchingRows()
    {
        final DatabaseSupport dbSuprt = DatabaseSupportFactory.getSupport(
                                           Teacher.class, CountyService.class );
        final DataManager dataManager = dbSuprt.getDataManager();

        final String boulderName = "Boulder County";
        final String jeffersonName = "Jefferson County";
        final long commonPopulation = 305318L;
        final County county1 = BeanFactory.newBean( County.class )
                .setName( boulderName )
                .setPopulation( commonPopulation );
        final County county2 = BeanFactory.newBean( County.class )
                .setName( jeffersonName )
                .setPopulation( commonPopulation );
        
        dataManager.createBean( county1 );
        dataManager.createBean( county2 );

        final CountyService countyService =
                dbSuprt.getServiceManager().getService( CountyService.class );
        
        final List< County > resultsByName =
                countyService.retrieveAll( County.NAME, boulderName ).toList();
        assertEquals(1,  resultsByName.size(), "Shoulda returned exactly one result.");
        assertEquals(boulderName, resultsByName.get( 0 ).getName(), "Shoulda returned the appropriate name.");

        final List< County > resultsByPop =
                countyService.retrieveAll( County.POPULATION, Long.valueOf( commonPopulation ) ).toList();
        Collections.sort( resultsByPop, new BeanComparator<>( County.class, County.NAME ) );
        assertEquals(2,  resultsByPop.size(), "Shoulda returned exactly two results.");
        assertEquals(boulderName, resultsByPop.get( 0 ).getName(), "Shoulda returned the appropriate name for the first bean.");
        assertEquals(jeffersonName, resultsByPop.get( 1 ).getName(), "Shoulda returned the appropriate name for the second bean.");

        final List< County > singleResultById = countyService
                .retrieveAll( CollectionFactory.toSet( county1.getId() ) )
                .toList();
        assertEquals(1,  singleResultById.size(), "Shoulda returned exactly one result.");
        assertEquals(boulderName, singleResultById.get( 0 ).getName(), "Shoulda returned the appropriate name for the first bean.");

        final List< County > resultsById = countyService
                .retrieveAll( CollectionFactory.toSet( county1.getId(), county2.getId() ) )
                .toList();
        Collections.sort( resultsById, new BeanComparator<>( County.class, County.NAME ) );
        assertEquals(2,  resultsById.size(), "Shoulda returned exactly two results.");
        assertEquals(boulderName, resultsById.get( 0 ).getName(), "Shoulda returned the appropriate name for the first bean.");
        assertEquals(jeffersonName, resultsById.get( 1 ).getName(), "Shoulda returned the appropriate name for the second bean.");
    }
    
    
    @Test
    public void testAttainWhenNoRowsExistNotAllowed()
    {
        final DatabaseSupport dbSuprt = DatabaseSupportFactory.getSupport(
                                           Teacher.class, CountyService.class );

        final CountyService countyService =
                dbSuprt.getServiceManager().getService( CountyService.class );
        TestUtil.assertThrows(
                "Shoulda thrown a not found exception from attain when it couldn't find a row.",
                GenericFailure.NOT_FOUND,
                new BlastContainer()
                {
                    public void test() throws Throwable
                    {
                        countyService.attain( UUID.randomUUID() );
                    }
                });
    }
    
    
    @Test
    public void testAttainWhenMultipleRowsMatchNotAllowed()
    {
        final DatabaseSupport dbSuprt = DatabaseSupportFactory.getSupport(
                                           Teacher.class, CountyService.class );
        final DataManager dataManager = dbSuprt.getDataManager();
        
        final long commonPopulation = 305318L;
        final County county1 = BeanFactory.newBean( County.class )
                .setName( "Boulder County" )
                .setPopulation( commonPopulation );
        final County county2 = BeanFactory.newBean( County.class )
                .setName( "Jefferson County" )
                .setPopulation( commonPopulation );
        
        dataManager.createBean( county1 );
        dataManager.createBean( county2 );

        final CountyService countyService =
                dbSuprt.getServiceManager().getService( CountyService.class );
        
        TestUtil.assertThrows(
                "Shoulda thrown an invalid result count exception.",
                GenericFailure.MULTIPLE_RESULTS_FOUND,
                new BlastContainer()
                {
                    public void test() throws Throwable
                    {
                        countyService.attain( County.POPULATION, Long.valueOf( commonPopulation ) );
                    }
                });
    }
    
    
    @Test
    public void testServiceInitializerWorks()
    {
        final DatabaseSupport dbSuprt = DatabaseSupportFactory.getSupport(
                                           Teacher.class, CountyService.class );
        final TeacherService teacherService =
                dbSuprt.getServiceManager().getService( TeacherService.class );
        assertEquals(1,  teacherService.getInitializerCallCount(), "Shoulda called the initializer exactly once.");
    }
    
    
    @Test
    public void testServiceAddInitializerWhenAlreadyInitializedNotAllowed()
    {
        final DatabaseSupport dbSuprt = DatabaseSupportFactory.getSupport(
                                           Teacher.class, CountyService.class );
        final TeacherService teacherService =
                dbSuprt.getServiceManager().getService( TeacherService.class );
        TestUtil.assertThrows(
                "Shoulda thrown an exception because this is an invalid call order.",
                IllegalStateException.class,
                new BlastContainer()
                {
                    public void test() throws Throwable
                    {
                        teacherService.addAnotherInitializer();
                    }
                } );
    }
    
    
    @Test
    public void testPublicServiceInitializationMethodsWhenAlreadyInitializedNotAllowed()
    {
        final DatabaseSupport dbSuprt = DatabaseSupportFactory.getSupport(
                                           Teacher.class, CountyService.class );
        
        final TeacherService teacherService =
                dbSuprt.getServiceManager().getService( TeacherService.class );
        TestUtil.assertThrows(
                "Shoulda thrown an exception because we already initialized.",
                IllegalStateException.class,
                new BlastContainer()
                {
                    public void test() throws Throwable
                    {
                        teacherService.initialize();
                    }
                } );
        TestUtil.assertThrows(
                "Shoulda thrown an exception because we already set initialization parameters.",
                IllegalStateException.class,
                new BlastContainer()
                {
                    public void test() throws Throwable
                    {
                        final InvocationHandler nullIh = NullInvocationHandler.getInstance();
                        teacherService.setInitParams(
                                InterfaceProxyFactory.getProxy( BeansServiceManager.class, nullIh ),
                                InterfaceProxyFactory.getProxy( DataManager.class, nullIh ),
                                InterfaceProxyFactory.getProxy( NotificationEventDispatcher.class, nullIh ) );
                    }
                } );
    }
    
    
    @Test
    public void testGetServicesReturnsCorrectNumberOfClassesWhenFilterProvided()
    {
        final DatabaseSupport dbSuprt = DatabaseSupportFactory.getSupport(
                                           Teacher.class, CountyService.class );
        
        final Set< BeansRetriever< ? > > actualServices =
                dbSuprt.getServiceManager().getServices( new UnaryPredicate< Class<?> >()
                {
                    public boolean test( final Class< ? > clazz )
                    {
                        return !MutexService.class.isAssignableFrom( clazz );
                    }
                } );
        
        final Set< ? > expectedServiceClasses = CollectionFactory.toSet(
                TestNotificationRegistration.class,
                CountyService.class,
                KeyValueService.class,
                MutexService.class,
                PrincipalService.class,
                SchoolService.class,
                TeacherService.class );
        assertEquals(expectedServiceClasses.size() - 1,  actualServices.size(), "Shoulda returned the expected services.");
    }
    
    
    @Test
    public void testTransactionCausesNoEffectsWhenNotCommitted()
    {
        final DatabaseSupport dbSuprt = DatabaseSupportFactory.getSupport(
                                           Teacher.class, CountyService.class );
        
        final BeansServiceManager serviceManager = dbSuprt.getServiceManager();
        final BeansServiceManager transaction = serviceManager.startTransaction();
        try
        {
            transaction.getService( TeacherService.class ).create( buildTeacher() );
        }
        finally
        {
            transaction.closeTransaction();
        }

        final TeacherService teacherService = serviceManager.getService( TeacherService.class );
        assertEquals(0,  teacherService.getCount(), "Should notta committed changes to the database.");
    }
    
    
    @Test
    public void testTransactionCausesEffectsWhenCommitted()
    {
        final DatabaseSupport dbSuprt = DatabaseSupportFactory.getSupport(
                                           Teacher.class, CountyService.class );
        
        final BeansServiceManager serviceManager = dbSuprt.getServiceManager();
        final BeansServiceManager transaction = serviceManager.startTransaction();
        try
        {
            transaction.getService( TeacherService.class ).create( buildTeacher() );
            transaction.commitTransaction();
        }
        finally
        {
            transaction.closeTransaction();
        }

        final TeacherService teacherService = serviceManager.getService( TeacherService.class );
        assertEquals(1,  teacherService.getCount(), "Shoulda committed changes to the database.");
    }
    
    
    private static Teacher buildTeacher()
    {
        return BeanFactory.newBean( Teacher.class )
                .setComments( "tests too easy" )
                .setDateOfBirth( Calendar.getInstance().getTime() )
                .setName( "Dr. Clements" )
                .setPassword( "racketman" )
                .setSsn( "908-78-2345" )
                .setType( TeacherType.TEACHER )
                .setWarningsIssued( 10 )
                .setYearsOfService( 15 );
    }
    
    
    @Test
    public void testCreateBeansSanitizesValues()
    {
        TestUtil.assertJvmEncodingIsUtf8();
        
        final DatabaseSupport dbSuprt = DatabaseSupportFactory.getSupport(
                                           Teacher.class, CountyService.class );
        final DataManager dataManager = dbSuprt.getDataManager();
        
        final Set< School > schools = createSchools(
                null,
                "",
                "\\N",
                "foo|bar",
                "foo\nbar",
                "hello",
                "\\",
                "\\.goodbye",
                "Ø§Ù† ÙˆØ§Ø­Ø¯Ø© Ù�ÙŠ Ø£Ø®Ø±, Ø£Ù… Ù‡Ø¬ÙˆÙ… Ù‚Ø¯Ù…Ø§ Ù‚ØµÙ�. Ø§Ù„Ø´Ø·Ø± Ø§Ù„Ø­Ø±Ø¨ØŒ",
                "foo\nbar",
                "foo\\.bar",
                "foo\\bbar",
                "foo\\fbar",
                "foo\\nbar",
                "foo\\rbar",
                "foo\\tbar",
                "foo\\vbar",
                "foo\\19876bar",
                "foo\\12876bar",
                "foo\\123876bar",
                "foo\\xa876bar",
                "foo\\xaf876bar" );
        
        final DataManager transaction = dataManager.startTransaction();
        try
        {
            transaction.createBeans( schools );
            transaction.commitTransaction();
        }
        finally
        {
            transaction.closeTransaction();
        }
        
        final Set< School > storedSchools =
                dataManager.getBeans( School.class, Query.where( Require.nothing() ) ).toSet();
        final Map< UUID, School > originalSchools = BeanUtils.toMap( schools );

        assertEquals(schools.size(),  storedSchools.size(), "Shoulda stored the same number of schools as we requested.");

        for ( final School school : storedSchools )
        {
            assertEquals(originalSchools.get( school.getId() ).getName(),
                    school.getName(),
                    "The stored school shoulda had the same name as the original.");
        }
    }
    
    
    private static Set< School > createSchools( final String ... addresses )
    {
        final Set< School > schools = new HashSet<>();
        int i = 0;
        for ( final String address : addresses )
        {
            schools.add( BeanFactory.newBean( School.class )
                    .setName( "School #" + Integer.toString( i ) )
                    .setAddress( address )
                    .setType( SchoolType.PUBLIC ) );
            ++i;
        }
        return schools;
    }
    
    
    @Test
    public void testCreateBeansOutsideOfTransactionNotAllowed()
            throws SecurityException, IllegalArgumentException
    {
        final DatabaseSupport dbSuprt = DatabaseSupportFactory.getSupport(
                                           Teacher.class, CountyService.class );
        final DataManager dataManager = dbSuprt.getDataManager();
        
        final Set< County > counties = buildNumberOfBeans( 10 );
        TestUtil.assertThrows(
                "Shoulda thrown an exception when batch creating beans outside of a transaction.",
                IllegalStateException.class,
                new BlastContainer()
                {
                    public void test() throws Throwable
                    {
                        dataManager.createBeans( counties );
                    }
                } );
    }
    
    
    @Test
    public void testCreateBeansReturnsExpectedNumberOfBeansWhenProvidedMoreThanPageSize()
            throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException
    {
        final DatabaseSupport dbSuprt = DatabaseSupportFactory.getSupport(
                                           Teacher.class, CountyService.class );
        final DataManager dataManager = dbSuprt.getDataManager();
        
        final int beansToCreate = getBeanCountToExerciseCreationPaging( dataManager );
        final Set< County > counties = buildNumberOfBeans( beansToCreate );
        
        final DataManager transaction = dataManager.startTransaction();
        try
        {
            transaction.createBeans( counties );
            transaction.commitTransaction();
        }
        finally
        {
            transaction.closeTransaction();
        }

        assertEquals(beansToCreate,  dataManager.getCount(County.class, Require.nothing()), "Shoulda created the expected number of beans.");
    }


    private static Set< County > buildNumberOfBeans( final int beansToCreate )
    {
        final Set< County > counties = new HashSet<>( beansToCreate );
        for ( int i = 0; i < beansToCreate; ++i )
        {
            counties.add( BeanFactory.newBean( County.class )
                    .setName( String.format( "County #%d", Integer.valueOf( i ) ) )
                    .setPopulation( i * 1000L ) );
        }
        return counties;
    }
    
    
    @Test
    public void testCreateComplexBeansDoesSo()
    {
        final DatabaseSupport dbSuprt = DatabaseSupportFactory.getSupport(
                                           Teacher.class, CountyService.class );
        final DataManager dataManager = dbSuprt.getDataManager();
        
        final Set< Teacher > teachers = new HashSet<>();
        teachers.add( BeanFactory.newBean( Teacher.class )
                .setType( TeacherType.values()[ 0 ] )
                .setName( "bill" )
                .setDateOfBirth( new Date() ) );
        
        DataManager transaction = dataManager.startTransaction();
        try
        {
            transaction.createBeans( teachers );
            transaction.commitTransaction();
        }
        finally
        {
            transaction.closeTransaction();
        }

        assertEquals(1,  dataManager.getCount(Teacher.class, Require.nothing()), "Shoulda created the expected number of beans.");

        teachers.clear();
        teachers.add( BeanFactory.newBean( Teacher.class )
                .setType( TeacherType.values()[ 0 ] )
                .setName( "barry" )
                .setDateOfBirth( new Date() ) );
        teachers.add( BeanFactory.newBean( Teacher.class )
                .setType( TeacherType.values()[ 0 ] )
                .setName( "jason" )
                .setDateOfBirth( new Date() )
                .setSsn( "sss" ) );
        teachers.add( BeanFactory.newBean( Teacher.class )
                .setType( TeacherType.values()[ 0 ] )
                .setName( "zeus" )
                .setDateOfBirth( new Date() ) );

        transaction = dataManager.startTransaction();
        final DataManager trans = transaction;
        try
        {
            TestUtil.assertThrows( null, DaoException.class, new BlastContainer()
            {
                public void test() throws Throwable
                {
                    trans.createBeans( teachers );
                }
            } );
            transaction.commitTransaction();
        }
        finally
        {
            transaction.closeTransaction();
        }
    }


    private static int getBeanCountToExerciseCreationPaging( final DataManager dataManager )
    {
        return 3 * Tunables.postgresDataManagerMaxBeansPerCreateBeansCommand() / 2;
    }
    
    
    private File moveColumnsInCountyTable()
    {
        try
        {
            final File retval = File.createTempFile( getClass().getSimpleName(), "sql" );
            final FileWriter writer = new FileWriter( retval );
            
            writer.write( "ALTER TABLE mockdomain.county DROP COLUMN name;" );
            writer.write( "ALTER TABLE mockdomain.county ADD COLUMN name varchar NOT NULL;" );
            writer.write( "ALTER TABLE mockdomain.county ADD CONSTRAINT nameunique UNIQUE (name);" );
            
            writer.close();
            return retval;
        }
        catch ( final Exception ex )
        {
            throw new RuntimeException( "Failed to generate sql file.", ex );
        }
    }

    
    private final static Logger LOG = Logger.getLogger( DatabaseIntegration_Test.class );
}
