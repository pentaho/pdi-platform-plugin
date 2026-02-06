/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


package org.pentaho.platform.plugin.kettle;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.pentaho.commons.connection.IPentahoResultSet;
import org.pentaho.commons.connection.memory.MemoryMetaData;
import org.pentaho.commons.connection.memory.MemoryResultSet;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.job.Job;
import org.pentaho.di.job.JobExecutionConfiguration;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.filerep.KettleFileRepositoryMeta;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransExecutionConfiguration;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.platform.api.engine.ActionExecutionException;
import org.pentaho.platform.api.engine.IAuthorizationPolicy;
import org.pentaho.platform.api.engine.IPentahoSession;
import org.pentaho.platform.api.engine.ISolutionEngine;
import org.pentaho.platform.api.engine.IUserRoleListService;
import org.pentaho.platform.api.repository2.unified.IUnifiedRepository;
import org.pentaho.platform.api.scheduler2.IScheduler;
import org.pentaho.platform.api.scheduler2.SchedulerException;
import org.pentaho.platform.engine.core.system.PathBasedSystemSettings;
import org.pentaho.platform.engine.core.system.PentahoSessionHolder;
import org.pentaho.platform.engine.core.system.StandaloneSession;
import org.pentaho.platform.engine.core.system.boot.PlatformInitializationException;
import org.pentaho.platform.engine.security.SecurityHelper;
import org.pentaho.platform.engine.services.solution.SolutionEngine;
import org.pentaho.platform.plugin.kettle.security.policy.rolebased.actions.RepositoryExecuteAction;
import org.pentaho.platform.repository2.unified.fs.FileSystemBackedUnifiedRepository;
import org.pentaho.platform.scheduler2.quartz.QuartzScheduler;
import org.pentaho.test.platform.engine.core.MicroPlatform;
import org.springframework.security.core.userdetails.UserDetailsService;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public class PdiActionTest {

  private static final String TEST_LOG_LEVEL_PARAM = "Rowlevel";
  private static final String TEST_TRUE_BOOLEAN_PARAM = "true";
  private static final String TEST_FALSE_BOOLEAN_PARAM = "false";
  private static final String TEST_START_COPY_NAME_PARAM = "startCopyName";

  private QuartzScheduler scheduler;

  public static final String SESSION_PRINCIPAL = "SECURITY_PRINCIPAL";

  private String TEST_USER = "TestUser";

  private static final String SOLUTION_REPOSITORY = "target/test-classes/solution";

  MicroPlatform mp = new MicroPlatform( SOLUTION_REPOSITORY );

  @Before
  public void init() throws SchedulerException, PlatformInitializationException {
    System.setProperty( "java.naming.factory.initial", "org.osjava.sj.SimpleContextFactory" );
    System.setProperty( "org.osjava.sj.root", SOLUTION_REPOSITORY );
    System.setProperty( "org.osjava.sj.delimiter", "/" );
    System.setProperty( "PENTAHO_SYS_CFG_PATH", new File( SOLUTION_REPOSITORY + "/pentaho.xml" ).getAbsolutePath() );

    IPentahoSession session = new StandaloneSession();
    PentahoSessionHolder.setSession( session );

    scheduler = new QuartzScheduler();
    scheduler.start();

    mp.define( IUserRoleListService.class, StubUserRoleListService.class );
    mp.define( UserDetailsService.class, StubUserDetailService.class );
    mp.defineInstance( IAuthorizationPolicy.class, new TestAuthorizationPolicy() );
    mp.setSettingsProvider( new PathBasedSystemSettings() );
    mp.defineInstance( IScheduler.class, scheduler );

    mp.define( ISolutionEngine.class, SolutionEngine.class );
    FileSystemBackedUnifiedRepository repo =  new FileSystemBackedUnifiedRepository( SOLUTION_REPOSITORY );
    mp.defineInstance(  IUnifiedRepository.class, repo );

    mp.start();

    SecurityHelper.getInstance().becomeUser( TEST_USER );
  }

  @After
  public void tearDown() {
    mp.stop();
    FileUtils.deleteQuietly( new File( "testTransformationVariableOverrides.out.txt" ) );
  }

  @Test( expected = Exception.class )
  public void testValidation() throws Exception {
    PdiAction action = getSpyPdiAction();
    action.setRepositoryName( KettleFileRepositoryMeta.REPOSITORY_TYPE_ID );
    action.execute();
  }

  @Test
  public void testTransformationVariableOverrides() throws Exception {
    Map<String, String> variables = new HashMap<>();
    variables.put( "customVariable", "customVariableValue" );

    String[] args = new String[] { "dummyArg" };

    Map<String, String> overrideParams = new HashMap<>();
    overrideParams.put( "param2", "12" );

    PdiAction action = getSpyPdiAction();
    action.setRepositoryName( KettleFileRepositoryMeta.REPOSITORY_TYPE_ID );
    action.setArguments( args );
    action.setVariables( variables );
    action.setParameters( overrideParams );
    action.setDirectory( SOLUTION_REPOSITORY );
    action.setTransformation( "/org/pentaho/platform/plugin/kettle/PdiActionTest_testTransformationVariableOverrides.ktr" );
    action.execute();
    assertArrayEquals( args, action.localTrans.getArguments() );
    List<String> lines = FileUtils.readLines( new File( "testTransformationVariableOverrides.out.txt" ), StandardCharsets.UTF_8 );
    assertTrue( "File \"testTransformationVariableOverrides.out.txt\" should not be empty", lines.size() > 0 );
    String rowData = (String) lines.get( 1 );
    // Columns are as follows:
    // generatedRow|cmdLineArg1|param1|param2|repositoryDirectory|customVariable
    String[] columnData = rowData.split( "\\|" );
    assertEquals( "param1 value is wrong (default value should be in effect)", "param1DefaultValue", columnData[2]
        .trim() );
    assertEquals( "param2 value is wrong (overridden value should be in effect)", 12L, Long.parseLong( columnData[3]
        .trim() ) );

    assertEquals( "The number of rows generated should have equaled the value of param2", 13, lines.size() );

    assertEquals( "customVariable value is wrong", "customVariableValue", columnData[5].trim() );
  }

  @Test
  public void testLoadingFromVariousPaths() throws Exception {
    PdiAction action = getSpyPdiAction();
    action.setRepositoryName( KettleFileRepositoryMeta.REPOSITORY_TYPE_ID );
    action.setArguments( new String[] { "dummyArg" } );
    action.setDirectory( SOLUTION_REPOSITORY  );
    action.setTransformation( "/org/pentaho/platform/plugin/kettle/PdiActionTest_testLoadingFromVariousPaths.ktr" );
    action.execute();
  }

  @Test( expected = ActionExecutionException.class )
  public void testBadFileThrowsException() throws Exception {
    PdiAction action = getSpyPdiAction();
    action.setRepositoryName( KettleFileRepositoryMeta.REPOSITORY_TYPE_ID );
    action.setArguments( new String[] { "dummyArg" } );
    action.setDirectory( "/" );
    action.setTransformation( "testBadFileThrowsException.ktr" );
    action.execute();
  }

  @Test
  public void testJobPaths() throws Exception {
    PdiAction component = getSpyPdiAction();
    component.setRepositoryName( KettleFileRepositoryMeta.REPOSITORY_TYPE_ID );
    component.setDirectory( SOLUTION_REPOSITORY );
    component.setJob( "/org/pentaho/platform/plugin/kettle/PdiActionTest_testJobPaths.kjb" );
    component.execute();
  }

  @Test
  public void testTransformationInjector() throws Exception {

    String[][] columnNames = { { "REGION", "DEPARTMENT", "POSITIONTITLE" } };
    MemoryMetaData metadata = new MemoryMetaData( columnNames, null );

    MemoryResultSet rowsIn = new MemoryResultSet( metadata );
    rowsIn.addRow( new Object[] { "abc", "123", "bogus" } );
    rowsIn.addRow( new Object[] { "region2", "Sales", "bad" } );
    rowsIn.addRow( new Object[] { "Central", "Sales", "test title" } );
    rowsIn.addRow( new Object[] { "Central", "xyz", "bad" } );

    PdiAction component = getSpyPdiAction();
    component.setRepositoryName( KettleFileRepositoryMeta.REPOSITORY_TYPE_ID );
    component.setDirectory( SOLUTION_REPOSITORY );
    component.setTransformation( "/org/pentaho/platform/plugin/kettle/PdiActionTest_testTransformationInjector.ktr" );
    component.setInjectorRows( rowsIn );
    component.setInjectorStep( "Injector" );
    component.setMonitorStep( "Output" );

    component.execute();

    assertEquals( 1, component.getTransformationOutputRowsCount() );
    assertEquals( 0, component.getTransformationOutputErrorRowsCount() );

    IPentahoResultSet rows = component.getTransformationOutputRows();
    assertNotNull( rows );
    assertEquals( 1, rows.getRowCount() );

    assertEquals( "Central", rows.getValueAt( 0, 0 ) );
    assertEquals( "Sales", rows.getValueAt( 0, 1 ) );
    assertEquals( "test title", rows.getValueAt( 0, 2 ) );
    assertEquals( "Hello, test title", rows.getValueAt( 0, 3 ) );

    rows = component.getTransformationOutputErrorRows();
    assertNotNull( rows );

    String log = component.getLog();
    assertTrue( log.contains( "Injector" ) );
    assertTrue( log.contains( "R=1" ) );
    assertTrue( log.contains( "Filter rows" ) );
    assertTrue( log.contains( "W=1" ) );
    assertTrue( log.contains( "Java Script Value" ) );
    assertTrue( log.contains( "W=1" ) );
    assertTrue( log.contains( "Output" ) );
    assertTrue( log.contains( "W=4" ) );
  }

  @Test
  public void testNoSettings() {
    PdiAction component = getSpyPdiAction();
    component.setRepositoryName( KettleFileRepositoryMeta.REPOSITORY_TYPE_ID );
    try {
      component.validate();
      fail();
    } catch ( Exception ex ) {
      // ignored
    }
  }

  @Test
  public void testBadSolutionTransformation() {
    PdiAction component = getSpyPdiAction();
    component.setRepositoryName( KettleFileRepositoryMeta.REPOSITORY_TYPE_ID );
    component.setTransformation( "testBadSolutionTransformation.ktr" );
    try {
      component.validate();
      fail();
    } catch ( Exception ex ) {
      // ignored
    }
  }

  @Test
  public void testBadSolutionJob() {
    PdiAction component = getSpyPdiAction();
    component.setRepositoryName( KettleFileRepositoryMeta.REPOSITORY_TYPE_ID );
    component.setJob( "testBadSolutionJob.kjb" );
    try {
      component.validate();
      fail();
    } catch ( Exception ex ) {
      // ignored
    }
  }

  @Test
  public void testJobParameterPassing() throws Exception {
    String[] args = new String[] { "dummyArg" };

    Map<String, String> params = new HashMap<>();
    params.put( "firstName", "John" );
    params.put( "lastName", "Doe" );

    // this job will pass these parameters to its underlying transformation
    // the transformation will simply add 'firstName' and 'lastName' into a 'fullName'
    PdiAction component = getSpyPdiAction();
    component.setRepositoryName( KettleFileRepositoryMeta.REPOSITORY_TYPE_ID );
    component.setDirectory( SOLUTION_REPOSITORY );
    component.setJob( "/org/pentaho/platform/plugin/kettle/PdiActionTest_testJobParameterPassing.kjb" );
    component.setArguments( args );
    component.setParameters( params );
    component.execute();

    assertArrayEquals( args, component.localJob.getArguments() );
    // 1) check if job execution is successful
    assertEquals( 0, component.getResult() );
    assertEquals( "Finished", component.getStatus() );

    // 2) log scraping: check what the ktr's calculation was for ${first} + ${last} = ${fullName}
    String logScraping =
        component.getLog().substring( component.getLog().indexOf( "${first} + ${last} = ${fullName}" ), component
            .getLog().indexOf( "====================" ) );

    if ( logScraping != null && logScraping.contains( "fullName =" ) ) {
      logScraping = logScraping.substring( logScraping.indexOf( "fullName =" ) );
      logScraping = logScraping.substring( 0, logScraping.indexOf( "\n" ) );
      String fullName = logScraping.replace( "fullName =", "" ).trim();

      assertEquals( "JohnDoe", fullName.trim() );
    }

    // 3) if no exception then the test passes
  }

  @Test
  public void testTransformationInitializationFail() throws Exception {
    try {
      PdiAction action = getSpyPdiAction();
      action.setRepositoryName( KettleFileRepositoryMeta.REPOSITORY_TYPE_ID );
      action.setDirectory( SOLUTION_REPOSITORY );
      action.setTransformation( "/org/pentaho/platform/plugin/kettle/PdiActionTest_testTransformationInitializationFail.ktr" );

      action.execute();
    } catch ( Exception e ) {
      e.printStackTrace();
      fail( "Exception is thrown: " + e.getLocalizedMessage() );
    }
  }

  @Test
  public void testTransformationPrepareExecutionFailed() {
    try {
      PdiAction action = getSpyPdiAction();
      action.setRepositoryName( KettleFileRepositoryMeta.REPOSITORY_TYPE_ID );
      action.setDirectory( SOLUTION_REPOSITORY );
      action.setTransformation( "/org/pentaho/platform/plugin/kettle/PdiActionTest_testTransformationPrepareExecutionFailed.ktr" );

      action.execute();
      assertTrue( action.isTransPrepareExecutionFailed() );
    } catch ( Exception e ) {
      e.printStackTrace();
      fail( "Exception is thrown: " + e.getLocalizedMessage() );
    }
  }

  @Test
  public void testExecutionSuccessful() {
    try {
      PdiAction action = getSpyPdiAction();
      action.setRepositoryName( KettleFileRepositoryMeta.REPOSITORY_TYPE_ID );
      action.setDirectory( SOLUTION_REPOSITORY );
      action.setTransformation( "/org/pentaho/platform/plugin/kettle/PdiActionTest_testTransformationExecutionFailure.ktr" );

      action.execute();
      assertFalse( action.isExecutionSuccessful() );
    } catch ( Exception e ) {
      e.printStackTrace();
      fail( "Exception is thrown: " + e.getLocalizedMessage() );
    }
  }

  @Test
  public void testSetParamsIntoExecuteConfigInExecuteTrans() throws ActionExecutionException {
    PdiAction action = getSpyPdiAction();
    TransMeta meta = spy( new TransMeta() );
    Trans trans = mock( Trans.class );
    TransExecutionConfiguration transExecutionConfiguration = mock( TransExecutionConfiguration.class );

    doReturn( trans ).when( action ).newTrans( meta );
    doReturn( transExecutionConfiguration ).when( action ).newTransExecutionConfiguration();

    // Let's avoid logging here
    Log log = mock( Log.class );
    action.setLogger( log );
    doReturn( false ).when( log ).isDebugEnabled();

    // SET INITIAL VALUES FOR THE TEST
    //

    // Log level
    action.setLogLevel( TEST_LOG_LEVEL_PARAM );
    meta.setLogLevel( LogLevel.getLogLevelForCode( TEST_LOG_LEVEL_PARAM ) );

    // Clear log
    action.setClearLog( TEST_TRUE_BOOLEAN_PARAM );

    // Safe mode
    action.setRunSafeMode( TEST_FALSE_BOOLEAN_PARAM );
    meta.setSafeModeEnabled( Boolean.parseBoolean( TEST_FALSE_BOOLEAN_PARAM ) );

    // Gather metrics
    action.setGatheringMetrics( TEST_FALSE_BOOLEAN_PARAM );
    meta.setGatheringMetrics( Boolean.parseBoolean( TEST_FALSE_BOOLEAN_PARAM ) );

    // EXECUTE
    //
    action.executeTransformation( meta );

    // VALIDATIONS
    //

    // TransExecutionConfiguration
    verify( transExecutionConfiguration ).setLogLevel( LogLevel.getLogLevelForCode( TEST_LOG_LEVEL_PARAM ) );
    verify( transExecutionConfiguration ).setClearingLog( Boolean.parseBoolean( TEST_TRUE_BOOLEAN_PARAM ) );
    verify( transExecutionConfiguration ).setSafeModeEnabled( Boolean.parseBoolean( TEST_FALSE_BOOLEAN_PARAM ) );
    verify( transExecutionConfiguration ).setGatheringMetrics( Boolean.parseBoolean( TEST_FALSE_BOOLEAN_PARAM ) );
  }

  @Test
  public void testSetParamsIntoExecuteConfigInExecuteJob() throws ActionExecutionException {
    PdiAction action = getSpyPdiAction();
    JobMeta meta = spy( new JobMeta() );
    Repository repository = mock( Repository.class );
    Job job = mock( Job.class );
    JobExecutionConfiguration jobExecutionConfiguration = mock( JobExecutionConfiguration.class );
    Result result = mock( Result.class );

    doReturn( job ).when( action ).newJob( repository, meta );
    doReturn( jobExecutionConfiguration ).when( action ).newJobExecutionConfiguration();
    doReturn( result ).when( job ).getResult();

    // Let's avoid logging here
    Log log = mock( Log.class );
    doReturn( false ).when( log ).isDebugEnabled();
    action.setLogger( log );

    // SET INITIAL VALUES FOR THE TEST
    //

    // Log level
    action.setLogLevel( TEST_LOG_LEVEL_PARAM );
    meta.setLogLevel( LogLevel.getLogLevelForCode( TEST_LOG_LEVEL_PARAM ) );

    // Clear log
    action.setClearLog( TEST_TRUE_BOOLEAN_PARAM );

    // Safe mode
    action.setRunSafeMode( TEST_FALSE_BOOLEAN_PARAM );
    meta.setSafeModeEnabled( Boolean.parseBoolean( TEST_FALSE_BOOLEAN_PARAM ) );

    // Gather metrics
    action.setGatheringMetrics( TEST_FALSE_BOOLEAN_PARAM );
    meta.setGatheringMetrics( Boolean.parseBoolean( TEST_FALSE_BOOLEAN_PARAM ) );

    // The following only apply to Jobs

    // Expanding remote job
    action.setExpandingRemoteJob( TEST_FALSE_BOOLEAN_PARAM );
    // Start copy name
    action.setStartCopyName( TEST_START_COPY_NAME_PARAM );

    // EXECUTE
    //
    action.executeJob( meta, repository );

    // VALIDATIONS
    //

    // Job
    verify( job ).setGatheringMetrics( Boolean.valueOf( TEST_FALSE_BOOLEAN_PARAM ) );
    verify( job ).setLogLevel( LogLevel.getLogLevelForCode( TEST_LOG_LEVEL_PARAM ) );

    // JobExecutionConfiguration
    verify( jobExecutionConfiguration ).setLogLevel( LogLevel.getLogLevelForCode( TEST_LOG_LEVEL_PARAM ) );
    verify( jobExecutionConfiguration ).setClearingLog( Boolean.parseBoolean( TEST_TRUE_BOOLEAN_PARAM ) );
    verify( jobExecutionConfiguration ).setSafeModeEnabled( Boolean.parseBoolean( TEST_FALSE_BOOLEAN_PARAM ) );
    verify( jobExecutionConfiguration ).setGatheringMetrics( Boolean.parseBoolean( TEST_FALSE_BOOLEAN_PARAM ) );

    verify( jobExecutionConfiguration ).setExpandingRemoteJob( Boolean.parseBoolean( TEST_FALSE_BOOLEAN_PARAM ) );
    verify( jobExecutionConfiguration ).setStartCopyName( TEST_START_COPY_NAME_PARAM );
  }

  // ===========================================================================================
  // Tests for KETTLE_USE_STORED_VARIABLES feature (variable source priority)
  // ===========================================================================================

  @Test
  public void testIsUseStoredVariables_NotSet_ReturnsFalse() {
    // Default behavior when property is not set
    System.clearProperty( PdiAction.USE_STORED_VARIABLES_PROPERTY );
    PdiAction action = getSpyPdiAction();
    assertFalse( "Should return false when property is not set", action.isUseStoredVariables() );
  }

  @Test
  public void testIsUseStoredVariables_SetToY_ReturnsTrue() {
    try {
      System.setProperty( PdiAction.USE_STORED_VARIABLES_PROPERTY, "Y" );
      PdiAction action = getSpyPdiAction();
      assertTrue( "Should return true when property is 'Y'", action.isUseStoredVariables() );
    } finally {
      System.clearProperty( PdiAction.USE_STORED_VARIABLES_PROPERTY );
    }
  }

  @Test
  public void testIsUseStoredVariables_SetToLowercaseY_ReturnsTrue() {
    try {
      System.setProperty( PdiAction.USE_STORED_VARIABLES_PROPERTY, "y" );
      PdiAction action = getSpyPdiAction();
      assertTrue( "Should return true when property is 'y'", action.isUseStoredVariables() );
    } finally {
      System.clearProperty( PdiAction.USE_STORED_VARIABLES_PROPERTY );
    }
  }

  @Test
  public void testIsUseStoredVariables_SetToTrue_ReturnsTrue() {
    try {
      System.setProperty( PdiAction.USE_STORED_VARIABLES_PROPERTY, "true" );
      PdiAction action = getSpyPdiAction();
      assertTrue( "Should return true when property is 'true'", action.isUseStoredVariables() );
    } finally {
      System.clearProperty( PdiAction.USE_STORED_VARIABLES_PROPERTY );
    }
  }

  @Test
  public void testIsUseStoredVariables_SetToTRUE_ReturnsTrue() {
    try {
      System.setProperty( PdiAction.USE_STORED_VARIABLES_PROPERTY, "TRUE" );
      PdiAction action = getSpyPdiAction();
      assertTrue( "Should return true when property is 'TRUE'", action.isUseStoredVariables() );
    } finally {
      System.clearProperty( PdiAction.USE_STORED_VARIABLES_PROPERTY );
    }
  }

  @Test
  public void testIsUseStoredVariables_SetToN_ReturnsFalse() {
    try {
      System.setProperty( PdiAction.USE_STORED_VARIABLES_PROPERTY, "N" );
      PdiAction action = getSpyPdiAction();
      assertFalse( "Should return false when property is 'N'", action.isUseStoredVariables() );
    } finally {
      System.clearProperty( PdiAction.USE_STORED_VARIABLES_PROPERTY );
    }
  }

  @Test
  public void testIsUseStoredVariables_SetToFalse_ReturnsFalse() {
    try {
      System.setProperty( PdiAction.USE_STORED_VARIABLES_PROPERTY, "false" );
      PdiAction action = getSpyPdiAction();
      assertFalse( "Should return false when property is 'false'", action.isUseStoredVariables() );
    } finally {
      System.clearProperty( PdiAction.USE_STORED_VARIABLES_PROPERTY );
    }
  }

  @Test
  public void testIsUseStoredVariables_SetToInvalidValue_ReturnsFalse() {
    try {
      System.setProperty( PdiAction.USE_STORED_VARIABLES_PROPERTY, "invalid" );
      PdiAction action = getSpyPdiAction();
      assertFalse( "Should return false when property has invalid value", action.isUseStoredVariables() );
    } finally {
      System.clearProperty( PdiAction.USE_STORED_VARIABLES_PROPERTY );
    }
  }

  @Test
  public void testResolveVariableValue_UseStoredVariables_ReturnsStoredValue() {
    try {
      System.setProperty( PdiAction.USE_STORED_VARIABLES_PROPERTY, "Y" );
      // Also set a kettle property that should be ignored
      System.setProperty( "testVar", "kettlePropertiesValue" );

      PdiAction action = getSpyPdiAction();
      String result = action.resolveVariableValue( "testVar", "storedValue" );

      assertEquals( "Should return stored value when USE_STORED_VARIABLES is Y", "storedValue", result );
    } finally {
      System.clearProperty( PdiAction.USE_STORED_VARIABLES_PROPERTY );
      System.clearProperty( "testVar" );
    }
  }

  @Test
  public void testResolveVariableValue_Default_PrefersKettleProperties() {
    try {
      System.clearProperty( PdiAction.USE_STORED_VARIABLES_PROPERTY );
      System.setProperty( "testVar", "kettlePropertiesValue" );

      PdiAction action = getSpyPdiAction();
      String result = action.resolveVariableValue( "testVar", "storedValue" );

      assertEquals( "Should return kettle.properties value by default", "kettlePropertiesValue", result );
    } finally {
      System.clearProperty( "testVar" );
    }
  }

  @Test
  public void testResolveVariableValue_Default_FallsBackToStoredWhenKettlePropertyMissing() {
    System.clearProperty( PdiAction.USE_STORED_VARIABLES_PROPERTY );
    // Ensure the variable is not in system properties
    System.clearProperty( "testVarNotInKettle" );

    PdiAction action = getSpyPdiAction();
    String result = action.resolveVariableValue( "testVarNotInKettle", "storedValue" );

    assertEquals( "Should fall back to stored value when kettle.properties doesn't have the variable", "storedValue", result );
  }

  @Test
  public void testResolveVariableValue_UseStoredVariables_WithNullStoredValue() {
    try {
      System.setProperty( PdiAction.USE_STORED_VARIABLES_PROPERTY, "Y" );
      System.setProperty( "testVar", "kettlePropertiesValue" );

      PdiAction action = getSpyPdiAction();
      String result = action.resolveVariableValue( "testVar", null );

      assertEquals( "Should return null stored value when USE_STORED_VARIABLES is Y", null, result );
    } finally {
      System.clearProperty( PdiAction.USE_STORED_VARIABLES_PROPERTY );
      System.clearProperty( "testVar" );
    }
  }

  @Test
  public void testResolveVariableValue_Default_WithNullStoredValueAndNoKettleProperty() {
    System.clearProperty( PdiAction.USE_STORED_VARIABLES_PROPERTY );
    System.clearProperty( "testVarNotExists" );

    PdiAction action = getSpyPdiAction();
    String result = action.resolveVariableValue( "testVarNotExists", null );

    assertEquals( "Should return null when both sources are null/missing", null, result );
  }

  @Test
  public void testResolveVariableValue_Default_WithEmptyKettlePropertyValue() {
    try {
      System.clearProperty( PdiAction.USE_STORED_VARIABLES_PROPERTY );
      System.setProperty( "testVar", "" );

      PdiAction action = getSpyPdiAction();
      String result = action.resolveVariableValue( "testVar", "storedValue" );

      // Empty string is a valid value, should be used over stored value
      assertEquals( "Should return empty kettle.properties value (not fall back to stored)", "", result );
    } finally {
      System.clearProperty( "testVar" );
    }
  }

  /**
   * End-to-end test: Transformation execution uses stored variable value when USE_STORED_VARIABLES=Y
   */
  @Test
  public void testTransformationExecution_UseStoredVariables_UsesStoredValue() throws Exception {
    try {
      // Configure to use stored variables
      System.setProperty( PdiAction.USE_STORED_VARIABLES_PROPERTY, "Y" );
      // Set a kettle property that should be IGNORED
      System.setProperty( "customVariable", "fromKettleProperties" );

      Map<String, String> variables = new HashMap<>();
      variables.put( "customVariable", "fromStoredValue" );

      PdiAction action = getSpyPdiAction();
      action.setRepositoryName( KettleFileRepositoryMeta.REPOSITORY_TYPE_ID );
      action.setArguments( new String[] { "dummyArg" } );
      action.setVariables( variables );
      action.setDirectory( SOLUTION_REPOSITORY );
      action.setTransformation( "/org/pentaho/platform/plugin/kettle/PdiActionTest_testTransformationVariableOverrides.ktr" );
      action.execute();

      List<String> lines = FileUtils.readLines( new File( "testTransformationVariableOverrides.out.txt" ), StandardCharsets.UTF_8 );
      assertTrue( "File should not be empty", lines.size() > 0 );
      String rowData = (String) lines.get( 1 );
      String[] columnData = rowData.split( "\\|" );

      assertEquals( "Should use stored value when USE_STORED_VARIABLES=Y", "fromStoredValue", columnData[5].trim() );
    } finally {
      System.clearProperty( PdiAction.USE_STORED_VARIABLES_PROPERTY );
      System.clearProperty( "customVariable" );
    }
  }

  /**
   * End-to-end test: Transformation execution uses kettle.properties value by default (when variable exists in kettle.properties)
   */
  @Test
  public void testTransformationExecution_Default_PrefersKettleProperties() throws Exception {
    try {
      // Clear the flag to use default behavior
      System.clearProperty( PdiAction.USE_STORED_VARIABLES_PROPERTY );
      // Set a kettle property that should be USED
      System.setProperty( "customVariable", "fromKettleProperties" );

      Map<String, String> variables = new HashMap<>();
      variables.put( "customVariable", "fromStoredValue" );

      PdiAction action = getSpyPdiAction();
      action.setRepositoryName( KettleFileRepositoryMeta.REPOSITORY_TYPE_ID );
      action.setArguments( new String[] { "dummyArg" } );
      action.setVariables( variables );
      action.setDirectory( SOLUTION_REPOSITORY );
      action.setTransformation( "/org/pentaho/platform/plugin/kettle/PdiActionTest_testTransformationVariableOverrides.ktr" );
      action.execute();

      List<String> lines = FileUtils.readLines( new File( "testTransformationVariableOverrides.out.txt" ), StandardCharsets.UTF_8 );
      assertTrue( "File should not be empty", lines.size() > 0 );
      String rowData = (String) lines.get( 1 );
      String[] columnData = rowData.split( "\\|" );

      assertEquals( "Should use kettle.properties value by default", "fromKettleProperties", columnData[5].trim() );
    } finally {
      System.clearProperty( "customVariable" );
    }
  }

  /**
   * End-to-end test: Transformation execution falls back to stored value when variable not in kettle.properties
   */
  @Test
  public void testTransformationExecution_Default_FallsBackToStoredValue() throws Exception {
    // Clear the flag to use default behavior
    System.clearProperty( PdiAction.USE_STORED_VARIABLES_PROPERTY );
    // Ensure variable is NOT in system properties
    System.clearProperty( "customVariable" );

    Map<String, String> variables = new HashMap<>();
    variables.put( "customVariable", "fromStoredValue" );

    PdiAction action = getSpyPdiAction();
    action.setRepositoryName( KettleFileRepositoryMeta.REPOSITORY_TYPE_ID );
    action.setArguments( new String[] { "dummyArg" } );
    action.setVariables( variables );
    action.setDirectory( SOLUTION_REPOSITORY );
    action.setTransformation( "/org/pentaho/platform/plugin/kettle/PdiActionTest_testTransformationVariableOverrides.ktr" );
    action.execute();

    List<String> lines = FileUtils.readLines( new File( "testTransformationVariableOverrides.out.txt" ), StandardCharsets.UTF_8 );
    assertTrue( "File should not be empty", lines.size() > 0 );
    String rowData = (String) lines.get( 1 );
    String[] columnData = rowData.split( "\\|" );

    assertEquals( "Should fall back to stored value when not in kettle.properties", "fromStoredValue", columnData[5].trim() );
  }

  // ===========================================================================================
  // END Tests for KETTLE_USE_STORED_VARIABLES feature (variable source priority)
  // ===========================================================================================


  public class TestAuthorizationPolicy implements IAuthorizationPolicy {

    List<String> allowedActions = new ArrayList<>();

    @Override
    public List<String> getAllowedActions( String arg0 ) {
      allowedActions.add( "org.pentaho.repository.read" );
      allowedActions.add( "org.pentaho.repository.create" );
      return allowedActions;
    }

    @Override
    public boolean isAllowed( String arg0 ) {
      return true;
    }

  }

  public class TestAuthorizationPolicyNoExecute implements IAuthorizationPolicy {

    List<String> allowedActions = new ArrayList<>();

    @Override
    public List<String> getAllowedActions( String arg0 ) {
      allowedActions.add( "org.pentaho.repository.read" );
      allowedActions.add( "org.pentaho.repository.create" );
      return allowedActions;
    }

    @Override
    public boolean isAllowed( String action ) {
      if ( action != null && action.equals( RepositoryExecuteAction.NAME ) ) {
        return false;
      }
      return true;
    }
  }

  /**
   * Returns a PdiAction that does not override any configuration.
   *
   * @return a PdiAction that does not override any configuration
   */
  private PdiAction getSpyPdiAction() {
    return getSpyPdiAction( null, null, null );
  }

  /**
   * Returns a PdiAction that will override the configuration based on the given parameters.
   * Passing a null for any of the configurations, will result in not existing the corresponding property.
   *
   * @param gatherMetrics the value for the Gather Metrics configuration
   * @param safeMode      the value for the Safe Mode configuration
   * @param logLevel      the value for the Log Level configuration
   * @return a PdiAction that will override the configuration based on the given parameters
   */
  private PdiAction getSpyPdiAction( String gatherMetrics, String safeMode, String logLevel ) {
    Properties props = mock( Properties.class );

    if ( null != gatherMetrics ) {
      doReturn( gatherMetrics ).when( props ).getProperty( PdiAction.GATHER_METRICS_PROPERTY );
    }

    if ( null != safeMode ) {
      doReturn( safeMode ).when( props ).getProperty( PdiAction.SAFE_MODE_PROPERTY );
    }

    if ( null != logLevel ) {
      doReturn( logLevel ).when( props ).getProperty( PdiAction.LOG_LEVEL_PROPERTY );
    }

    PdiAction spiedPdiAction = spy( new PdiAction() );
    doReturn( props ).when( spiedPdiAction ).getPluginSettings();

    return spiedPdiAction;
  }
}
