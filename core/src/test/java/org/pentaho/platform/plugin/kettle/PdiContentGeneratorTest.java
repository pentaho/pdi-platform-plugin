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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;

import java.io.File;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.pentaho.di.repository.filerep.KettleFileRepositoryMeta;
import org.pentaho.platform.api.repository2.unified.RepositoryFile;
import org.pentaho.platform.plugin.kettle.messages.Messages;
import org.pentaho.test.platform.engine.core.MicroPlatform;
import org.pentaho.platform.api.engine.IUserRoleListService;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.pentaho.platform.api.engine.IAuthorizationPolicy;
import org.pentaho.platform.api.engine.ISolutionEngine;
import org.pentaho.platform.api.repository2.unified.IUnifiedRepository;
import org.pentaho.platform.repository2.unified.fs.FileSystemBackedUnifiedRepository;
import org.pentaho.platform.engine.services.solution.SolutionEngine;
//import org.pentaho.platform.scheduler2.quartz.QuartzScheduler;
import org.pentaho.platform.api.scheduler2.IScheduler;
import org.pentaho.platform.api.engine.IPentahoSession;
import org.pentaho.platform.engine.core.system.PentahoSessionHolder;
import org.pentaho.platform.engine.core.system.StandaloneSession;

public class PdiContentGeneratorTest {

  private static final String SOLUTION_REPOSITORY = "target/test-classes/solution";

  MicroPlatform mp = new MicroPlatform( SOLUTION_REPOSITORY );

  private PdiContentGenerator pdiContentGenerator;
  private OutputStream outputStream;
  private RepositoryFile repositoryFile;
  private PdiAction pdiAction;

  @Before
  public void setUp() throws Exception {
    System.setProperty( "java.naming.factory.initial", "org.osjava.sj.SimpleContextFactory" );
    System.setProperty( "org.osjava.sj.root", SOLUTION_REPOSITORY );
    System.setProperty( "org.osjava.sj.delimiter", "/" );

    System.setProperty( "PENTAHO_SYS_CFG_PATH", new File( SOLUTION_REPOSITORY + "/pentaho.xml" ).getAbsolutePath() );

    IPentahoSession session = new StandaloneSession();
    PentahoSessionHolder.setSession( session );

    pdiContentGenerator = new PdiContentGenerator();

    pdiAction = getSpyPdiAction();
    pdiAction.setRepositoryName( KettleFileRepositoryMeta.REPOSITORY_TYPE_ID );
    pdiContentGenerator.setPdiAction( pdiAction );

    outputStream = mock( OutputStream.class );
    repositoryFile = mock( RepositoryFile.class );
    pdiContentGenerator.setOutputStream( outputStream );
    pdiContentGenerator.setRepositoryFile( repositoryFile );

    mp.define( IUserRoleListService.class, StubUserRoleListService.class );
    mp.define( UserDetailsService.class, StubUserDetailService.class );
    mp.defineInstance( IAuthorizationPolicy.class, new TestAuthorizationPolicy() );

    mp.define( ISolutionEngine.class, SolutionEngine.class );
    FileSystemBackedUnifiedRepository repo =  new FileSystemBackedUnifiedRepository( SOLUTION_REPOSITORY );
    mp.defineInstance(  IUnifiedRepository.class, repo );

    mp.start();
  }

  @After
  public void tearDown() {
    mp.stop();
  }

  @Test
  public void testExecuteSuccess_KTR() throws Exception {
    testExecuteSuccess( "/org/pentaho/platform/plugin/kettle/PdiContentGeneratorTest_success.ktr", "PdiContentGeneratorTest_success.ktr" );
  }

  @Test
  public void testExecuteSuccess_KJB() throws Exception {
    testExecuteSuccess( "/org/pentaho/platform/plugin/kettle/PdiContentGeneratorTest_success.kjb", "PdiContentGeneratorTest_success.kjb" );
  }

  private void testExecuteSuccess( String path, String name ) throws Exception {
    when( repositoryFile.getPath() ).thenReturn( path );
    when( repositoryFile.getName() ).thenReturn( name );

    pdiContentGenerator.execute();
    String output = pdiContentGenerator.getOutputStringBuilder().toString();
    assertTrue( output.contains( Messages.getInstance().getString( "PdiAction.STATUS_SUCCESS_HEADING" ) ) );
  }

  @Test
  public void testExecuteFailure_KTR() {
    testExecuteFailure( "/org/pentaho/platform/plugin/kettle/PdiContentGeneratorTest_fail.ktr", "PdiContentGeneratorTest_fail.ktr" );
  }

  @Test
  public void testExecuteFailure_KJB() {
    testExecuteFailure( "/org/pentaho/platform/plugin/kettle/PdiContentGeneratorTest_fail.kjb", "PdiContentGeneratorTest_fail.kjb" );
  }

  private void testExecuteFailure( String path, String name ) {
    when( repositoryFile.getPath() ).thenReturn( path );
    when( repositoryFile.getName() ).thenReturn( name );

    try {
      pdiContentGenerator.execute();
    } catch ( Exception ex ) {
      // This is expected as the transformation fails with at the prepareExecution
    }
  }

  static class TestAuthorizationPolicy implements IAuthorizationPolicy {
    private static final String ACTION_READ = "org.pentaho.repository.read";
    private static final String ACTION_CREATE = "org.pentaho.repository.create";

    List<String> allowedActions = new ArrayList<>();

    @Override
    public List<String> getAllowedActions( String arg0 ) {
      allowedActions.add( ACTION_READ );
      allowedActions.add( ACTION_CREATE );
      return allowedActions;
    }

    @Override
    public boolean isAllowed( String arg0 ) {
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
